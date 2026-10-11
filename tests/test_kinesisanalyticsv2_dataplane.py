"""
Managed Service for Apache Flink data plane: real jobs in the official Flink
image, driven through the kinesisanalyticsv2 API.

The test job (tests/fixtures/flink) is compiled at setup against the Flink
classes in the runtime image, so no binary is committed. It reads the "Test"
runtime property group and writes a checkpointed counter to a MiniStack
Kinesis stream, which covers the property file, the endpoint injection, and
restore from a savepoint.
"""

import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from ministack.services import kinesisanalyticsv2_flink as dataplane

RUNTIME = "FLINK-1_20"
ROLE = "arn:aws:iam::000000000000:role/flink-app"
SOURCE_DIR = Path(__file__).parent / "fixtures" / "flink" / "src"
MAIN_CLASS = "ministack.flinktest.CounterJob"

requires_docker = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- skipping Flink data-plane tests"
)


def _wait_status(client, name, wanted, timeout=240):
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = client.describe_application(ApplicationName=name)["ApplicationDetail"]["ApplicationStatus"]
        if status == wanted:
            return
        time.sleep(1)
    raise AssertionError(f"{name} is {status}, expected {wanted} within {timeout}s")


def _wait_snapshot(client, name, snapshot, wanted="READY", timeout=180):
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = client.describe_application_snapshot(ApplicationName=name, SnapshotName=snapshot)[
            "SnapshotDetails"]["SnapshotStatus"]
        if status == wanted:
            return
        time.sleep(1)
    raise AssertionError(f"snapshot {snapshot} of {name} is {status}, expected {wanted} within {timeout}s")


def _containers(name):
    import docker

    client = docker.from_env()
    filters = {"label": ["ministack=kinesisanalyticsv2", f"application={name}"]}
    return client.containers.list(all=True, filters=filters)


class _Stream:
    """Reads the counter records the job writes, in order."""

    def __init__(self, kin, name):
        self.kin = kin
        self.name = name
        self.iterator = kin.get_shard_iterator(
            StreamName=name, ShardId="shardId-000000000000", ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]

    def read(self):
        records = []
        while True:
            resp = self.kin.get_records(ShardIterator=self.iterator)
            self.iterator = resp["NextShardIterator"]
            records += [json.loads(r["Data"]) for r in resp["Records"]]
            if not resp["Records"]:
                return records

    def wait_for(self, count, timeout=60):
        records = []
        deadline = time.monotonic() + timeout
        while len(records) < count and time.monotonic() < deadline:
            records += self.read()
            time.sleep(0.5)
        assert len(records) >= count, f"expected {count} records from the job, got {len(records)}"
        return records


@pytest.fixture(scope="module")
def counter_jar():
    """Compile the test job against the Flink classes of the runtime image."""
    if shutil.which("javac") is None or shutil.which("jar") is None:
        pytest.skip("a JDK (javac and jar) is needed to build the Flink test job")
    import docker

    client = docker.from_env()
    image = dataplane._image_for({"RuntimeEnvironment": RUNTIME})
    try:
        client.images.pull(image)
    except Exception as e:
        try:
            client.images.get(image)
        except Exception:
            pytest.skip(f"could not pull {image}: {e}")
    work = Path(tempfile.mkdtemp(prefix="flink-test-job-"))
    container = client.containers.create(image)
    try:
        stream, _ = container.get_archive("/opt/flink/lib")
        with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as tar:
            tar.extractall(work, filter="data")
        sources = [str(p) for p in SOURCE_DIR.rglob("*.java")]
        subprocess.run(
            ["javac", "--release", "11", "-nowarn", "-cp", f"{work / 'lib'}{os.sep}*", "-d", str(work / "classes"),
             *sources],
            check=True, capture_output=True,
        )
        jar_path = work / "counter-job.jar"
        subprocess.run(
            ["jar", "--create", "--file", str(jar_path), "--main-class", MAIN_CLASS, "-C", str(work / "classes"), "."],
            check=True, capture_output=True,
        )
        yield jar_path.read_bytes()
    finally:
        container.remove(force=True)
        shutil.rmtree(work, ignore_errors=True)


@pytest.fixture
def flink_app(kinesisanalyticsv2, s3, kin, counter_jar):
    """An application running the counter job against a fresh Kinesis stream."""
    suffix = uuid.uuid4().hex[:8]
    bucket, stream, name = f"flink-jars-{suffix}", f"flink-out-{suffix}", f"flink-dp-{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="counter-job.jar", Body=counter_jar)
    kin.create_stream(StreamName=stream, ShardCount=1)
    kinesisanalyticsv2.create_application(
        ApplicationName=name,
        RuntimeEnvironment=RUNTIME,
        ServiceExecutionRole=ROLE,
        ApplicationConfiguration={
            "ApplicationCodeConfiguration": {
                "CodeContentType": "ZIPFILE",
                "CodeContent": {
                    "S3ContentLocation": {"BucketARN": f"arn:aws:s3:::{bucket}", "FileKey": "counter-job.jar"}
                },
            },
            "EnvironmentProperties": {
                "PropertyGroups": [
                    {"PropertyGroupId": "Test", "PropertyMap": {"StreamName": stream, "Message": f"hello-{suffix}"}}
                ]
            },
            "FlinkApplicationConfiguration": {
                "ParallelismConfiguration": {"ConfigurationType": "CUSTOM", "Parallelism": 2,
                                             "ParallelismPerKPU": 1, "AutoScalingEnabled": False}
            },
            "ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True},
        },
    )
    yield {"name": name, "stream": stream, "message": f"hello-{suffix}"}
    try:
        status = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]["ApplicationStatus"]
        if status in ("RUNNING", "STARTING"):
            kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
            _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
        created = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]["CreateTimestamp"]
        kinesisanalyticsv2.delete_application(ApplicationName=name, CreateTimestamp=created)
    except Exception:
        pass


@requires_docker
@pytest.mark.data_plane
def test_job_runs_with_runtime_properties_and_writes_to_kinesis(kinesisanalyticsv2, kin, flink_app):
    name = flink_app["name"]
    stream = _Stream(kin, flink_app["stream"])
    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    assert {c.labels.get("role") for c in _containers(name)} == {"jobmanager", "taskmanager"}

    records = stream.wait_for(3)
    assert records[0] == {"message": flink_app["message"], "n": 0}

    kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
    assert _containers(name) == []


@requires_docker
@pytest.mark.data_plane
def test_restore_from_latest_snapshot_resumes_the_job(kinesisanalyticsv2, kin, flink_app):
    name = flink_app["name"]
    stream = _Stream(kin, flink_app["stream"])
    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    before = stream.wait_for(4)

    kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="mid-run")
    _wait_snapshot(kinesisanalyticsv2, name, "mid-run")

    # A graceful stop with snapshots enabled takes the latest snapshot.
    kinesisanalyticsv2.stop_application(ApplicationName=name)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=180)
    last_before = max(r["n"] for r in before + stream.read())

    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    resumed = stream.wait_for(1)
    assert resumed[0]["n"] > 0, "the job started from scratch instead of the snapshot"
    assert resumed[0]["n"] >= last_before


@requires_docker
@pytest.mark.data_plane
def test_start_with_missing_code_returns_to_ready(kinesisanalyticsv2, s3, flink_app):
    name = flink_app["name"]
    detail = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]
    loc = detail["ApplicationConfigurationDescription"]["ApplicationCodeConfigurationDescription"][
        "CodeContentDescription"]["S3ApplicationCodeLocationDescription"]
    s3.delete_object(Bucket=loc["BucketARN"].split(":::")[-1], Key=loc["FileKey"])

    kinesisanalyticsv2.start_application(ApplicationName=name)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
    assert _containers(name) == []
