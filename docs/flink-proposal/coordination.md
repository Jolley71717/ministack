# Flink data plane: coordination notes

From the local Mac session, for review by the cloud session. This file is for
coordination only. Drop it from the branch before any upstream PR.

## Branch

`feat/flink-dataplane` is 3 commits on `feat/managed-flink` at `a98b75d`, plus this note:

| Commit | What |
|---|---|
| `3f35f67` feat | Docker data plane (`kinesisanalyticsv2_flink.py`) and data-plane tests. The test job is compiled from source at setup. |
| `39e1c77` fix | Pull a missing image; jobs sign with the app's account; CheckpointConfiguration reaches the cluster; `supports(runtime)`; clearer start errors. |
| `66eeab9` feat | Replaces `502b64c`: jars in `/opt/ministack/flink-lib` go into each cluster's `/opt/flink/lib`. `Dockerfile.full` ships `aws-kinesisanalytics-runtime` 1.2.0 there. No runtime download. Separate, so it can be dropped. |

No binary anywhere in the branch:
`git log --all --diff-filter=A --name-only origin/master..HEAD | grep -iE '\.(jar|class|zip)$'`
prints nothing (exit 1).

## Files (`git diff --stat origin/feat/managed-flink...HEAD`)

```
 Dockerfile.full                                    |  17 +
 README.md                                          |   6 +
 docs/flink-proposal/coordination.md                | (this file)
 ministack/services/kinesisanalyticsv2.py           |  16 +-
 ministack/services/kinesisanalyticsv2_flink.py     | 720 +++++++++++++++++++++
 .../flink/src/ministack/flinktest/CounterJob.java  | 132 ++++
 tests/test_kinesisanalyticsv2.py                   |  21 +
 tests/test_kinesisanalyticsv2_dataplane.py         | 341 ++++++++++
```

## Edits to `kinesisanalyticsv2.py` (yours), and why

1. **New `_runs_jobs(record)` helper**: `_dataplane is not None and _dataplane.available() and _dataplane.supports(record["RuntimeEnvironment"])`.
   - Why: the data plane only has images for `FLINK-1_19` and `FLINK-1_20`.
   - Before this, a `FLINK-1_18` (or 1.15 or 2.x) app with Docker present was handed to the data plane. It started no cluster, but it was marked `_driven`.
   - Its graceful stop then got `None` back as the savepoint, so the stop snapshot became `FAILED`.
   - Those runtimes now behave exactly as they do without Docker.
2. **`_start_application`**: `_dataplane.available()` becomes `_runs_jobs(record)`. The log line names the runtime, since "no Docker" is no longer the only reason a job doesn't run.
3. **`update_application` and `replace_from_template`**: the restart condition becomes `record.get("_driven") and _runs_jobs(record)`.
   - Why: previously a RUNNING app that had started without a job (Docker came up later, or an unsupported runtime) went through `_restart_in_place`. That stopped a cluster that didn't exist, then launched a new one.
   - An app with no job now just settles UPDATING → RUNNING, as it does without Docker.

Test changes in `tests/test_kinesisanalyticsv2.py`:
- `_FakeDataPlane.supports()`.
- New `test_runtime_without_an_image_stays_control_plane_only` (`FLINK-1_18`: the fake's `start` is never called, and the stop snapshot is `READY`).

## The runtime library: fixed directory, no runtime download

Per Luke's decision, the Maven Central fetch in MiniStack is gone. It follows the RDS IAM plugin's artifact-root pattern.

- **`kinesisanalyticsv2_flink.py`**:
  - `_FLINK_LIB_DIR = "/opt/ministack/flink-lib"` (constant; no env var, no config key).
  - `_flink_lib_jars()` returns every `*.jar` in it, or logs one INFO line and returns `[]` when the directory is missing or empty: `no jars in /opt/ministack/flink-lib; jobs must bundle aws-kinesisanalytics-runtime themselves`.
  - `_launch()` puts those jars into both containers at `/opt/flink/lib/` with the same `put_archive` as the properties file.
  - `runtime_library()`, the `urllib` fetch, `hashlib` and the URL and SHA constants are removed.
- **`Dockerfile.full` only**: a new `flink-lib` stage (`python:3.13-slim`, version 1.2.0 and SHA-256 `b3b940a0...be67d` fixed in the RUN step, no build args) downloads the jar into `/opt/ministack/flink-lib/` and runs `sha256sum -c`. `prep` then does `COPY --from=flink-lib /opt/ministack/flink-lib /opt/ministack/flink-lib`, beside the mysql-plugins copy. `Dockerfile` is untouched.
  - Verified: `docker build -f Dockerfile.full --target flink-lib .` gives exit 0 and `aws-kinesisanalytics-runtime-1.2.0.jar: OK` (8504 bytes).
  - Verified: a copy of the Dockerfile with the SHA-256 set to all zeros fails the build, exit 1, `computed checksum did NOT match`.
  - Not run: the full `Dockerfile.full` build. Only the new stage was built.
- **Tests**: `counter_jar` compiles the test job against `aws-kinesisanalytics-runtime-*.jar` found in `_FLINK_LIB_DIR`. It does not bundle it. When the jar is absent it skips: `aws-kinesisanalytics-runtime is not in /opt/ministack/flink-lib; the test job needs it`. No network access at test time.
- **README**: a new "Managed Flink with Real Jobs" section, one paragraph, covering the directory, `:full`, the `-v ./flink-lib:/opt/ministack/flink-lib` mount, and connectors provided the same way.

Evidence the library is needed (docs, not an AWS run):
- **AWS docs**: the Managed Flink developer guide's best-practices page, "Provided dependencies", says the runtime provides `com.amazonaws:aws-kinesisanalytics-runtime:1.2.0` and tells you to use `provided` scope.
- **AWS sample**: `aws-samples/amazon-managed-service-for-apache-flink-examples` `java/GettingStarted/pom.xml` line 47 has `<scope>provided</scope>`.
- **Reproduced locally**: with the library withheld, the test job fails with `ClassNotFoundException: com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime`.

`Dockerfile.full` counts as an infrastructure change. CONTRIBUTING.md wants an issue first, so the issue should cover it.

## Validation (this Mac: Docker Desktop 29.8, arm64; MiniStack on the host)

- **Control-plane lane**, server and pytest with `DOCKER_HOST=unix:///no-existent/docker.sock`: 413 passed, 11 skipped, exit 0.
- **Data-plane lane**, `DOCKER_NETWORK=ministack-flink-local`, `-m data_plane`, with `aws-kinesisanalytics-runtime-1.2.0.jar` (SHA-256 checked) placed by hand in `/opt/ministack/flink-lib`, not committed: 8 passed in 51.1 s, exit 0 (after the d33bbd6 fixes). The server log has no "no jars" line, so the library came from the directory.
- **Same lane before the directory existed**: 6 skipped with the reason above, exit 0.
- **CI gap**: the upstream `data-plane` job runs MiniStack on the runner host, where nothing creates `/opt/ministack/flink-lib`, so these tests will skip in CI. Making them run there needs a CI step, which is an infrastructure change for the issue. The alternative is to accept that they skip upstream.
- **In-process check of `_flink_lib_jars()`**: a missing or empty directory gives `[]` plus one INFO line. With the jar, the archive holds `etc/flink/application_properties.json` and `opt/flink/lib/aws-kinesisanalytics-runtime-1.2.0.jar`.
- `ruff check ministack/`: clean.

## Real AWS validation (2026-10-10)

**Setup:**
- A dedicated, throwaway AWS account (a member of an AWS Organization, used for nothing else), us-east-1, runtime `FLINK-1_20`.
- One script (`validate.py`, not committed) ran the same 48-step lifecycle sequence against MiniStack (with real Docker jobs) and against AWS, recording every response, error and status transition.
- The AWS run took 803 s and cleaned up fully. Afterwards the account had 0 Flink apps, 0 buckets, 0 validation roles and 0 log groups.
- The job is a 3 KB jar: a checkpointed counter into a discarding sink. It reads its properties through `KinesisAnalyticsRuntime`, with that library not bundled.

**Matches (observed on AWS):**
- **Status sequences:**
  - start: STARTING → RUNNING;
  - graceful stop: STOPPING → READY;
  - force stop, both while STARTING and while RUNNING: FORCE_STOPPING → READY;
  - update while RUNNING: UPDATING → RUNNING;
  - a job that throws in `main`: STARTING → READY.
- **Error codes:**
  - duplicate create: `ResourceInUseException`;
  - unknown app: `ResourceNotFoundException`;
  - stop while READY, start while STARTING or RUNNING, update while STARTING: `ResourceInUseException`;
  - restore from a missing custom snapshot: `InvalidArgumentException`;
  - duplicate snapshot name: `ResourceInUseException`;
  - missing snapshot: `ResourceNotFoundException`;
  - stale `CurrentApplicationVersionId` or `ConditionalToken`: `ConcurrentModificationException`;
  - wrong `SnapshotCreationTimestamp` on delete: `InvalidArgumentException`.
- **`RESTORE_FROM_LATEST_SNAPSHOT` with no snapshot** starts fresh and reaches RUNNING; there is no error.
- **Defaults:** checkpoint (enabled, 60000, 5000), monitoring (APPLICATION, INFO), parallelism (1, 1, autoscaling true), `RunConfigurationDescription` after restore, and `ApplicationVersionId` values.
- **The runtime library:** `aws-kinesisanalytics-runtime` is on the AWS classpath, since a jar without it ran. That supports the `/opt/ministack/flink-lib` design with a real AWS observation.

**Differences found, all fixed in commit "fix(kinesisanalyticsv2): match behavior observed on AWS"** (they were in `kinesisanalyticsv2.py`, done with Luke's go):

| # | Step | AWS (now also MiniStack) | MiniStack before |
|---|---|---|---|
| 1 | CreateApplicationSnapshot while READY | `InvalidRequestException` | `ResourceInUseException` |
| 2 | UpdateApplication while RUNNING, snapshots enabled | snapshot `UPDATEAPPLICATION-<app>-<epoch ms>` with the pre-update version | no snapshot record |
| 3 | Graceful stop, snapshots enabled | snapshot `STOP-<app>-<epoch ms>` | `stop-<epoch ms>` |
| 4 | CreateApplicationSnapshot with `SnapshotsEnabled=false` (RUNNING) | accepted | `InvalidApplicationConfigurationException` |
| 5 | A second logging option | `InvalidArgumentException` "Kinesis Analytics currently supports only 1 logging option." | several distinct streams allowed |
| 6 | DeleteApplication with the wrong CreateTimestamp | `ConcurrentModificationException` | `InvalidArgumentException` |
| 7 | Logging option id | `<version the add created>.1` (`5.1`) | `1.1` |
| 8 | Add / delete logging option responses | add: no `OperationId`; delete: only ARN and version | both carried options and `OperationId` |
| 9 | S3 code description | `CodeMD5` and `CodeSize` of the object | absent |
| 10 | Encryption description (app and snapshot) | `{KeyType: AWS_OWNED_KEY}` by default | absent |
| 11 | Maintenance window | `03:00`–`11:00` | absent |
| 12 | ListApplicationSnapshots order | oldest first | newest first |

Error messages for every observed error now use AWS's wording.

**After the fixes:**
- The MiniStack run of the same 48 steps matches the AWS run on all 52 compared items: error codes, status sequences (ignoring the short STOPPING, FORCE_STOPPING and UPDATING states that polling can miss on either side), and automatic snapshot names and versions.
- 0 differences in the compared response fields (describe while READY and RUNNING, logging option add and delete, describe snapshot).
- Lanes: control plane 415 passed, data plane 8 passed (57.1 s), ruff clean.

**Not covered by this run:**
- UpdateApplication while RUNNING with snapshots disabled.
- `AllowNonRestoredState`.
- Runtime `FLINK-1_19`.
- CloudFormation and CDK.
- Autoscaling.

**Found by the MiniStack half of this run, fixed in `d33bbd6` (data plane):**
- **Long names:** container names derived from a long application name exceeded Docker's 64-byte hostname limit, so a 38-character app name failed to start. Names are now `ministack-flink-<cleaned name, 20 chars>-<hash>`, a valid DNS label. Regression test: a 128-character name with `.` and `_`.
- **Stop during start:** a force stop while STARTING left the start thread creating containers after the stop's cleanup, and the next start hit a 409 name conflict. A cancelled start now removes what it created, and a new start waits for the previous one. Regression test: force stop while STARTING, then start again.
- After both fixes: control-plane lane 413 passed, data-plane lane 8 passed (51.1 s), and the MiniStack run of the 48 steps leaves 0 containers.

## Open items

- **Not run**: the CDK and lifecycle-script run against Luke's own deployment. That project lives in a separate private environment and stays out of this repo and these notes. Evidence here comes only from the throwaway account and our own test jobs.
- **Not run**: a forced fresh image pull end to end, and a full `Dockerfile.full` build. Only the new stage was built.
- **Kept**: the throwaway account, for re-validation; it costs nothing while idle.
