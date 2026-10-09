# New service: Managed Service for Apache Flink (kinesisanalyticsv2)

Label: `enhancement`

## Service

Amazon Managed Service for Apache Flink, API namespace `kinesisanalyticsv2` (API version 2018-05-23).

Wire format (botocore model): JSON 1.1, `X-Amz-Target: KinesisAnalytics_20180523.<Operation>`, signing name and endpoint prefix `kinesisanalytics`. The v1 API (`kinesisanalytics`, target prefix `KinesisAnalytics_20150814`) shares the signing name, so routing should key on the target prefix. v1 is out of scope.

I found no existing issue or PR for Flink, Kinesis Analytics, or kinesisanalyticsv2 (GitHub search over issues and PRs, open and closed). Nothing in `core/router.py` claims the target prefix or the `kinesisanalytics` scope today.

## Use case

I run Flink jobs on Managed Service for Apache Flink. They read and write Kinesis, Kafka (MSK), S3 and DynamoDB. The deploy path is:

1. A CI job runs a Python CLI.
2. The CLI builds a CDK app and runs `cdk bootstrap` and `cdk deploy`.
3. The CDK stack creates one `AWS::KinesisAnalyticsV2::Application` (jar in S3 as `ZIPFILE`, checkpoint, monitoring and parallelism settings, snapshots enabled, one runtime property group) and one `AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption`, plus an `AWS::Logs::LogStream`.
4. Separate scripts drive the lifecycle with boto3: `StartApplication` with `RESTORE_FROM_LATEST_SNAPSHOT` or `SKIP_RESTORE_FROM_SNAPSHOT`, `StopApplication` with and without `Force`, `UpdateApplication`, and `DescribeApplication` polling. A scheduled job creates snapshots and prunes old ones.

The apps use the `FLINK-1_20` and `FLINK-1_19` runtimes.

We want to test three things locally and in CI:

- the job logic end to end, against MiniStack Kinesis, S3 and DynamoDB
- the deploy and lifecycle: create, start, stop, update, snapshots, runtime properties
- the exact jar we deploy, not a rebuilt test variant

Most of our CI runs on Kubernetes pods without a Docker daemon. There we would get the control plane and the CDK deploy, not job execution (see "Without Docker" below). Running jobs needs Docker, which we have on developer machines and some CI runners.

## Proposed design

Follow the RDS, EKS, MWAA and Glue pattern: start real containers when Docker is available, and fall back to control plane only when it is not. No new setting or env var. Docker detection stays automatic, as in those services.

**Control plane (always on).** In-memory, account and region scoped application records:

- application config, `ApplicationVersionId` (incremented on every change) and `ConditionalToken`, with `ConcurrentModificationException` on a stale version or token
- status transitions using the model's `ApplicationStatus` values: `READY` after create, `STARTING` then `RUNNING` on start, `STOPPING` or `FORCE_STOPPING` then `READY` on stop, `UPDATING` on update of a running app
- snapshot records with `SnapshotStatus` (`CREATING`, `READY`, `DELETING`, `FAILED`)
- logging options and tags
- VPC configuration is accepted and stored, and has no effect on containers

**Data plane (when Docker is available).** On `StartApplication`:

1. Start a JobManager and a TaskManager container for the application, named and labelled with the existing account and region scoped helpers and `container_reaper.own_labels`. The image comes from the runtime: `FLINK-1_20` and `FLINK-1_19` each map to an official Flink image pinned to a patch version, with the same Java version AWS uses for that runtime (to verify before the PR). The image name goes through `apply_image_prefix`, so a mirror or private registry works. An unmapped runtime gives control plane behaviour with a log line.
2. Size the TaskManager so the job can be scheduled: `taskmanager.numberOfTaskSlots` comes from the application's configured parallelism. Without this, a job whose parallelism exceeds the slots waits in `STARTING` forever.
3. Fetch the jar from MiniStack S3 and submit it through the Flink REST API (`POST /jars/upload`, then `POST /jars/:jarid/run`). Pass `savepointPath` for a restore and `allowNonRestoredState` from `FlinkRunConfiguration`.
4. Write the application's property groups to `/etc/flink/application_properties.json` in both containers, as a JSON array of `{"PropertyGroupId": ..., "PropertyMap": {...}}`.
5. Give the containers credentials, region and `AWS_ENDPOINT_URL` pointing at MiniStack, with the same host resolution Glue Spark uses. Set `s3.endpoint` and `s3.path.style.access` in the Flink config for the S3 filesystem.
6. Move to `RUNNING` when the Flink job reports `RUNNING`. If the cluster or job fails to start, show a failed state rather than staying in `STARTING`.

Other operations map to Flink REST calls:

- **Snapshot:** `POST /jobs/:jobid/savepoints` into a per-application directory.
- **Graceful stop:** `POST /jobs/:jobid/stop` with a savepoint when snapshots are enabled.
- **Force stop:** cancel the job without a savepoint.
- **Update of a running app:** stop with a savepoint, apply the new config, and restart from that savepoint.
- **Delete and reset:** remove the containers.
- **MiniStack restart with persistence:** applications saved as `RUNNING` have no containers after a restart (the reaper removes them). On load they go back to `READY`, so a lifecycle script can start them again.

**Without Docker (for example MiniStack in a Kubernetes pod).** The container-backed part does not work there. This is the same limit LocalStack documents for its Docker executor. What still works there:

- every operation in scope, with correct shapes, errors, version ids and status transitions
- CDK and CloudFormation deploys of the application resources
- snapshot records, so lifecycle scripts and tests that check status and snapshot lists can run

In that mode `StartApplication` moves through `STARTING` to `RUNNING` without a cluster, so lifecycle scripts behave as they do on AWS. What does not work there: the job does not run, and a restore restores nothing. The README and a server log line at `StartApplication` should say so plainly.

## Operations in scope

| Operation | Why we need it |
|---|---|
| CreateApplication | CloudFormation create |
| DescribeApplication | CloudFormation, lifecycle polling |
| UpdateApplication | CloudFormation update, config and runtime property changes |
| DeleteApplication | CloudFormation delete |
| StartApplication | lifecycle (`RunConfiguration`, `ApplicationRestoreConfiguration`) |
| StopApplication | lifecycle (`Force`) |
| CreateApplicationSnapshot | snapshot job |
| ListApplicationSnapshots | snapshot pruning |
| DescribeApplicationSnapshot | snapshot status polling |
| DeleteApplicationSnapshot | snapshot pruning |
| AddApplicationCloudWatchLoggingOption | CloudFormation logging option resource |
| DeleteApplicationCloudWatchLoggingOption | CloudFormation logging option resource |
| TagResource, UntagResource, ListTagsForResource | CloudFormation tags |

CloudFormation resource types (registered in `_RESOURCE_HANDLERS`):

- `AWS::KinesisAnalyticsV2::Application`
- `AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption`
- `AWS::Logs::LogStream`. This is not a Flink type, but a CDK stack with a log stream synthesizes it and it is not registered today.

Restore types: `SKIP_RESTORE_FROM_SNAPSHOT` and `RESTORE_FROM_LATEST_SNAPSHOT`. `RESTORE_FROM_CUSTOM_SNAPSHOT` is a small addition if maintainers prefer it in the first PR.

## Out of scope for the first PR

- `ListApplications`, application versions and rollback (`ListApplicationVersions`, `DescribeApplicationVersion`, `RollbackApplication`)
- input, output, reference data source and VPC configuration operations (`AddApplicationInput`, `AddApplicationVpcConfiguration` and the rest)
- SQL and Studio (Zeppelin) applications, `ApplicationMode: INTERACTIVE`
- operations and maintenance APIs (`ListApplicationOperations`, `UpdateApplicationMaintenanceConfiguration`)
- autoscaling and parallelism above what one TaskManager provides
- runtimes other than `FLINK-1_19` and `FLINK-1_20`
- the Kafka wire protocol. MSK in MiniStack stays control plane only. Jobs read bootstrap servers from their own runtime properties (or from `GetBootstrapBrokers`, which returns `MINISTACK_MSK_BOOTSTRAP`), and the broker must be reachable from the Flink containers' Docker network. This adds no new setting.
- the newer `FLINK-2_2` and `FLINK-2_3` runtimes, which the botocore model now lists
- the Terraform `aws_kinesisanalyticsv2_application` resource. It may need a few more operations, which I have not checked. I can raise a follow-up.

## Evidence

| Claim | Evidence |
|---|---|
| Wire format, required fields, enums, error shapes | botocore model 1.43.110, `kinesisanalyticsv2/2018-05-23/service-2.json` |
| `DeleteApplication` requires `CreateTimestamp`, `DeleteApplicationSnapshot` requires `SnapshotCreationTimestamp` | botocore model |
| `StopApplication` with `Force` stops "without taking a snapshot" | botocore model doc string |
| The runtime reads `/etc/flink/application_properties.json` | source code: `com.amazonaws:aws-kinesisanalytics-runtime:1.2.0`, `config.properties` line 19. AWS docs name only `KinesisAnalyticsRuntime.getApplicationProperties()`, not the path. A missing file gives empty properties. |
| The runtime library comes from the Managed Flink runtime, not the user jar | AWS docs, Managed Flink best practices page. Apps that mark it `compileOnly` need it on the cluster classpath. |
| `AWS_ENDPOINT_URL` works for AWS SDK for Java v2 from 2.28.1, and not for v1 | AWS SDK v2 changelog 2.28.1 (2024-09-13), AWS SDK reference guide "Service-specific endpoints" page. Glue's code comment in MiniStack says 2.21, which looks off by a few minors. |
| flink-connector-aws honours it only when the classpath has SDK 2.28.1 or later and `aws.endpoint` is not set | source code: flink-connector-aws v5.0.0 pins SDK 2.26.19 in `pom.xml` and does not bundle it. `AWSClientUtil` calls `endpointOverride` only when `aws.endpoint` is set. The rest is inference. |
| The Flink S3 filesystem ignores `AWS_ENDPOINT_URL` | source code: `flink-s3-fs-hadoop` 1.20.0 shades AWS SDK v1. Hence `s3.endpoint` in the Flink config. |
| Flink REST endpoints and parameters | Apache Flink 1.20 REST API docs |
| LocalStack runs a JobManager and TaskManager container per application, and its Docker executor does not work when LocalStack runs in Kubernetes | LocalStack docs, kinesisanalyticsv2 page. The same page lists a separate Kubernetes executor, Ultimate plan only, and snapshots as not implemented. |
| The Java version AWS runs for `FLINK-1_19` and `FLINK-1_20` | to verify before the PR. The image tag must match it, or a jar can pass locally and fail on AWS. |
| Image pulls can be rate limited | observed: ECR Public answered `toomanyrequests: Data limit exceeded` during anonymous tag checks, and Docker Hub limits anonymous pulls. `apply_image_prefix` covers mirrors. |
| `flink:1.20-java17` starts a JobManager and TaskManager pair in about 13 s at about 650 MB idle, and a jar submitted through the REST API runs | a local spike on Docker, image `public.ecr.aws/docker/library/flink:1.20-java17` (1.4 GB). Not AWS. |
| Docker auto-detection and control plane fallback | existing code: `mwaa.py`, `eks.py`, `rds.py`, `glue.py` |

None of this was validated against a real AWS account. Before the PR I will run the same lifecycle sequence against AWS and record the status transitions and error codes, including:

- `StartApplication` with `RESTORE_FROM_LATEST_SNAPSHOT` when no snapshot exists
- `CreateApplicationSnapshot` when the application is not running, or snapshots are disabled
- `StartApplication` on an application that is already running
- `UpdateApplication` while the application is `STARTING`

## Alternatives considered

- **Flink MiniCluster in the test JVM, with MiniStack for I/O.** Fast and good for job logic. It does not test the deploy, the lifecycle, the runtime property file, or the classpath of the jar that actually ships. It stays useful for unit tests, but it does not cover this use case.
- **Control plane only.** Works everywhere, including Kubernetes, and covers deploy and lifecycle. It never runs the job. We propose it as the fallback, not the whole service.
- **One shared Flink cluster for all applications.** Lighter on memory. Applications would share classpath, slots and restarts, and one failing job could affect another. AWS runs one cluster per application, and so does LocalStack. Rejected.
- **Bring your own Flink endpoint.** Needs a new setting, which goes against the direction in PR #2075 and AGENTS.md. `StartApplication` and `StopApplication` would no longer own the cluster lifecycle, so status and snapshots would drift from what the job is doing. Rejected.

## Test plan

Control plane tests (`tests/test_kinesisanalyticsv2.py`, no marker, run in the existing parallel lane):

- create, describe, update, delete, with required fields and model errors (`ResourceNotFoundException`, `ResourceInUseException`, `InvalidArgumentException`)
- `ConcurrentModificationException` on a stale `CurrentApplicationVersionId` or `ConditionalToken`
- start and stop status transitions, including `Force`
- snapshot create, list, describe, delete, and restore type validation
- logging option add and delete, tags
- a CloudFormation template with `AWS::KinesisAnalyticsV2::Application`, `ApplicationCloudWatchLoggingOption` and `AWS::Logs::LogStream`: create, update a runtime property, delete
- account and region isolation, and `get_state` / `load_persisted_state` / `reset` (`tests/test_persistence.py`)

Data plane tests (`@pytest.mark.data_plane`, `skipif(not DOCKER_NETWORK)`, run in the existing `data-plane` CI job):

1. **Run a job.** Upload a Flink jar to MiniStack S3, create and start the application, wait for `RUNNING`, stop it, wait for `READY`, and check that the containers are gone after delete.
2. **Properties and endpoint.** A small job reads one runtime property and writes it to a MiniStack Kinesis stream. The test reads the record back. This covers the property file and the endpoint injection.
3. **Snapshot and restore.** Create a snapshot, stop, start with `RESTORE_FROM_LATEST_SNAPSHOT`, and check that the job resumed from the savepoint (for example, a counter continues).

CI cost: the image is pulled once per shard, since the job has no pre-pull step. Anonymous pulls can be rate limited, so the tests should skip with a clear reason when the pull fails rather than fail. Each test starts one application (about 650 MB, 13 s) and cleans up in `finally`, which fits the 20 minute shard timeout.

## Questions for maintainers

1. **Test jar.** Test 1 can use an example jar that ships in the Flink image (`/opt/flink/examples/streaming/WordCount.jar`), so it needs no build step. Tests 2 and 3 need a job that reads a property and writes to Kinesis. Would you rather we compile a small job in test setup, or commit a small prebuilt jar under `tests/fixtures`?
2. **Runtime library.** On AWS, `com.amazonaws:aws-kinesisanalytics-runtime` (the library apps use to read runtime properties) is already on the cluster classpath, so apps often leave it out of their jar. To run those jars unchanged, MiniStack would need to put that existing Apache 2.0 jar from Maven Central into the Flink `lib` directory. Is downloading and caching it on first use acceptable, or is there a preferred way to provide a jar to a container?
3. **Snapshot storage.** Is a per-application local directory acceptable for snapshots, or should they go to a MiniStack S3 bucket? A local directory avoids the S3 filesystem endpoint configuration.
