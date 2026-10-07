Title: feat(appconfig): gradual rollout, alarm rollback and polling behavior

Base: ministackorg/ministack main
Head: Jolley71717/ministack feat/appconfig-gradual-rollout

---

Closes #<issue number>

## Changes

### Rollout (`ministack/services/appconfig.py`)

- Deployments used to be marked `COMPLETE` as soon as they started. They now move through `DEPLOYING`, `BAKING` and `COMPLETE`.
- `LINEAR` steps add `GrowthFactor`. `EXPONENTIAL` steps follow G×2^N. Steps are spread evenly over `DeploymentDurationInMinutes`, and the last one reaches 100% when the duration ends. The deployment then bakes for `FinalBakeTimeInMinutes`.
- `PercentageComplete`, `EventLog` (newest first), `CompletedAt` and the environment `State` follow the rollout.
- Progress is worked out from the stored start time whenever the deployment is read, so it survives a restart with persistence on.
- `StartDeployment` returns `ConflictException` while a deployment is in progress in the environment, or when `LatestDeploymentNumber` is stale.

### Alarm rollback (`appconfig.py`, `cloudwatch.py`)

- If a `Monitors` alarm goes to `ALARM` before the bake ends, the deployment is rolled back at the time the alarm fired, and no later steps are taken. The rollback events have `TriggeredBy: CLOUDWATCH_ALARM` and the alarm ARN in the description.
- The new `cloudwatch.first_alarm_time()` reads the alarm's state history. An alarm that fired and recovered between two reads still triggers the rollback.

### StopDeployment

- An in-progress deployment is rolled back.
- A completed deployment can be reverted with `Allow-Revert` within 72 hours of completing (`REVERTED`).
- Anything else returns `BadRequestException`. Before this change it marked any deployment `ROLLED_BACK`.

### Data plane

- Each session has a fixed slot from 0 to 99 and gets a `DEPLOYING` version once `PercentageComplete` covers its slot. `BAKING` and `COMPLETE` versions go to every session. After a rollback or revert, sessions get the previous completed version.
- `GetLatestConfiguration` returns an empty body when the session already has the version, and returns `Version-Label`.
- `RequiredMinimumPollIntervalInSeconds` (15 to 86400) sets `Next-Poll-Interval-In-Seconds`. The default stays 30.
- `CreateHostedConfigurationVersion` stores `VersionLabel`, and the get and list calls return it.

### Setting

`APPCONFIG_DEPLOYMENT_MINUTE_SECONDS` is the number of real seconds per strategy minute. It can also be set through `/_ministack/config` as `appconfig._DEPLOYMENT_MINUTE_SECONDS`. The default `0` keeps the current behavior, where deployments complete immediately.

## Evidence

From the botocore model (`appconfig` and `appconfigdata`, botocore 1.43.106):
- the state, event type and `TriggeredBy` enums
- the `StopDeployment` docs on `DEPLOYING`, `AllowRevert`, `REVERTED` and the 72-hour limit
- the `GrowthType` docs and the G×2^N formula
- `EventLog` order ("The most recent events are displayed first.")
- the empty `GetLatestConfiguration` response when the client already has the latest version
- the `ConflictException` and `LatestDeploymentNumber` docs
- the 15 to 86400 range for `RequiredMinimumPollIntervalInSeconds`

Inferred, not checked against AWS:
- step timing (step i of n at i/n of the duration), which matches the predefined strategy names and durations
- one deployment per environment at a time
- an alarm already in `ALARM` rolls back before the first step
- the event description text

MiniStack's choice: AWS does not document how it picks targets, so a session's slot is a SHA-256 hash of the session.

## Testing

- New `tests/test_appconfig_rollout.py` with 18 tests. Most call the handlers directly with a fake clock at 60 seconds per strategy minute. They cover:
  - stepping, baking and completion
  - the event log
  - conflicts and `LatestDeploymentNumber`
  - stop and revert, including the 72-hour limit
  - alarm rollback: an alarm that fires mid-rollout, one that recovers during the bake, one that fires after completion, one already firing, and an unknown alarm
  - rollout targeting across 60 sessions
  - the empty response, `Version-Label` and the poll interval

  Three tests use boto3 against the running server.
- Removing the alarm check or the empty-response check makes 5 of the new tests fail.
- `test_appconfig_stop_deployment` in `tests/test_appconfig.py` now expects `BadRequestException` without `AllowRevert`, and `REVERTED` with it.
- `pytest tests/test_appconfig.py tests/test_appconfig_rollout.py tests/test_cloudwatch.py tests/test_persistence.py`: 495 passed, 11 skipped.
- `ruff check ministack/` passes.
- Full parallel run (`-m "not serial and not data_plane"`): 137 failures.
  - 134 of them also fail on the base commit `434330e` in the same environment (API Gateway, CloudFront, MWAA, S3, Lambda, EKS, IoT data).
  - The other 3 pass when run on their own.
- Real AppConfig Agent (`public.ecr.aws/aws-appconfig/aws-appconfig-agent:2.x`, version 2.0.216811), with MiniStack at `APPCONFIG_DEPLOYMENT_MINUTE_SECONDS=1`:
  - The agent works when `AWS_ENDPOINT_URL` points at MiniStack. It calls `StartConfigurationSession`, passing its `POLL_INTERVAL` as `RequiredMinimumPollIntervalInSeconds`, then `GetLatestConfiguration`, and serves the configuration with `Version-Label` on port 2772.
  - Rollout with 5 agents and a linear strategy of 20% steps over 100 seconds plus a 30-second bake: agents switched to the new version as the rollout reached them (1, then 2, then 3), about one 15-second poll after each step. All 5 had it during the bake, and the deployment completed.
  - Rollback with the same 5 agents mid-rollout: `PutMetricData` pushed the monitored alarm into `ALARM`. The deployment and environment went to `ROLLED_BACK`, and agents on the new version went back to the previous one within one poll.
  - On its first fetch the agent logs `configuration data missing timestamp, defaulting to 'now'`. It expects a response header that is not in the botocore model, and nothing else is affected.

## Out of scope

Validators and `ValidateConfiguration`, non-hosted `LocationUri` sources, extensions and notifications, experiments, enforcing the minimum poll interval, and the agent's entity-based gradual deployments.
