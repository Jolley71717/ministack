Title: AppConfig: gradual rollout, alarm rollback and polling behavior

## Use case

We run Spring Boot services on Kubernetes that read configuration from AppConfig through the AppConfig Agent sidecar. We want to use MiniStack to try a canary or linear rollout locally, and to check that a CloudWatch alarm rolls back a bad configuration. Today every deployment completes as soon as it starts, so neither can be tested.

## Current behavior

- `StartDeployment` sets `State: COMPLETE` and `PercentageComplete: 100` immediately. The strategy's duration, growth and bake time are stored but not used.
- `StopDeployment` sets `ROLLED_BACK` on any deployment, including completed ones.
- Environment `Monitors` are stored but never checked.
- A second deployment can start while one is in progress.
- `GetLatestConfiguration` returns the full configuration on every poll, never returns `Version-Label`, and always sets the poll interval to 30 seconds.

## Proposed change

1. Deployments move through `DEPLOYING`, `BAKING` and `COMPLETE`. `LINEAR` steps add `GrowthFactor`, `EXPONENTIAL` steps follow G×2^N, the steps are spread evenly over `DeploymentDurationInMinutes`, and the deployment bakes for `FinalBakeTimeInMinutes`. `PercentageComplete`, `EventLog` and the environment `State` follow the rollout.
2. If a `Monitors` alarm goes to `ALARM` before the bake ends, the deployment is rolled back with `TriggeredBy: CLOUDWATCH_ALARM`. This uses MiniStack's CloudWatch alarms.
3. `StartDeployment` returns `ConflictException` while a deployment is in progress in the environment, or when `LatestDeploymentNumber` is stale.
4. `StopDeployment` rolls back an in-progress deployment. With `AllowRevert` it reverts a completed deployment within 72 hours (`REVERTED`). Otherwise it returns `BadRequestException`.
5. During a rollout, each configuration session gets the new version once `PercentageComplete` covers its fixed slot. `GetLatestConfiguration` returns an empty body when the session already has the latest version, returns `Version-Label`, and uses `RequiredMinimumPollIntervalInSeconds` as the poll interval.
6. A new setting, `APPCONFIG_DEPLOYMENT_MINUTE_SECONDS`, sets the real seconds per strategy minute. The default `0` keeps today's behavior.

## Evidence

From the botocore model (`appconfig` and `appconfigdata`, botocore 1.43.106):
- the deployment state, environment state, event type and `TriggeredBy` enums
- the `StopDeployment` docs on `DEPLOYING`, `AllowRevert`, `REVERTED` and the 72-hour limit
- the `GrowthType` docs and the G×2^N formula
- `EventLog`: "The most recent events are displayed first."
- `GetLatestConfiguration`: "may return empty configuration data if the client already has the latest version"
- `ConflictException` on `StartDeployment`, and the `LatestDeploymentNumber` docs

Inferred, not checked against AWS:
- step timing: step i of n happens at i/n of the duration, which matches the predefined strategies (for example `Linear50PercentEvery30Seconds` has a 1-minute duration)
- one deployment per environment at a time
- an alarm already in `ALARM` rolls back before the first step
- the event description text

MiniStack's choice: AWS does not document how it picks targets, so a session's slot is a hash of the session.

## Agent compatibility

The unmodified AppConfig Agent (`public.ecr.aws/aws-appconfig/aws-appconfig-agent:2.x`, version 2.0.216811) works against MiniStack when `AWS_ENDPOINT_URL` is set. It only calls `StartConfigurationSession` and `GetLatestConfiguration`, so the rollout behavior above reaches every sidecar. I have an implementation ready and will link the PR.

## Out of scope

Validators and `ValidateConfiguration`, non-hosted `LocationUri` sources, extensions and notifications, experiments, enforcing the minimum poll interval, and the agent's entity-based gradual deployments.
