# Managed Flink support in MiniStack

## Problem

We run our streaming jobs on Amazon Managed Service for Apache Flink. MiniStack has no support for that service. Today the only way to test a Flink deploy, its lifecycle, or the real job is against AWS itself, which is slow, costs money, and cannot run in CI or on a laptop.

## Goal

Let a developer or a CI pipeline do three things against MiniStack, with no AWS account:

1. **Deploy** the Flink application with our existing CDK script, unchanged.
2. **Operate** it: start, stop, update, snapshot and restore, with the same API calls and status changes as AWS.
3. **Run the real job**: the exact jar we ship, reading and writing MiniStack's Kinesis, S3 and DynamoDB, so we can check its output end to end.

## What we will add

- **The Managed Flink API** (`kinesisanalyticsv2`). Only the calls our deploy and lifecycle scripts use, behaving like AWS.
- **CloudFormation support** for the Flink application resources, so `cdk deploy` works against MiniStack.
- **Real job execution.** When Docker is available, starting an application launches the official Apache Flink image and runs our jar in it, connected to MiniStack's services.

## How

- **Reuse, don't invent.** We build no Flink engine and no new platform. We use the official Flink image, and the pattern MiniStack already uses for RDS, EKS and Glue: run real containers when Docker is present, fall back automatically when it is not. No new settings, which matches the maintainers' stated direction.
- **Graceful fallback.** Without Docker (for example inside a Kubernetes pod), the API and the CDK deploy still work fully. Only the job itself does not run, and we say so plainly.
- **Prove it.** Automated tests for the API and the deploy, plus tests that run a real job in MiniStack's existing Docker-enabled CI. Before the code PR, run the same lifecycle against real AWS once and match its behavior.

## Out of scope for now

Flink SQL and Studio notebooks, autoscaling, Flink runtimes other than 1.19 and 1.20, and running jobs inside Kubernetes without Docker.

## Steps

1. Open the scoped GitHub issue and agree the scope with the maintainers.
2. Build the API and CloudFormation support (works everywhere).
3. Add real job execution (where Docker is available).
4. Validate against real AWS, then raise the PR.

## Risks

- **Kubernetes CI** gets the API and the deploy, not job execution, unless the pods have Docker.
- **Resources:** each running application costs about 650 MB of RAM and roughly 15 seconds to start.
- **Maintainers may push back on scope**, which is why the issue comes first.
