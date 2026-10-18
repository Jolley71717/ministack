# Proposals

Design notes, issue drafts and PR drafts for features proposed to
[ministackorg/ministack](https://github.com/ministackorg/ministack). This branch
never merges to `master`; it is a record of what was pitched, how, and what came
of it. Code lives in the upstream PRs linked below.

One folder per proposal, named `YYYY-MM-<topic>`.

| Proposal | Upstream | Outcome |
|---|---|---|
| [2026-10-appconfig-gradual-rollout](2026-10-appconfig-gradual-rollout/) | [PR #2075](https://github.com/ministackorg/ministack/pull/2075) | Merged 2026-10-07. Maintainers removed the `APPCONFIG_DEPLOYMENT_MINUTE_SECONDS` setting before release: a strategy minute is a real minute, as on AWS. |
| [2026-10-managed-flink](2026-10-managed-flink/) | [Issue #2133](https://github.com/ministackorg/ministack/issues/2133), [PR #2134](https://github.com/ministackorg/ministack/pull/2134), [PR #2135](https://github.com/ministackorg/ministack/pull/2135) | Merged 2026-10-17, released in v1.5.29. |

## Lessons

- Open the scoped issue before writing code (CONTRIBUTING.md requires it for new services).
- No new environment variables or config keys for behavior AWS itself fixes. Maintainers remove them.
- Prefer a fixed directory (the RDS IAM plugin pattern) over a runtime download for artifacts a container needs.
- Label evidence precisely: botocore model, AWS docs, a real AWS run, or inference.
