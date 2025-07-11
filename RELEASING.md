- [Overview](#overview)
- [Branching](#branching)
  - [Release Branching](#release-branching)
  - [Feature Branches](#feature-branches)
- [Release Labels](#release-labels)
- [Releasing](#releasing)
- [Snapshots](#snapshot-builds)

## Overview

This document explains the release strategy for artifacts in this organization.

## Branching

### Release Branching

Given the current major release of 1.0, projects in this organization maintain the following active branches.

- **main**: The next _major_ release. This is the branch where all merges take place and code moves fast.
- **1.x**: The next _minor_ release. Once a change is merged into `main`, decide whether to backport it to `1.x`.
- **1.0**: The _current_ release. In between minor releases, only hotfixes (e.g. security) are backported to `1.0`.

Label PRs with the next major version label (e.g. `2.0.0`) and merge changes into `main`. Label PRs that you believe need to be backported as `1.x` and `1.0`. Backport PRs by checking out the versioned branch, cherry-pick changes and open a PR against each target backport branch.

### Feature Branches

Do not creating branches in the upstream repo, use your fork, for the exception of long lasting feature branches that require active collaboration from multiple developers. Name feature branches `feature/<thing>`. Once the work is merged to `main`, please make sure to delete the feature branch.

## Release Labels

Repositories create consistent release labels, such as `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `patch` and `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md#triage-open-issues) for more information on triaging issues.

## Releasing

The release process is standard across repositories in this org and is run by a release manager volunteering from amongst [MAINTAINERS](MAINTAINERS.md).

1. Create a tag, e.g. `v2.1.0`, and push it to the GitHub repo.
1. The [release-drafter.yml](.github/workflows/release-drafter.yml) will be automatically kicked off and is responsible for drafting a new release on GitHub containing release artifacts.
1. Before creating a draft release, this workflow creates a GitHub issue asking for approval from the [maintainers](MAINTAINERS.md). See sample [issue](https://github.com/gaiksaya/opensearch-java/issues/1). The maintainers need to approve in order to continue the workflow run.
1. Once a release is drafted [opensearch-hadoop-release](https://build.ci.opensearch.org/job/opensearch-hadoop-release/) jenkins workflow is triggered. The artifacts will be automcatically signed and published to maven.
1. Increment the version in [opensearch-hadoop-version.properties](./buildSrc/opensearch-hadoop-version.properties) to the next patch release, e.g. `2.1.1`.

## Snapshot Builds
The [snapshots builds](https://central.sonatype.com/service/rest/repository/browse/maven-snapshots/org/opensearch/client/) are published to sonatype using [publish_snapshot.yml](./.github/workflows/publish_snapshot.yml) workflow. Each `push` event to the main branch triggers this workflow.