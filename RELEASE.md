# Boxo Releases

## Release Policies
### Breaking Changes
TLDR: APIs may break between changes.

There are two kinds of breaking changes to consider:

1. **API refactors/changes**. There is equivalent functionality, but the API has changed. Generally adapting to these changes is trivial, and release notes will contain information about how to adapt.

1. **Removal of a module or functionality**. In this case, Boxo maintainers will deprecate the relevant types in at least one release prior to their removal, and users will be forewarned in release notes. This will not be done casually and justification will be provided.

We know that breaking changes may cause short-term pain, and we only make breaking changes when we believe the long-term gain is much greater than the short-term pain.

Feedback from users helps us in this determination, so *please* provide feedback on any GitHub issues or PRs that may affect you, and let us know when we're wrong! We believe there's *a lot* of innovation and growth for IPFS in the future, so we don't want Boxo to ossify.

### Versioning
Boxo only releases minor and patch versions, and has no plans to change major versions from v0.

Minor versions contain new or removed functionality, or significant changes in dependencies.

Patch versions contain "fixes", which are generally small, targeted, and involve limited changes in functionality (such as bug fixes, security fixes, etc.). 

### Backporting
The amount of backporting of a fix depends on the severity of the issue and the impact on users. Most non-critical fixes won't be backported.

As a result, Boxo maintainers recommend that consumers stay up-to-date with Boxo releases.

### Go Compatibility
At any given point, the Go team supports only the latest two versions of Go released (see https://go.dev/doc/devel/release). Boxo maintainers will strive to maintain compatibility with the older of the two supported versions, so that Boxo is also compatible with the latest two versions of Go.

### Release Criteria
Boxo releases occur _at least_ on every Kubo release. Releases can also be initiated on-demand, regardless of Kubo's release cadence, whenever there are significant changes (new features, refactorings, deprecations, etc.).

The [release process](#release-process) below describes how maintainers perform a release.

Boxo may release on Fridays or even weekends. This is in contrast to Kubo the deployable binary, which does not release on Fridays to avoid weekend surprises.

### Testing
Boxo maintainers are in the process of moving tests into Boxo from Kubo (see [#275](https://github.com/ipfs/boxo/issues/275)) . Until that's done, Boxo releases rely on Kubo tests for extra confidence. As a result releases must plumb Boxo into Kubo and ensure its tests pass. See [Release Process](#release-process) for details.

### Changelogs
Boxo loosely follows the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format. New commits should add changelog entries into the `[Unreleased]` section so that there is no changelog scramble when a new version needs to be released.

We take to heart the "guiding principle" that "changelogs are for humans, not machines.".  As a result:
* Breaking changes and notable features are called out following the changelog emoji legend.
* Changelog entries provide links to a relevant issue/PR so an interested party can learn more.
We intend to use the standardized changelog to automate releases by onboarding with [Changelog Driven Release](https://github.com/pl-strflt/changelog-driven-release) (see [#269](https://github.com/ipfs/boxo/issues/269)).

At least as of 2023-06-08, changelog test is manually copied from [the changelog](CHANGELOG.md) to https://github.com/ipfs/boxo/releases.  This means that any updates to the changelog after a release need to be manually copied as well.

### Related Work
Below are links of related/adjacent work that has informed some of the decisions in this document:
1. https://github.com/ipfs/boxo/issues/170
2. https://ipshipyard.notion.site/Kubo-Release-Process-6dba4f5755c9458ab5685eeb28173778
3. https://github.com/ipfs/kubo/blob/master/docs/RELEASE_ISSUE_TEMPLATE.md

## Release Process
1. Create an issue for the version: https://github.com/ipfs/boxo/issues/new?title=Release+X.Y.Z (can be blank for now)
2. Pin the issue
3. Copy-paste the following checklist into the description:

- [ ] Verify your [GPG signature](https://docs.github.com/en/authentication/managing-commit-signature-verification) is configured in local git and GitHub
- [ ] Ensure Boxo and Kubo are checked out on your system
- [ ] Create a release branch called `release-vX.Y.Z` and push it
  * Use `main` as base if `Z == 0`
  * Use `release-X.Y.Z-1` as base if `Z > 0`
- [ ] Tidy the changelog and push any changelog changes to `release-vX.Y.Z`
- [ ] Create a draft PR merging `release-vX.Y.Z` into `main`
  - [ ] Name it "Release vX.Y.Z"
  - [ ] Reference this release issue
- [ ] If `Z > 0`, then cherry-pick the necessary commits from `master` into `release-vX.Y.Z` using `git cherry-pick -x <commit>`
- [ ] Ensure Boxo tests are passing
- [ ] Ensure Kubo tests are passing
  - [ ] Go to Kubo dir and run `go get github.com/ipfs/boxo@<commit_hash>` using the commit hash of the `release-vX.Y.Z` branch
  - [ ] Run `make mod_tidy` in repo root (to apply `go mod tidy` to code, tests, and examples)
  - [ ] Commit the changes and open a draft PR in Kubo
  - [ ] Name the PR "Upgrade to Boxo vX.Y.Z"
  - [ ] Paste a link to the Kubo PR in the Boxo PR, so reviewers can verify the Kubo test run
  - [ ] Verify the CI passes
- [ ] Add a commit in `release-vX.Y.Z` bumping the version in `version.json` to `vX.Y.Z`
- [ ] Add a "release" label to the Boxo PR
- [ ] Check for warnings from gorelease
  - [ ] Ensure any warnings of breaking changes, such as moved packages, are properly documented.
- [ ] After the release checker creates a draft release, copy-paste the changelog into the draft
- [ ] Wait for approval from another Boxo maintainer
- [ ] Merge the PR into `main`, _using "Create a Merge Commit"_, and do not delete the `release-vX.Y.X` branch
  - [ ] Verify the tag is created
- [ ] Announce the release
  - [ ] Click [this link](https://discuss.ipfs.tech/new-topic?title=Boxo%20vX.Y.Z%20is%20out%21&tags=boxo&category=News&body=%23%23%20Boxo%20vX.Y.Z%20is%20out%21%0A%0ASee%3A%0A-%20Code%3A%20https%3A%2F%2Fgithub.com%2Fipfs%2Fboxo%2Freleases%2Ftag%2FvX.Y.Z%0A-%20Release%20Notes%3A%20https%3A%2F%2Fgithub.com%2Fipfs%2Fboxo%2Fblob%2FvX.Y.Z%2FCHANGELOG.md) to start a new Discourse topic <!--docs: https://meta.discourse.org/t/create-a-link-to-start-a-new-topic-with-pre-filled-information/28074 -->
    - [ ] Update `vX.Y.Z` in the title and body
	- [ ] Create the topic
- [ ] Update the Kubo PR to use the newly released version, mark it as "Ready for Review", get approval, and merge into Kubo
