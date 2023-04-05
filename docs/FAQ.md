<!-- omit in toc -->
## Table of Contents
- [Rationale FAQ](#rationale-faq)
  - [What were the problems with the \<2023 Go IPFS repo set?  Why take action now? What's changed from 2022 and earlier?](#what-were-the-problems-with-the-2023-go-ipfs-repo-set--why-take-action-now-whats-changed-from-2022-and-earlier)
  - [Are there other reaons/goals for doing repo consolidation?](#are-there-other-reaonsgoals-for-doing-repo-consolidation)
  - [Why is the Boxo project consolidating repos and using example-driven development despite ecosystem friction?](#why-is-the-boxo-project-consolidating-repos-and-using-example-driven-development-despite-ecosystem-friction)
  - [What are the risks in repo consolidation?](#what-are-the-risks-in-repo-consolidation)
  - [How much does a repo cost?](#how-much-does-a-repo-cost)
  - [What alternatives were considered and why were they ruled against?](#what-alternatives-were-considered-and-why-were-they-ruled-against)
  - [Given this isn't free for maintainers or those affected, what has been the cost/benefit analysis?](#given-this-isnt-free-for-maintainers-or-those-affected-what-has-been-the-costbenefit-analysis)
  - [What are some concrete usecases where repo consolidation would have been beneficial?](#what-are-some-concrete-usecases-where-repo-consolidation-would-have-been-beneficial)
  - [Is this really for developers or just maintaienrs?](#is-this-really-for-developers-or-just-maintaienrs)
  - [What kind of user outreach was done about this consolidation work?](#what-kind-of-user-outreach-was-done-about-this-consolidation-work)
- [Copied or Migrated Repos FAQ](#copied-or-migrated-repos-faq)
  - [What repos have been copied into Boxo?](#what-repos-have-been-copied-into-boxo)
  - [What will happen if there is a security issue in one of the legacy ipfs/\* repos that has been copied into boxo?](#what-will-happen-if-there-is-a-security-issue-in-one-of-the-legacy-ipfs-repos-that-has-been-copied-into-boxo)
  - [I don't like the "deprecated" warnings in "not maintained" repos that have been moved to boxo.  What can be done about this?](#i-dont-like-the-deprecated-warnings-in-not-maintained-repos-that-have-been-moved-to-boxo--what-can-be-done-about-this)
  - [How does one claim ownership of a "not maintained" repo?](#how-does-one-claim-ownership-of-a-not-maintained-repo)
  - [Will the "not maintained" ipfs/\* repos be left around to rot?](#will-the-not-maintained-ipfs-repos-be-left-around-to-rot)

## Rationale FAQ
These provide more information on the history and justification for this project, and particularly the repo consolidation aspect.

### What were the problems with the <2023 Go IPFS repo set?  Why take action now? What's changed from 2022 and earlier?
**Background:**
The existing layout with many repos was done to encourage flexibility, extensibility, and experimentation. Functionality of IPFS could be reused in other projects without depending on Kubo as a whole.  Those repos predate most Go tooling though.

The maintainers have hit a breaking point on two fronts:

**Maintenance cost:**
Unfortunatelhe dependency tree to to pull off an IPFS implemetnation like Kubo is huge.  (See https://github.com/ipfs/kubo/issues/8543#issue-1048101154 for the nastiness, but at the time of analysis, this was 47 modules under github.com/ipfs.).  You can argue that Kubo is a kitchen sink we should move away from and at least not the standard we should have others model against.  This doesh't change that we have an increasing amount of repos [repos aren't free](#how-much-does-a-repo-cost).  

Problems we observed:
1. When needing/wanting to make a refactor, changes must be propagated across many repos, in the right order.  The cost and risk of bubbling changes around between dozens of repos and versions is so high that even PL folks try to avoid making changes to them, which is not a healthy dynamic. 
2. Test coverage was often happening at higher levels like Kubo which would require manually testing the change in Kubo or waiting until the depencny had been updated in Kubo.  (Resulting fixes from failed tests would then requie similar bubbleup for validation)
3. While github tooling has gotten better to pull in issues and PRs across repos, and the Ecosystem Dashboard helps as well, there is still work on staying on top of multiple repos.  Labeling, milestones, permissions, settings, CI setup, etc. are all magnified x 50.
4. Repos didn't have clear ownership of was owning what.  They were best-effort to maintain and keep up-to-date, leading to complex dependency graphs due to different versions floating around.
5. Some are in various states of deprecation, which adds to the maintenance costs and the cost of implementing new features.  Some don't build due to flaky tests, with not enough incentive to fix them until it becomes a blocker.

This has discoraged experimentation or cleanup.  Friction in maintence has resulted in the spiraling out that we have observed because it gets too costly and the "band aid" approach is taken.  

**User challenges to help themselves:**

In the 2022 push to have more IPFS implementations and less bottlencecking on Kubo, we saw too many cases where users didn't go down the path of helping themselves and instead lived with sub-optimal solutions, waited on Kubo updates, or gave up.  Anecdotes were heard spending time in chat channels, forums, GitHub issues, in-person events, and engaging either directly or indirectly with companies and hackathon builders operating on our stack. Many of these people find building an IPFS implementation with just the parts they need hard, and some have explicitly flagged the many repos as a problem.  (We admittedly haven't done a good job cataloging these anecdotes, but more recently have been building up an incomplete list [here](#what-are-some-concrete-usecases-where-repo-consolidation-would-have-been-beneficial)).

We want to lower the barrier to entry so that people will use these libraries and refactor/contribute instead of using Kubo when it's not appropriate or avoiding the ecosystem altogether.  There were cases where the number of repos and their version inconsistencies was overwhelming.  This wasn't surprising as we could resonate, even as people who work with them full-time. 

We believe that putting IPFS things in one place (as much as possible/practical) and treating them as one cohesive product, testing them together, and ensuring version consistency will result in a much better experience for other devs who want to build applications and implementations on top of these libraries. There will be pain as we make this transition, and some ambiguities to work out, but we think the end result for the community and users will be more than worth it.

In addotopm. a welcome answer to help with these exasperations is that Go modules now exist, along with [module graph pruning](https://golang.org/ref/mod#graph-pruning). The latter is key to preventing consumers from having an explosion of transient dependencies if they just want to reuse some small piece of code.

### Are there other reaons/goals for doing repo consolidation?
Not really but there are some related angles we also look at this.

A key message and theme from IPFS Thing 2022 onwards is that IPFS is not Kubo and that PL EngRes IPFS Stewards need to foster other implementations.  As a result, the PL EngRes IPFS Stewards, the maintainers of most IPFS org repos, are shifting focus to provide a first-class library experience, to enable users to build their own binaries/implementations instead of piling everything into Kubo.  Unforuntately existing libraries are hit-or-miss, with large amounts of tech debt (poor testing, unsafe APIs, multiple versions/forks, half-completed migrations, "experimental" features sitting unfinished, etc.). We want to refactor these but refactoring is very time consuming given the large number of repos and versions involved.  Building a quality library also requires ;ibrary components being tested together.

We also need to grow the Go IPFS community to include more than just PL EngRes engineers.  Per above, the large number of repos and versions is intimidating to newcomers.  It is difficult to understand the impact of a change across all the repos+versions.  There is no cross-repo testing feedback until you plumb everything up to Kubo.  A repo's owneship and status has also not been consistently clear.

### Why is the Boxo project consolidating repos and using example-driven development despite ecosystem friction?
We believe that the current Go ipfs ecosystem is suboptimal due to issues like incompatible versions, complex APIs, and hard-to-navigate projects. We think a shared monorepo following an example-driven development will create a better ecosystem. Although the transition causes friction, we developed a [migration tool](https://github.com/ipfs/boxo/issues/189) to help with migration.  We believe the tradeoff is worthwhile for long-term ecosystem improvements, but the validation of this approach won't come until [evaluating metrics and user anecdotes 3-6 months after](https://github.com/ipfs/boxo/issues/203).

### What are the risks in repo consolidation?
1. Users will be broken by import paths changing.
   - Mitigation: per https://github.com/ipfs/boxo/issues/202, most repos are copied in so users can live with the status quo if they like (albeit on "no maintainer" code).  There is also [tooling to help someone upgrade](https://github.com/ipfs/boxo#migrating-to-boxo).

2. Unable to track individual releases and changelogs from individual subcomponents
   - Mitigation: changelogs can be made at the module level.  rust-libp2p has been successful with this for example.

3. Opportunity to get sloppy about what interfaces to expose
    - Mitigation: relying on code reviews and maintainers making good judgement and then being quick to change when they mess up.

4. Applications that only need a sliver of functionality still needs to wait until the monorepo releases things, and the monorepo can only release things when all the functionality is aligned.
   - Mitigation: establigh a [predictable release process](https://github.com/ipfs/boxo/issues/170) that improves and gets better with eacher iteration.  The maintainers have a track record with doing this Kubo, which has a more ellaborate release process.

5. Lack of clarity about a module's status and that there is an inconsistent quality bar across modules.
    - Mitigation: we're relying on maintainers to [hold the standard for what can be added to boxo](https://github.com/ipfs/boxo/issues/219).  Followup work will also be happening to [make clear a module's stability](https://github.com/ipfs/boxo/issues/219). 

6. Boxo becomes a kitchen sink (like Kubo)
    - Mitigation: rely on maintainer judgement and community input to avoid this.  Wone of the guides for whether to include functioanlity is if matches to an example that has been asked for.  For exaample, not all Kubo dependencies have been pulled in to Boxo.  And where we get this wrong, we'll take the hit to move things out following [responsible but willing deprecation](#what-is-the-deprecation-and-breaking-change-policy).  We acknowledge that getting this wrong has costs for all involved and isn't to be taken lightly.  [Migration tooling](https://github.com/ipfs/boxo#migrating-to-boxo) will continue to be leveraged to reduce the burden.
    - Note: we believe doing this at a library and binary level are pretty different things. A binary level “put everything in here” allows for very little choice in what you support and results in either one-size-misfits-all defaults or unwieldy config files. A repo that has tons of sub-packages is not really that; you can use the ones you want and not use the ones you don’t.  We're still relying on maintainers to make careful consideration of cross-package dependencies so that just because they live together and version together doesn’t mean all depend on each other.  It's true that we're relying "best intentions" currently.  [#258](https://github.com/ipfs/boxo/issues/258) will help mechanize this.

7. Modules in Boxo become ossified
    - Mitigation: lean into [responsible but willing deprecation](#what-is-the-deprecation-and-breaking-change-policy) to remove functionality that is providing more drag than gain.

### How much does a repo cost?
Repo maintenance costs include:
- Keeping dependencies up-to-date
    - Yes dependabot helps create PR but those PRs still need to be reviewed/merged.  Flaky tests or depending on CI test coverage at the root Kubo layer adds complexist complex.
    - This is also non-trivial as it often requires chasing down other dependencies in the dependency graph...  In practice mostly we don't do this until we have to
- Releasing new versions as necessary.
    - Yes unified CI workflows help here but this isn't free. 
- Making sure CI is still working.
  - Yes this still applies in a monorepo and will be non-green more often, but it also provides a single place to focus and eradicate flakiness and make improvements.
- Maintenance/upgrades of CI (e.g., migrating from Travis/CircleCI to Actions)
- In some cases backporting changes across major versions as necessary
- In some cases manually testing impact of new code changes on downstream consumers 
   - Yes this still needs to still happen from a monorepo but it's less net steps. 
- Monitoring issue trackers, PRs, etc.
   - Yes, Github tooling has gotten better and the IPFS Ecosystem Dashboard helps. 
- Updating submodules
    - Commonly used for testing, example code, etc.
    - Often these contain circular module dependencies which complicate propagating breaking changes

### What alternatives were considered and why were they ruled against?
The primary alternatives proposed and considered centered around focus on docs/examples in a repo with a known set of versions that play nice together.

Concern 1: While this would showing users how they can get started, it wouldn't help the maintainers make things easier based on the learning of those examples because they might hit friction from the dependency sprawl.  Maintainers have already seen opportunities to improve as a result of dogfooding the library code in making examples.  We don't want there to be anything code organization wise to stand in the way from making improvements. 

Concern 2: We could release a library called `kubo-maintainers-blessed-package-versions-examples-only-no-code-included` and ask everyone to import that and then rewrite the README on every repo to say "the maintainers of this repo recommend importing `kubo-maintainers-blessed-package-versions-examples-only-no-code-included` to fix your versioning; if you don't do this you're on your own" but this unintuitive, doesn't work if someone accidentally imports a higher version from somewhere else, and is not how anyone else in the Go ecosystem does this which means its unlikely our users will as well (e.g. due to habits, IDE integrations, etc.).

### Given this isn't free for maintainers or those affected, what has been the cost/benefit analysis?
This effort boils down to productivity for maitainers and users.  Unfortunately "hard numbers on net developer productivity" is hard to directly measure.  We can estimate proxies that we think impact productivity. Here are some factors that we think are important and our intuitive estimate of the impact of this on them:
1. Cost for new developers to ramp up on the codebase
    - Long term positive, sort term negative (during migration phase)
2. Confidence that a change works with other in-use code
    - Positive, since we can easily test the code together in the same repo, and there are less versions to test
3. Cost of interruptions to workflows of existing developers
    - Negative: workflows will definitely change (set up codeowners for areas of ownership, permissions, CI issues / flakiness, etc)
4. Cost for application developers to figure out how to use our suite of libs to solve their problems
    - Positive, this gives us one place to document how to use these libraries together and lower the cost of refactoring to make them easier to use
5. Cost of coordinating new changes
    - Negative. Developers will want to reach agreements earlier to land things in Boxo instead of working on different versions/forks indefinitely (as is the case with the current setup which causes the proliferation of incompatible versions).
6. Cost of propagating changes/fixes/refactors
    - Positive. This trades off directly with "cost of coordinating new changes". By design this makes it harder to keep multiple versions of libs floating around and instead encourages code to be versioned consistently, so that changes can be propagated quickly and without much dependency hell.

Additional measurement planning will be done as part of [#203](https://github.com/ipfs/boxo/issues/203), particularly to look at metrics of time to merge and release PRs.

### What are some concrete usecases where repo consolidation would have been beneficial?
Below are anecdotes from the last few months where EngRes IPFS Stewards wished for directly or saw need from others for consolidated repos.  Ideally these anecdotes would have been better catalogued.  We're at least going to add them through 2023H1 as we see or hear about them.
* [2019-2021 adding context tracing throgh blockstore/datastore](https://github.com/ipfs/kubo/issues/6803) - Boxo wouldn't have been fully helped here as there were changes outside of Boxo modules like ipfs/go-datstore and libp2p, This whole endeavor was a total pain despite having a very tolerant contributor doing much of the work.  The status quo was painful too, so we made the changes, bubbled them up and communicated with people about when the changes would be coming. A lot of the plumbing there was pretty miserable and the number of repos we had to communicate around was painful too.  The complexity involved wass quite high--you need to find an appropriate topological order of the dependency graph, make the changes & test & release repo versions incrementally, etc. And if you make a single mistake, you have to start over. With a smaller number of repos these kinds of changes would be easier to execute and communicate about.
* ADD DETAILS: Changing the fetcher API - @aschmahmann to fill in.  @BigLep jotted this down during 2023-02-28 conversation.
* ADD DETAAILS: Finding a bug in a sub-repo and not being able to handle 
* 2023-02-26 Bacallhau raising concerns about their dependency tree. 
 https://filecoinproject.slack.com/archives/C02RLM3JHUY/p1677409745784779
* 202302 security event handling where doing a fix spanned 3 repos instead of the ideal one.  This mean 3 security advisories, releases, etc.
* 202302 Gala Games - After experiencing Kubo footguns that affected DHT performance, they raised their hands to be consumers of a project like boxo so they could assemble a IPFS implementation tailored to their needs.   
* 202303: https://github.com/ipfs/boxo/pull/242 .  This would have needed to be a complex DAG of 4 PRs and go.mod bubbling for what is a simple API extension.  This is now 1 PR.  Those improvements would take an hour before, this was 5 minutes of a maintainer's time.
* 2023-03-31: Unsolicited feedback from engineers working on ipfs/gateway-conformance-testing: "❤️ the boxo consolidation. In conformance we went from importing ~8 packages to open a car file containing a unixfs node to 4 packages. The lib is easier to work with, it feels more consistent, and it’s less overwhelming to get started."

### Is this really for developers or just maintaienrs?
It is for both.  Maintainers have had consistent feedback from users and PL new-hires that the sheer volume of repos and effort required to plumb changes around is immediately off-putting. Some of the current maintainers were those new hires [who kicked off this effort in 2021](https://github.com/ipfs/kubo/issues/8543), there have been others with the same sentiment, there are users who contribute with the same sentiment, and we believe there's likely a large group of people who would have contributed or built on this ecosystem but don't because the cost is so high. Sure, this may not be for all developers but we see a clear signal from many folks that they don't like working with the existing setup.

It is true that we may not be taking into account the people that were OK with the repro sprawl of <2023 who are not reaching out to remind us that they were happy.  At least as of 202303, we have only heard concern from other engineers outside the PL EngRes team (see [here](https://github.com/ipfs/kubo/issues/8543)).  

### What kind of user outreach was done about this consolidation work?
The [original issue was created in Kubo back in 202111](https://github.com/ipfs/kubo/issues/8543).  It hasn't been hidden, but it admittedly wasn't broadcasted.  It was talked about in in various 2022 PL EngRes IPFS roadmap documents and EngRes All Hands.  It's understandable that users may have not been able to discern whether this was "just talk" or whether this was actually going to translate into action.  Given this issue had been open for a year plus, it was a miss to not better communicate what would be happening when and use all the communication channels at our disposal (chat, discussion forums, blog post, tracking issues with plans).  At this point, it's water under the bridge because the disruption has already happened.  More formal announcing will happen at IPFS Thing 2023 and followup [announcement blog post](https://github.com/ipfs/boxo/issues/259) (albet it with a "this has happened" tone rather than a "we're seeking input" tone.  We expect we'll still get signal though if the community who was happy with the <2023 repo sprawl hears about this for the first time.) 

## Copied or Migrated Repos FAQ
The FAQs below concern all the repos that were copied or migrated into boxo to bootstrap the library.  Some of the repos are copies of repos that are maintained by others, but most of the repos that were copied are now not maintained.

### What repos have been copied into Boxo?
The authoritative list is in https://github.com/ipfs/boxo/blob/main/cmd/migrate/internal/config.go.  Many of these were handled as part of https://github.com/ipfs/boxo/issues/202.

### What will happen if there is a security issue in one of the legacy ipfs/* repos that has been copied into boxo?
@ipfs/kubo-maintainers (which primarily maps to [PL EngRes IPFS Stewards](https://pl-strflt.notion.site/IPFS-f3c309cecfd844e788d8b9e13472a97b) as of 202203) will certainly handle patching boxo. If there are maintainers for the original/source repos, they will need to handle patching/disclosing. @ipfs/kubo-maintainers will certainly coordinate and share work, but they won't handle communication with or updating of the affected consumers of the original repos that boxo copied from. If there are no maintainers for the original/source repos, there will likely be internal/private PL EngRes chats analyzing which projects are impacted, and it will then be up to those projects to determine whether they want to patch the existing repos or update to use boxo.  (We understand and expect that the "update to boxo" option will often not be possible under tight timelines given boxo's plans to upgrade its dependencies frequently and refactor the existing code.)

### I don't like the "deprecated" warnings in "not maintained" repos that have been moved to boxo.  What can be done about this?
The intent of the "deprecated" warnings was to be clear to consumers about the status of the repo: that it isn't maintained and that there is a repo where similar code is being maintained.  If another maintainer comes along, then they can [claim ownership](#how-does-one-claim-ownership-of-a-not-maintained-repo) and the state of the repo can be adjusted.  This satisfies the requirement about being clear to users about who owns a repo and its status. 

### How does one claim ownership of a "not maintained" repo?
A new maintainer can 
1. create a PR that includes:
   * Revert of the deprecation types
   * Removal of the "not maintained" notice from the README
   * Addition of themselves to CODEOWNERS
2. Get @ipfs/kubo-maintainers attention by @mentioning them in the PR and/or posting in the venue(s) discussed in https://github.com/ipfs/boxo#help

Assuming the new maintainer is known/trusted, @ipfs/kubo-maintainers will merge.

### Will the "not maintained" ipfs/* repos be left around to rot?
PL EngRes @ipfs/kubo-maintainers don't want to leave a wasteland of unmaintained repos.  We'll come back through and archive repos that haven't had a maintainer step up in sometime 2023Q3.  See https://github.com/ipfs/boxo/issues/201.


