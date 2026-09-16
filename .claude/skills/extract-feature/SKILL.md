---
name: extract-feature
description: Extract how a feature is implemented in one of the riskstore repositories - or survey all of them, pick the canonical implementation and record where the others diverge - and write it up as a precise implementation spec that a different Claude session can build a new service from without access to the source repos. Use this whenever the goal is to reimplement, port, or repeat an existing integration in a new service - phrases like "how does X work in repo Y", "document how we connect to keepie/cranker/clickhouse/kafka", "extract the reference implementation for Z", "I'm standing up a new service that needs the same integration", or "write this up so another session can implement it". Also use it when asked which repo has the best version of a shared integration, or to refresh a reference doc that has gone stale.
---

# Extract Feature

Produce an implementation reference for one feature, written for a consumer who will never open the source repository.

## Why this is not a normal "explain the code" task

The default instinct when asked to document a feature is to explain the design: what the pieces are, how they relate, why it's shaped that way. That output is pleasant to read and useless to build from. The session that receives it still has to guess the config key names, the exact dependency versions, the startup ordering, and what happens on a 401 - and it will guess wrong, confidently, and the new service will half-work in a way that takes a day to debug.

The target is closer to a build spec than an explainer. The test to hold yourself to:

> A Claude session that has this document and **no access to the source repositories** must be able to implement the feature in a new service and have it compile and work against the real dependency. Anywhere it would have to guess, the document is incomplete.

Keep that sentence in mind while writing every section. It resolves most judgment calls about what to include.

## Layout

Run from the riskstore root, which holds every repository as a sibling directory:

```
riskstore/
├── docs/reference/             <- extracted docs land here, shared by every service
├── riskstore-ingestion-service/
├── riskstore-data-lineage/
├── riskstore-rods-query-service/
├── riskstore-kafka-manager/
├── riskstore-kafka-publisher/
├── riskstore-goldeneye-query-service/
├── riskstore-marketdata-service/
├── riskstore-reference-data-service/
└── <new-service>/              <- target being built
```

Those eight are the source repositories. New services are targets, never sources, until one has been running long enough to be worth extracting from.

Before reading anything, check the repos you need are actually cloned and contain a build file. If one is missing, stop and say which - a survey silently missing three repos produces a confident, wrong answer about which implementation is canonical.

## Step 1: Parse the request

- **Feature** - required. If absent, ask.
- **`--from <repo>`** - the source repository. Required unless surveying.
- **`--survey`** - compare all eight, pick the canonical implementation, record divergences. See Step 2b.
- **`--into <service>`** - the new service being built. Optional. It does not change where the document is written; it only decides which service gets the pointer in Step 5.

Examples:

```
/extract-feature keepie credentials --from riskstore-ingestion-service --into riskstore-perf-bench
/extract-feature clickhouse connect, insert and query --from riskstore-rods-query-service --into riskstore-perf-bench
/extract-feature kafka publishing --survey --into riskstore-perf-bench
```

## Step 2: Read the named repository

Normal mode. The user picked the repo; use it. Don't second-guess the choice or wander into other repos for a better version - if they wanted the comparison they'd have asked for the survey.

## Step 2b: Survey mode

Only when `--survey` is given.

Grep all eight repos for the feature's fingerprints - dependency coordinates, distinctive class or config names - and build a table of which ones implement it. Then pick the **canonical** implementation. Signals, roughly in order of weight:

- **Most recent real work on those files** (`git -C <repo> log -1 --format=%ai -- <paths>`), because integrations rot and stale ones carry dead workarounds.
- **Newest version of the underlying client library.**
- **Best test coverage**, especially integration tests - tests are evidence someone verified the behaviour rather than assumed it.
- **Fewest local workarounds and TODOs.**

Show the table and your pick before continuing, and keep it brief. If two repos disagree in a way that looks deliberate rather than accidental, say so and ask - that is usually a real architectural fork, and guessing wrong propagates into every service built from the document.

Record where the others differ; that becomes section 9. Divergence is information - it shows which parts are essential and which are one team's taste.

## Step 3: Trace the implementation

Follow the actual call chain from process start to the feature's first use, including the paths people forget:

- **Startup and shutdown** - what constructs this, when, on which thread, what blocks waiting for it.
- **The refresh / reconnect / retry path** - usually where the real complexity lives and always where a reimplementation breaks.
- **Error handling** - not "it handles errors", but which exceptions, caught where, what happens next.

Then gather what can't be inferred from reading code:

- **Exact dependency versions**, resolved from the build system rather than the manifest - `mvn dependency:tree`, `gradle dependencies`. Transitive versions matter when the consumer has a different resolution graph.
- **The tests** - they encode intended behaviour and the failure modes someone already hit. Note which need a live dependency.
- **Configuration in all its forms** - env vars, system properties, config files, defaults buried in constructors.

## Step 4: Write the document

Write to `riskstore/docs/reference/<feature-name>.md` - the shared library at the root, not inside any service.

Several services need these same integrations. One copy per service means paying for the extraction repeatedly and maintaining copies that drift apart, so that when keepie changes you update five files or silently don't. One document at the root, pointed at by every service, is the version that stays true.

If a document for this feature already exists there, check its provenance header against the source repo's current HEAD. If the source hasn't moved, say so and stop rather than rewriting a good document. If it has, re-extract and replace it, noting what changed.

Start with a provenance header, so a future reader can tell whether the document still matches reality:

```
Source: <repo> @ <commit-sha> (<commit date>)
Extracted: <today>
Also implemented in: <other repos>   # survey mode only
```

Then these sections:

1. **What it does** - three sentences.
2. **Dependencies** - exact coordinates and versions, and what each is actually for.
3. **Configuration** - every key: type, default, required or not, and what breaks if it's wrong. The failure column is the valuable one.
4. **The implementation** - the code, not a description of the code. Quote the classes and methods that matter verbatim, each with a `<repo>/<path>:<line>` citation, and give full signatures for anything a caller touches. Paraphrase is where precision dies; when in doubt, quote.
5. **Lifecycle and ordering** - what must happen before what, at startup and on refresh. Be explicit about threading and about what blocks.
6. **Failure modes** - behaviour on timeout, auth rejection, malformed response, dependency unavailable at startup. Retry and backoff with real numbers.
7. **Wiring it into a new service** - the minimal ordered steps.
8. **What NOT to copy** - anything specific to the source repo: internal helpers, legacy shims, workarounds for problems a new service won't have. Say what to do instead. Without this section every new service inherits every historical accident in the old one.
9. **Variants across repos** - survey mode only. Where the other implementations differ, and whether the difference is a deliberate choice the new service must make or just drift.

Close with a **verification status** note: which parts you confirmed by running tests or the service, and which you established by reading only. The consumer deserves to know which claims carry more weight.

Include the repo name in every citation. `riskstore-ingestion-service/src/main/java/.../KeepieClient.java:42` is checkable from the root; a bare path is ambiguous across eight repos.

## Rules that keep the document trustworthy

- **Cite `<repo>/<path>:<line>` for every factual claim about the code.** This lets a reviewer spot-check you, and keeps you honest about what you inferred rather than read.
- **Write `UNVERIFIED: <the specific question>` wherever you're unsure**, and never smooth uncertainty into confident prose. This is the highest-value habit in the workflow: it converts your uncertainty into a visible list the user can clear in minutes, instead of a silent error the consumer hits in production.
- **Ban the hedge words** - "typically", "usually", "should", "generally". Each marks a place you didn't check. Either check, or write `UNVERIFIED:`.
- **Document what is there, not the cleaner version you'd design.** Awkward code is often load-bearing awkwardness; note it in section 8 and let the consumer decide.

## Step 5: Wire it up for the consuming session

A document the implementing session never opens is the same as no document, and a shared library makes this the failure mode to guard against - the document is one directory further away than the session's own repo, so it won't be stumbled upon.

Add or update a pointer in the target service's `CLAUDE.md`:

```
Reference implementations extracted from the riskstore source repos live in
../docs/reference/ (shared across services). Read the relevant one before
implementing that integration, and do not copy it into this repo.
```

Then add a one-line entry to `riskstore/docs/reference/README.md`: feature, source repo, extraction date, and which services consume it. That index is how you later find out which documents have gone stale and who is affected when they do.

When a later service needs a feature that is already documented, point at the existing document instead of re-running this skill. Re-extract only when the provenance header no longer matches the source repo's HEAD.

## Step 6: Offer the fresh-session check

Reviewing your own document doesn't work - you know the answers, so your brain fills the gaps silently and it reads as complete. A subagent doesn't help either; it shares your context.

The real test is a **separate session with only the document**, asked to implement the feature, with the result diffed against the source. Every place it guessed, invented a config key, or had to ask is a hole. Fix the document, repeat; two rounds usually gets it solid.

Offer this when the draft is done. It's cheap, and it's the only check that measures whether the document does its job.
