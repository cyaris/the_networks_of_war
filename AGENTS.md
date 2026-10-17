# Repository Guidance

## Data Generation

- Regenerate and commit `frontend/src/lib/static/graphData.json` after changing the backend pipeline, its SQL, or
  source data under `backend/data/`.

## Documentation And Data Semantics

- Keep README pipeline commands aligned with `backend/src/pipeline.py` CLI behavior. Keep frontend build-artifact notes
  near frontend setup or commands rather than in a standalone top-level section.
- When adding, removing, renaming, or changing the calculation or formula of a backend metric, update that metric's
  definition in `frontend/src/lib/static/metricDataDictionary.json` in the same change. Align README source,
  calculation, unit, and metric summaries with that file.
- Split long README assumption sections into focused subsections. Show normalization and replacement direction with
  `source -> replacement` example subbullets.
- Consult relevant source documentation under `backend/data/` before deciding whether a source value, adjustment, or
  transformation is correct. Use the backend `pdftotext` development dependency to extract searchable PDF text, and
  treat PDF metadata dates as build metadata unless the text identifies a source release date.
- Do not preserve removed or renamed interfaces unless the user requests a migration bridge. Delete stale parameters,
  aliases, scripts, and docs so old callers fail clearly.

# Inherited Rules

Copied from `../shared-automation/AGENTS.md` by `sync-agents.js`. Edit them there, then run
`node ../shared-automation/.github/scripts/sync-agents.js` from this repository's root.

<!-- inherit: shared-automation/code-style -->
## Code Style

These rules apply to tracked, nonignored source files in every language. Skip generated, vendored, build-output, and
ignored files, such as `.svelte-kit/` or `dist/`, unless a task explicitly requires refreshing them. The Python And SQL
topic and `svelte-lib`'s JavaScript topic add to them.

- Follow the installed formatter, import sorter, and linter rather than hand-formatting against their output. After
  changing code in a repository that defines formatting or lint validation, run every applicable validation and
  address its findings.
- Write comments only when they materially clarify necessary context. Fix typos in comments that you write or edit
  nearby.
- Inline local variables referenced only once and functions with only one caller unless a separate name is materially
  useful for clarity, behavior, or correctness. Separation of concerns alone does not justify a single-use helper when
  its caller remains clear and manageable after inlining. Retain a single-use variable or function only for a concrete
  reason, such as substantial complexity, meaningful semantic abstraction, independent testing value, genuinely
  expected reuse, avoiding duplicated evaluation or side effects, or measured performance. Do not mechanically inline
  code when that would make a function excessively large or materially harder to understand. Staying within the
  formatter's line length is not such a reason: let the formatter wrap inlined code.
- Prefer an existing module- or component-scope variable over passing the same invariant value through internal helper
  parameters. Keep parameters for values that genuinely vary by call, define a reusable public utility's input
  contract, or make a dependency explicit for isolation or testing. Do not introduce mutable application-wide state
  merely to shorten a function signature.
- Group consecutive statements by purpose with one blank line between groups. Keep statements together when they are
  the same kind or directly sequentially related. A standalone comment counts as the separator, so do not also add a
  blank line immediately before it. Do not make cleanup changes that only alter blank lines or line breaks; apply this
  grouping when the surrounding code is being changed substantively or the formatter requires it.
- When order has no semantic, dependency, source-order, or configuration-defined meaning, arrange lists, object or
  dictionary keys, named definitions, and other code collections alphabetically. Compare code case-sensitively with
  uppercase before lowercase, and let the formatter, import sorter, linter, or another installed ordering tool take
  precedence.
- Keep imports at the top of a module. Defer an import into a function only to break a circular dependency or when the
  module must run without a genuinely optional dependency. Let the installed formatter, import sorter, or linter
  determine import ordering. When none owns it, sort imports alphabetically and group package imports above project
  imports with one blank line between them, preserving CSS import order because it can affect the cascade.
- Declare every installed dependency in the owning package's or project's dependency file rather than relying on the
  local environment or a transitive dependency.
- Format user-facing or human-readable logged numeric quantities with thousands separators, through `.toLocaleString()`
  in JavaScript or a `,` format specifier such as `f"{count:,d}"` in Python. Do not apply numeric formatting to
  identifiers, to other values whose digits are not quantities, or to structured log fields a parser or metrics
  pipeline consumes, because the separators make them locale-dependent strings.
- Treat a valid `0` as data and keep `null` distinct from zero. Use finite-value checks rather than truthiness for
  numeric values, and coalesce a missing value to `0` only when coverage or derivation proves it is zero, so a display
  can show it as unknown.
<!-- /inherit: shared-automation/code-style -->

<!-- inherit: shared-automation/documentation -->
## Documentation

- Fix typos in documentation that you write and in existing nearby text that you edit.
- When the order of prose list items is not significant, arrange them alphabetically using case-insensitive comparison, with the uppercase item first when two items differ only by case. Preserve semantic, procedural, severity, and other meaningful orders.
- Keep GitHub Actions workflow sections as the final top-level section in caller README files.
- Keep README and AGENTS guidance focused on current behavior, active requirements, and durable project decisions. Remove
  migration-era notes, deprecated-option explanations, old fallback paths, and historical caveats once they no longer
  affect how someone uses, maintains, deploys, or releases the project. When a state change makes a requirement obsolete,
  update the affected docs and configuration in that same change. This removes content that has stopped being true, never
  an accurate statement of how the project works today: a fact stays documented while it holds, including when a tracked
  issue proposes replacing the behavior, and including when the same fact is stated in another section. Consolidate a
  genuine repetition by choosing which section owns it and keeping that one, rather than deleting both.
- Do not add repository-owned `CHANGELOG.md` files. Keep durable current behavior in README/AGENTS documentation and
  publish milestone history through GitHub release notes.
- Keep README link behavior intentional and consistent. Use standard Markdown links by default, and use HTML anchors
  with `target="_blank"` and `rel="noopener noreferrer"` only when links should explicitly open in a new tab.
- In README files and user-facing technical Markdown documentation, use bullets, subbullets, or a compact table whenever a
  sentence or paragraph enumerates three or more peer technical items, including files, paths, commands, flags, fields,
  tables, outputs, steps, requirements, metrics, dependencies, or triggers. Do not leave a concrete inventory as
  comma-separated prose merely because each item is short. Keep one item or a genuinely inseparable two-item phrase in
  prose. When aligning a README with these rules, audit existing touched sections for dense inline inventories instead
  of applying the rule only to new text.
- Do not use bullets solely to separate README command examples or other code-block sections. Introduce each code block
  with a short prose sentence instead.
- Do not place separate bullet groups directly next to each other when they document different concepts, because
  Markdown can render them as one list. Use prose, a table, or an explicit subsection label to separate the concepts.
- Keep each README bullet list focused on one kind of item. If a bullet stands out as metadata, a context note, an
  example, an identifier, or a behavior note rather than a peer of the surrounding bullets, move it into
  prose, a table, a new subsection, or a clearly labeled subbullet group.
- When README bullet items are sentence fragments, omit trailing periods. Keep periods for bullets that are complete
  sentences or contain multiple sentences.
- Avoid starting README bullets with ambiguous pronouns such as `it`, `this`, or `these` unless the noun is explicit in
  the same bullet. Repeat the noun when that makes the bullet clearer.
- Avoid vague README verbs such as `use`, `provide`, `support`, or `available` when the relationship can be named more
  directly. Prefer concrete wording that identifies the field, flag, table, file path, setting, destination, or UI
  behavior.
- Prefer active wording when it identifies the actor or operation more clearly, especially for ownership, workflow
  actions, requirements, generated outputs, and failure behavior. Keep passive or state-oriented wording when the actor
  is unknown, irrelevant, or less important than the resulting state; do not perform mechanical voice rewrites.
- When documenting multiple README tables, files, or generated outputs, describe each item separately when a shared
  description would become vague or hide meaningful differences.
- Do not restate a section's subject in the text directly beneath its heading. A heading that already names a file, such
  as `.github/workflows/rollup.yml` or `contact.html`, makes that restatement redundant, so the text below it opens with
  the verb: "Calls the shared rollup workflow with these local details:", not "The `Rollup` workflow calls the shared
  rollup workflow with these local details:". This applies to prose and bullets alike.
- Use prose instead of a bullet list when a section would contain only one bullet, unless the section heading already
  names the subject; prose would then have to restate that subject, so keep the single bullet and let it open with the
  verb. Prefer prose over subbullets when a nested list would have only two items, unless the pair needs extra visual
  separation to avoid ambiguity.
- When an example supports an existing README bullet, make the example a subbullet under that point even when there is
  only one example. Use `Example:` for one example and `Examples:` for multiple examples.
- Let table-of-contents nesting reflect the document structure even when a section has only two children.
- Keep documentation style guidance and future maintainer instructions in AGENTS.md instead of the README. The README should describe project behavior,
  commands, outputs, and user-facing effects rather than telling future editors what they should do. This covers durable
  standing conventions only, such as a step that must be repeated whenever a given file changes. Do not move an open work
  item into AGENTS.md: a `TODO`, a known gap, a planned migration, or an undecided design choice is tracked where the
  project already tracks work, such as a GitHub issue or the README note that already records it, because AGENTS.md
  states how the repository is maintained rather than what is still outstanding.
- In Markdown files, always format the literal as `null`.
- Before completing a change that adds or changes a user-followable link, verify the real target rather than relying
  on the URL's appearance. Check an internal destination in every route implementation that serves it. For an
  external destination, make a live request that follows redirects and confirm the final page is the intended one, not
  merely a non-404 response. Add focused regression coverage for the stable route or URL. If automation is blocked by
  login, bot protection, or another access restriction, perform and report the required manual check instead of
  claiming the link was verified.
- Keep private action and dependency access requirements documented in `README.md`.
- Keep every Markdown pipe table compact in source. This is required formatting, not a preference: separate columns
  with exactly one space on either side of each pipe, as in `Prop | Behavior` and `--- | ---:`, with no leading or
  trailing pipe. Never pad cells or lengthen separator dashes to align columns. Preserve only required alignment
  markers such as `---:`, `:---`, and `:---:`. When editing a document for any reason, preserve compact tables and
  restore compact formatting in every table touched by the change. Prettier pads every pipe table it formats, so never
  run Prettier, or any other formatter that rewrites tables, on a Markdown file. Keep Markdown out of a repository's
  `format` scripts and list it in `.prettierignore` so a direct `prettier --write` on a document is a no-op.
- Downstream README files should link to this repository's workflow descriptions instead of repeating shared behavior.
  For each local wrapper, document only the applicable local trigger and branch behavior, working directory, skipped
  commands, destination or S3 prefix, bundle files and naming, dependency refs, policy overrides, and required local
  variables or secrets. Then link to the shared workflow description for common behavior, inputs, and secrets.
- When a change affects product behavior, routes, architecture, APIs or data sources, data models, live or background
  behavior, or a meaningful edge case, update the Markdown document that owns that subject in the same change. Prefer an
  existing owner over a new document, and make no documentation-only change for a trivial implementation detail.
- Label documented functionality as implemented, partially implemented, or planned when its status could be
  ambiguous, and update the label when implementation changes, so planned behavior never reads as current.
- When a change makes a meaningful architectural decision, record the problem, the approved approach and its
  rationale, the alternatives considered, tradeoffs, and important edge cases in the document that owns that area. For
  a later replacement, document what changed and why rather than rewriting the earlier record.
<!-- /inherit: shared-automation/documentation -->

<!-- inherit: shared-automation/git-and-pull-requests -->
## Git And Pull Requests

### Agent And Skill Execution

- Execute a skill's instructions in the active primary agent session, using that session's model and compute. Treat a skill as portable instruction text: its location under Claude, Codex, or another agent does not authorize launching that vendor's CLI, subprocess, remote session, or separate agent runtime. Naming a skill from another agent ecosystem requests the skill's workflow, not that ecosystem's compute. Use separate agent compute only when the user explicitly asks to run that separate agent or model rather than merely naming its skill.

### Commit Integrity

- Assume another session may be editing the same working tree at the same time. A commit must hold exactly the
  changes its session made, and never another session's.
- Stage by path, never with `git add -A`, `git add .`, or `git commit -a`. Before staging a file, read its
  `git diff HEAD -- <path>`, which shows staged hunks as well as unstaged ones: if any hunk is not yours, stage only
  your hunks, or leave the file and tell the user. Before committing, read the full `git diff --cached` on its own,
  hunk by hunk: another session may have staged files you never touched, or hunks in a file you also changed, and a
  plain `git commit` takes all of them. `git diff --cached --stat` summarizes it but lists only files, so it cannot
  show a foreign hunk in your own file. If the staged diff holds anything that is not yours, commit through the
  temporary index described below instead. Never chain staging, this check, and the commit in one command.
- Never stash, reset, restore, or check out a file you did not change, because doing so moves another session's
  uncommitted work.
- Make, commit, and push a change from the working tree the user runs, never from a separate worktree or clone of
  it. A commit pushed from elsewhere leaves the user's checkout behind its own branch, so a running dev server never
  shows the change, and where another session has uncommitted edits to the same files, the next pull cannot apply
  without stashing or merging that work. A scratch worktree may build or test a candidate tree but never commits to
  a branch the user's checkout tracks. When the shared index holds anything another session staged, commit through
  a temporary index built from `HEAD` plus only your hunks, then add the same hunks to the shared index, so neither
  the working tree nor the other session's staging moves.
- Right after committing, compare the full `git show HEAD` with the hunks you meant to commit, and confirm that
  `HEAD`'s parent is the commit you started from, as a separate step before any push, never chained to the commit. If
  your change is missing, landed in another session's commit, or shares your commit with work you did not make, stop
  and tell the user before pushing. Never rewrite a pushed commit to fix this.
- Write clear, specific commit subjects that describe the actual change. Prefer plain language over release-tool syntax,
  and do not exaggerate routine maintenance as user-facing work.
- Never commit, print, or log a secret, or put one in a fixture, URL, client response, analytics event, or pull
  request. Treat public keys, such as a CAPTCHA site key, as configuration, and keep secrets server-side and out of
  browser-readable roles. A provider's official test credentials may appear in local-development fixtures, never in
  production configuration.

### Pull Requests

- Never merge a pull request unless the user explicitly instructs you to merge that specific pull request or an
  unambiguous identified set of pull requests. Requests to fix, finish, deploy, publish, investigate, make checks green,
  or continue do not authorize a merge; neither do green checks, review completion, mergeability, or prior authorization
  for a different pull request. When a pull request is ready without explicit merge authorization, leave it open and
  report its readiness. The instruction must say **merge** and name the pull request by number or URL. A reply such as
  "go for it," "proceed," or "ship it" does not authorize a merge, even right after the assistant proposes merging a
  specific pull request, and permission to commit or push is not permission to merge. Ask for that instruction when a
  deployment needs a merge.
- Before merging any pull request, explicitly inspect CodeRabbit comments and reviews and assess every still-applicable
  finding; do not merge solely because checks are green.
- When a pull request depends on configuration or approval that cannot be committed to the repository, put a prominent
  caution in the pull-request description before its merge or deployment instructions. This includes repository,
  organization, or environment variables and secrets; API credentials and tokens; cloud projects, billing, identities,
  permissions, services, and resources; OAuth applications, origins, and callback URLs; DNS and provider-dashboard
  settings; third-party approval; paid-plan requirements; production migrations; and other manual external state.
- Make the caution an actionable inventory. Name every required item exactly, identify where it must be configured,
  distinguish public configuration from secrets, state prerequisites and required ordering, explain how to verify the
  resulting contract without revealing secret values, and state what remains unavailable or fails while an item is
  missing. Never put a credential, token, private key, or other secret value in the pull request.
- Separate completed external setup from pending work and include verification evidence for completed items. Keep an
  activation-dependent pull request explicitly blocked while required external setup or verification remains pending,
  and update the caution as each item is completed so later maintainers do not repeat a destructive step or assume that
  committed workflow or migration files changed the external system.

### Release Management

- Use the default policy in `.github/release-policy.yml` unless a caller has a durable project-specific override in its
  own `.github/release-policy.yml`, which never reintroduces shared release implementation logic. Keep version and
  title formats in policy files rather than duplicating them in `AGENTS.md`.
- Evaluate whether accumulated changes form a meaningful release milestone. Substantial user-facing features,
  architecture changes, important reliability, security, accessibility, performance, or compatibility improvements,
  and coherent groups of material changes may warrant a release. Routine maintenance, formatting, minor refactors,
  isolated dependency updates, and small fixes normally do not.
- When a release may be warranted, explain the milestone, suggest a policy-consistent title and tag, summarize release
  notes and migration concerns, and recommend full release, prerelease, or draft status.
- Treat work on PR or development branches as a release candidate. The final tag should normally point to the merge
  commit on `main` or `master` unless the user explicitly approves another branch.
- Do not create, rename, move, or delete tags or publish a GitHub release unless the user explicitly requests it.
- Treat upstream automation, shared workflow reference, dependency-pin, Renovate, and release-policy maintenance as
  non-release work unless it changes repository user behavior or a published package/runtime API.
<!-- /inherit: shared-automation/git-and-pull-requests -->

<!-- inherit: shared-automation/workflows -->
## Workflows

### Workflow Ownership And Caller Expectations

- Keep shared AWS CDK deployment, CI, rollup, auto-create-dev-PR, and auto-release implementation changes in this repository. Caller repositories should keep thin local wrapper workflows that define triggers, permissions, inputs, secrets, and repository-specific values before calling the reusable workflow here.
- Keep reusable workflow defaults general. Caller-specific commands, local dependency refs, bundle file lists, metadata refresh files, branch selections, S3 prefixes, and release naming or milestone overrides belong in caller workflow inputs or caller release-policy files.
- First-party upstream refs should track the latest production branch by default. Reusable workflow and composite-action
  callers must use `cyaris/shared-automation` refs on `main`, including when a new downstream caller temporarily fails
  because its required shared feature exists only on `dev`. Rollup local dependency specs should name production refs
  such as `main`
  unless a repository documents an intentional override; the shared Rollup workflow substitutes `dev` for those
  production refs when the caller runs on `dev`, then resolves every selected ref to an exact commit SHA before checkout
  and upload. Dependencies needed by both CI and upload should be listed in `local-dependency-repositories`. Rollup
  checks out `svelte-lib` from the separate `svelte-lib-repository` and `svelte-lib-ref` inputs instead of that list,
  applying the same `dev` substitution and SHA pinning.
- Do not hardcode commit SHAs or substitute a feature branch in first-party `cyaris/*` reusable workflow,
  composite-action, or source dependency references to expose an upstream feature before it reaches the referenced
  production branch. Keep caller configuration on the appropriate `main` or `master` production-branch contract; exact
  SHAs resolved internally by a workflow for a single reproducible run are not hardcoded caller references. Third-party
  GitHub Actions remain SHA-pinned under the security rule below.
- When a downstream change needs an upstream feature that is not yet available on its configured production branch,
  tell the user before publication that the downstream workflow, build, or runtime will fail until upstream lands. Keep
  the downstream reference on the production branch and publish it when requested; do not conceal the dependency by
  pinning the unpublished commit or changing the caller to an upstream feature branch.
- Keep release boundary decisions in the shared auto-release workflow and release-policy files. Caller repositories should
  not add separate release automation unless the user explicitly asks for a repository-specific exception.
- Do not move a repository-specific workflow implementation into this repository unless another repository will share the same behavior.
- Manual `workflow_dispatch` paths must remain restricted to the `cyaris` GitHub actor by default. Trusted Rollup
  dispatches from the repository's upstream-watch workflow may also allow `github-actions[bot]`, gated on Rollup
  verifying via the GitHub API that the supplied `source-run-id` references an authorized `upstream-watch.yml` run in
  the same repository that was active when GitHub created the Rollup run. This is time-bounded authorization of a
  legitimate run ID, not verified provenance: `source-run-id` is caller-supplied input, so it does not cryptographically
  prove which workflow actually issued the dispatch. Do not extend that automation exception to other actors or
  workflows, and do not grant `actions: write` to additional workflows in this repository, without first closing that
  gap (for example, with a `workflow_run` trigger).

### Workflow Behavior

- Do not disable a shared CI standard-script flag (`run-format`, `run-lint`, `run-check`, `run-build`) unless the caller
  defines no such npm script, and document that reason where the wrapper is described. A caller that defines the script
  runs it; skipping one silently removes coverage every other caller has.
- Auto-create-dev-pr callers should pass `CHECKOUT_TOKEN` so the shared workflow can write the `DEV_PR_OPEN`
  repository variable, and should pass a repository `RELEASE_TOKEN` secret, when present, for trusted user or
  agent-authored dev pull requests. Callers must also trigger on production-branch pushes and keep the shared
  `DEV_PR_OPEN` job-level skip so `dev` pushes do not start a runner while a dev pull request is open.
- Workflows must fail clearly when a requested feature requires credentials, secrets, repository variables, external
  permissions, or paid services that are not configured. Apply this to dry-run modes too: a dry run may avoid external
  writes, but it should still prove that required credentials exist unless the feature is explicitly documented as
  credential-optional.
- Do not hide required workflow failures with `continue-on-error: true`, `|| true`, warning-only error handlers,
  harmless default outputs, or skipped jobs. Intentional no-op paths are fine when the work is genuinely unnecessary,
  such as no release action being needed or no dev pull request being possible, but missing configuration and failed
  required operations should end in a failed step or job with enough context to debug the cause.
- Keep scheduled workflows simple. Prefer one schedule away from the top of the hour over multiple UTC schedules plus a
  local-time gate, unless the repository truly needs exact local-time behavior. If a schedule is only approximate, let
  the job run and document the chosen UTC time instead of creating helper jobs that make expected runs appear skipped.
- Prefer AWS OIDC for deployment workflows. Reusable workflows and caller jobs that pass `aws-role-to-assume` must grant
  `id-token: write`; static AWS access-key secrets may remain only as a compatibility fallback until callers have
  repository-specific OIDC roles configured.
- For rollup upload callers, `dev` pushes should not run Rollup. Pushes from `main` or `master` should run production
  uploads with unprefixed bundle names. Manual or trusted upstream-watch dispatches from `dev` should run staged uploads
  with `dev_bundle.*` names.
- When a Rollup caller needs a temporary deployment-readiness gate, pass the shared `deployment-enabled` input. Keep the
  caller job active so shared CI still runs, and apply the gate only to the shared upload job.
- Do not trigger GitHub Actions workflows from pull-request events. `auto-create-dev-pr` is the only workflow that runs
  automatically on `dev` pushes; run pre-merge CI, build, Pages, and workflow-validation checks on `dev` through manual
  dispatch, and retain production-branch push checks after merge.

### Dependency Automation

- Keep shared Renovate defaults in `default.json`. Participating repositories should keep local `renovate.json` files as
  thin wrappers that extend `github>cyaris/shared-automation`, then add repository-specific overrides only when the
  shared preset cannot describe a durable local need.

### Workflow Validation And Security

- Add workflow-validation callers to dependent repositories when they own meaningful local workflow logic, such as
  deployment jobs, rollup input-resolution shell, local permissions decisions, secret wiring, dispatch inputs,
  concurrency behavior, or nontrivial Pages workflows. A thin reusable-workflow caller still owns meaningful local
  logic when it declares any of those behaviors. Avoid adding validation wrappers solely for repositories whose local
  workflows are completely declarative calls without security-sensitive or behavioral configuration.
- Treat `actionlint` failures as workflow contract or syntax issues to fix before rollout.
- Treat `zizmor` findings as security-review prompts. Fix true positives, document accepted risks, and avoid broad
  suppressions.
- Keep third-party GitHub Actions hash-pinned when `zizmor` requires it.
- Extract non-trivial inline Node.js logic out of `run:` heredocs into standalone files under `.github/scripts/`,
  covered by `node --test`, rather than leaving security-relevant or otherwise complex logic untested inside a workflow
  step. Wire the reusable CI workflow's `run-test` input to run those tests before rollout.
<!-- /inherit: shared-automation/workflows -->
