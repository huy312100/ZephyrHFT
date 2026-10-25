# Review stack

ZephyrHFT gets two independent AI reviews on every pull request. Both are free
for public repositories, so there is no key to buy, rotate, or leak, and no
vendor bill that can quietly start accruing.

| Reviewer | Responsibility | Trigger | Cost |
| --- | --- | --- | --- |
| **CodeRabbit** | Primary review: walkthrough, per-line findings, follow-up chat | automatic on push; `@coderabbitai review` | free for public repos |
| **Sourcery** | Second opinion on a different model, plus on-demand failover and its security checks | automatic on push; `@sourcery-ai review` | free for public repos |

CodeRabbit is configured in [`.coderabbit.yaml`](../.coderabbit.yaml), which is
version-controlled, so its instructions change by pull request like any other
file. CodeRabbit reads it from the pull request's own branch — when the file was
introduced, `@coderabbitai configuration` reported it as the entry file at that
commit — so tuning applies to the pull request that makes it rather than only
after a merge to `main`.

**Sourcery's configuration lives only in its dashboard** and cannot be
committed; the text to paste into it is kept below so it is at least reviewable
and reproducible. If Sourcery's dashboard settings are ever lost, this file is
the record of what they were.

Both reviewers are **advisory**. CI decides whether a change is safe to merge:
CircleCI runs `lint`, `build-and-test-py39`, `build-and-test-py312`, `sanitizers`
and `secret-scan`; GitHub runs CodeQL and GitGuardian. A reviewer that is
rate-limited, outage-struck, or simply wrong must never be able to block a
hotfix.

## Commands

CodeRabbit, in a pull request comment:

- `@coderabbitai review` — request a review
- `@coderabbitai configuration` — print the effective config and where each value came from
- `@coderabbitai summary` — regenerate the walkthrough
- `@coderabbitai resolve` / `@coderabbitai pause` / `@coderabbitai resume`
- `@coderabbitai help` — everything else

Sourcery, in a pull request comment:

- `@sourcery-ai review` — request a review; this also resets the once-per-push limit
- `@sourcery-ai summary` — PR summary and reviewer's guide
- `@sourcery-ai guide`, `@sourcery-ai title`
- `@sourcery-ai resolve` — mark its comments addressed
- `@sourcery-ai dismiss` — remove its comments from the pull request

## Sourcery dashboard setup

Applied once, by whoever owns the GitHub account. Sourcery has no committed
config file, so these live here:

1. Install the Sourcery GitHub App on `ZephyrHFT` only (not all repositories).
2. **Review settings**: enable AI review comments and the PR summary; set the
   review profile to **Quiet**. Turn off the reviewer's guide and sequence
   diagrams — on a diff this size they are noise.
3. **Path filters**: `build/**, reports/**, data/**, **/__pycache__/**`
4. **Base branches**: `^main$`
5. **Draft pull requests**: do not review.
6. **Auto-approve low-risk pull requests: leave OFF.** A bot approval that can
   satisfy a required review is not a control, and this repository can move
   real money.
7. Keep Sourcery's status check **not required** in branch protection.

Then add one review rule per block below (Rules tab: path patterns plus rule
text). All plans support custom rules; each rule text must stay under 3,000
characters.


### Rule 1 — ambiguous venue responses are not rejections

Path patterns:

```
python_analytics/live/**
scripts/**
```

Rule:

```
In this trading system, `-1006`, `-1007` and HTTP 5xx from the venue mean the
execution status is unknown, not that the order failed: the exchange may be
holding a live order. Any code that treats these as a clean rejection, drops
the order intent, retries the submit, or reports success is a double-fill risk
— call that out as blocking and say why.

The intent must be persisted with its client order id before the request is
sent, and any recovery must resolve through the venue using that same id.

Post-only rejections are only benign when the code proves them: `-5022` is
futures-only and self-proving, while `-2010` is the generic new-order rejection
and must be confirmed by its error text ("would immediately trigger").
Swallowing any other rejection code as expected is blocking.
```

### Rule 2 — an unknown state stays unknown

Path patterns:

```
python_analytics/live/**
```

Rule:

```
`UNKNOWN` is an active state and `CANCELLED` is terminal. Marking an order
CANCELLED removes it from `active_orders()`, after which `reconcile()` can no
longer chase it, so a terminal status may only be written for a state the venue
confirmed. Flag any transition to a terminal status without venue confirmation,
and any attempt to move a terminal order back to an active status.

`mark_status()` takes `BEGIN IMMEDIATE` and writes its audit event inside the
same transaction, deliberately: two commits would leave a row that changed with
no event explaining it. Flag any new status or event write that opens its own
transaction or commits separately.
```

### Rule 3 — tests must fail when the behaviour is removed

Path patterns:

```
tests/**
```

Rule:

```
Review tests for whether they would actually fail if the behaviour they name
were deleted. A test that passes with the ambiguity resolution, the
terminal-state guard, or the atomic status-plus-event write removed proves
nothing. Prefer assertions on the order-store state that an operator would
inspect (status, note, events) over assertions on internals or call counts.
```

### Rule 4 — operator paths and CI hygiene

Path patterns:

```
scripts/**
.github/workflows/**
```

Rule:

```
Exit codes are a contract: 0 clean, 1 drift or unresolved order, 2 configuration
refusal, 3 alpha gate refused, 130 interrupted. Flag any path that reports
success while an order is unresolved or the kill path did not complete, and
remember that on an interrupt the cancel and flatten are attempted, not
guaranteed.

Flag logging of API keys, listen keys or signatures. In workflows, flag
third-party actions that are not pinned to a commit SHA, `pull_request_target`
jobs that check out pull request code, and secrets echoed to logs.
```

Sourcery's security scanners run their own fixed rule sets and ignore the
rules above, which is fine: those checks are the reason it is here.

## Not used, and why

Recorded so nobody re-adds a dependency that is not free:

- **GitHub Copilot code review** — the review feature is not in the free plan.
- **Qodo Merge (hosted)** — there is no permanent free tier; open-source access
  is an application, not a signup.
- **Claude Code Action (`@claude`)** — needs a paid Anthropic API key.
- **Gemini Code Assist for GitHub** — free for individuals and worth adding if a
  third voice is ever wanted, but it is pre-GA and does not review
  `.github/workflows`, which is exactly where this repository has had
  security-relevant changes.

## Optional third reviewer

Self-hosted **PR-Agent** on the **Groq free tier** is the way to add a third
reviewer without paying: the software is Apache-2.0, Actions minutes are free on
public repositories, and `groq/llama-3.3-70b-versatile` is in PR-Agent's own
model table. It costs one repository secret (`GROQ_API_KEY`) and one workflow to
keep pinned, which is why it is not installed by default.

Not present in this repository today. If it is added:

- Trigger on `opened` and `ready_for_review` only, never `synchronize` — the two
  reviewers above already cover pushes, and a free-tier daily quota spent on
  duplicate per-push reviews is wasted.
- Pin `the-pr-agent/pr-agent` to a commit SHA, as with every other action here.
- Skip fork pull requests, which receive no secrets, rather than letting the job
  fail red.
- Put the invariants from rules 1–3 above in `.pr_agent.toml`
  (`[pr_reviewer] extra_instructions`), so all three reviewers judge the same
  things.

