# Development Workflow

GitHub issues are the canonical source for requirements, tasks, bugs, and follow-up work. Slock is the coordination layer for short status updates, cross-agent handoffs, and decisions that need attention.

## Issue Intake

Each issue should include:

- Background: why this work exists.
- Goal: the concrete outcome to deliver.
- Non-goals: boundaries that prevent scope growth.
- Implementation notes: suggested files, constraints, and dependencies.
- Acceptance criteria: observable behavior that must be true.
- Verification: exact commands or checks to run.
- Risk: known uncertainty or follow-up concern.

If an issue is too broad to implement directly, first add a design or execution-plan comment and wait for review before coding.

## Branches

Use a branch name that starts with the issue number:

```bash
git checkout -b issue-N-short-name
```

For tightly coupled setup work, one PR may cover multiple issues if the PR body clearly links and verifies each issue.

## Implementation

Before editing:

1. Read the issue and linked discussion.
2. Check `git status --short --branch`.
3. Inspect nearby code and existing tests.
4. Post a short Slock note when coordination is needed.

During implementation:

- Keep changes scoped to the issue.
- Prefer existing project patterns.
- Add or update tests when behavior or workflow changes.
- Do not refactor profiler core internals unless the issue explicitly allows it.

## Verification

At minimum, run:

```bash
bash tests/run_all_tests.sh
```

If the issue documents additional commands, run those too. Paste relevant command results into the PR body.

## Pull Requests

PRs must include:

- Linked issue, using `Closes #N` when the PR fully resolves it.
- Summary of changed files or behavior.
- Verification commands and results.
- Remaining risk or follow-up issues.

The PR should not rely on Slock history to explain what changed. Important decisions belong in the issue or PR.

## Status Updates

Use Slock for:

- Start/stop status on active work.
- Blockers that need coordination.
- Handoffs between architecture, coding, and QA.
- Human decisions or permissions.

Keep long-term requirements, acceptance criteria, and implementation discussion in GitHub issues/PRs.
