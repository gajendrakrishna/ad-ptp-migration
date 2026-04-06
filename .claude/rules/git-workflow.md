# Git Workflow

## Worktrees

Worktree base path: `../worktrees`

Commands create worktrees at `<base>/<run-slug>` where `<run-slug>` is generated from the command name and table names (e.g. `scope-dimcustomer-dimproduct`).

## Cleanup

Run `/cleanup-worktrees` after PRs are merged to remove worktrees and branches.
