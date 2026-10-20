# Patch branch

Base: upstream tag `release-1.1.1` (commit `531700ca`), the stable 1.1.1
umbrella release.

## Patch carried here

**execd ends a Jupyter run when its kernel stops.** A run whose kernel stops
before replying never ended. Deleting the context of a running cell makes the
kernel broadcast `shutdown_reply` and an `idle` status, and then it is killed.
It never sends the cell's `execute_reply`, and Jupyter Server keeps the kernel
websocket open, so `finalizeExecution` waited for that reply forever and the
`/code` stream stayed open, sending only pings, until the client gave up. A
kernel that dies on its own (status `restarting` or `dead`) hung the same way.

Deleting the context is the only way a client can stop code that ignores
interrupts (a long join in native code, say) without destroying the whole
sandbox, so a stream that never ends turns a recoverable stop into a lost
sandbox.

`shutdown_reply`, status `restarting` and status `dead` now end the run in
flight (`components/execd/pkg/jupyter/execute`). execd sends an error result
with `ename` `KernelStopped` and lets `finalizeExecution` close the stream. A
run that had already replied is finalized without the extra error. As in
`handleExecutionError`, the error is sent before it is recorded, so a
`finalizeExecution` already polling cannot close the channel first.

A client sees one new `error` event (`ename` `KernelStopped`) where the stream
used to hang; nothing else changes.

The `components/execd/pkg/jupyter/execute` package is unchanged between
`release-1.1.1-rc.1`, this tag, upstream `main` at the time of the cut, and
`release-1.2.0-rc.1`, so upstream has not fixed this yet.

Drop this patch when an upstream release ends a run whose kernel stops.

## Rules for this branch

- Do not merge or fast-forward upstream `main` into it. Re-cut it onto a
  newer pinned commit or tag instead, and say which.
- Do not add unrelated work; one branch, one concern.
- Never open a pull request or issue against `opensandbox-group`. `gh pr
  create` from a fork defaults to the parent.
- **Never reference anything downstream of this repository.** No product
  names, private repository names, internal tracker or document paths,
  registries, environments, deployments, or customer names — in code,
  comments, documentation, file names, branch names, commit messages, or
  pull request text. This is a public repository. The patches are public and
  that is fine; where they are consumed is not.
