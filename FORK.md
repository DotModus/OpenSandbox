# Patch branch

Base: upstream tag `release-1.1.1` (commit `531700ca`), the stable 1.1.1
umbrella release.

## Patches carried here

Two patches to how execd runs code in a Jupyter kernel
(`components/execd/pkg/jupyter/execute`). Both make a run's `/code` stream end
when that run's execution ends, and no sooner.

### 1. execd ends a Jupyter run when its kernel stops

A run whose kernel stops before replying never ended. Deleting the context of
a running cell makes the kernel broadcast `shutdown_reply` and an `idle`
status, and then it is killed.
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

Where the stream used to hang, a client now sees the events of an ordinary
failing cell: `status`, `error` (`ename` `KernelStopped`), then
`execution_complete`, and the stream closes. Nothing else changes. (Before
patch 2, an `idle` for another request could start finalization first, and
`execution_complete` then came before the error.)

What this does not cover:

- **Code that holds the GIL.** ipykernel answers a shutdown request from its
  control thread, which needs the GIL. Native code that keeps the GIL blocks
  it, so no `shutdown_reply` arrives; the kernel is killed after the shutdown
  wait, and Jupyter Server sends no `restarting` or `dead` for a kernel it shut
  down on purpose. That run still waits until the client gives up. Native code
  that releases the GIL while it works (a DuckDB query, for one) is covered.
- **A websocket that drops mid-run.** `receiveMessages` stops on a read error
  and the run waits until the client gives up, as before. Ending the run there
  needs the reader to know which run owns the connection (a late read error
  from one run's closed socket must not end the next run on the same client).
  Since patch 2 each reader reads only its own connection, but a read error
  still ends nothing.

The `components/execd/pkg/jupyter/execute` package is unchanged between
`release-1.1.1-rc.1`, this tag, upstream `main` at the time of the cut, and
`release-1.2.0-rc.1`, so upstream has not fixed this yet.

Drop this patch when an upstream release ends a run whose kernel stops.

### 2. execd matches messages to the run's request and wakes a stalled kernel

About one `/code` run in 50 on an existing context never ended. execd
reported `execution_complete` almost at once, then the stream sent only
pings. The context stayed busy for good: interrupting did nothing, and
deleting the context ended the stream only through patch 1. Two defects
combine here.

**The kernel left the request unread.** The kernel never read the run's
`execute_request`: it published no `busy`, no output and no reply, and Jupyter
Server reported it idle. The request was still queued on the kernel's shell
socket; opening any new websocket to the kernel made the kernel read it, and
the run then finished with the right result. ipykernel 7.0 to 7.3 reads shell
requests through a `ZMQStream` but sends replies directly on the same socket,
and that send can consume the read event for a request arriving at the same
moment ([ipython/ipykernel#1529](https://github.com/ipython/ipykernel/pull/1529),
fixed in ipykernel 7.4.0). The request is then not read until something else
wakes the socket. `opensandbox/code-interpreter:v1.1.0` ships ipykernel 7.3.0.
execd makes the collision common: it connects to the kernel once per run;
on every new websocket Jupyter Server sends a `kernel_info_request` on the
shell and control channels (unless it believes the kernel is busy); and execd
sends the run's request as soon as it is connected, so the shell reply to that
`kernel_info_request` often goes out just as the request arrives. Re-arming
the kernel's stream after each reply (a local patch to the kernel, to the same
effect as the ipykernel 7.4.0 fix) removed every hang (0 in 300 runs).

If nothing for the run's request has arrived 500 ms after it was sent, execd now
opens and closes another websocket to the kernel; Jupyter Server connects new
sockets to the kernel for it, which wakes the shell socket. It tries up to four
times, doubling the delay (after 0.5, 1.5, 3.5 and 7.5 s). Before each attempt
it stops if anything for the request has arrived, if the run has ended (its
result channel is closed), or if the run's connection has been closed, as
happens when the run's context is cancelled. A kernel that picked up the
request is never woken, however long the run takes. Sending another message on
the run's own connection does not work (tried: every stalled request stayed
unread through all four attempts): it queues behind the unread request on the
same zmq pipe.

The 500 ms count from when execd writes the request, and a run can receive
nothing for longer without any stall: Jupyter Server forwards the request only
after its own `kernel_info_request` round on the new websocket, and a kernel
still busy with an earlier request (a run whose client gave up, say) sends
nothing for the new one until it finishes. Such a run gets up to four
wake-ups, each a websocket opened and closed (Jupyter Server sends no
`kernel_info_request` to a kernel it believes busy), and an info-level log line
that says nothing has arrived yet.

**Messages were not matched to the request.** A run's websocket also carries
messages for other requests: the `busy` and `idle` statuses for Jupyter
Server's `kernel_info_request`, usually before the run's own, and the output
and reply of an earlier run that is still going. The handlers ignored
`parent_header`, so that `idle` started `finalizeExecution` on almost every
run and `execution_complete` could come before the result (in 24 of the 300
upstream runs below), and another request's output, error or execution count
was reported as the run's. An earlier request's reply could also end
finalization before the run's own result arrived, and an error in it made a
later kernel stop look like a run that had already replied, so no
`KernelStopped` error was sent. Replies, output and `idle` statuses now count
only when their parent is the run's `execute_request`. `shutdown_reply`,
status `restarting` and status `dead` still end the run whatever the parent
(patch 1).

Each kernel websocket is also now read only by the reader started for it.
The reader used to look up the client's current connection before every
read, so a reader still handling a message when its run disconnected could
go on reading the next run's connection alongside that connection's own
reader.

Measured with a local `opensandbox-server` 1.1.1 and
`opensandbox/code-interpreter:v1.1.0` (ipykernel 7.3.0), running `z = i; z`
back to back on one context through the Python SDK 1.1.1, a run counted as hung
when its stream had not ended after 8 s:

| execd | runs | hung | `execution_complete` before the result |
| --- | --- | --- | --- |
| upstream `release-1.1.1` | 300 | 6 (deleting the context did not end them) | 24 |
| patch 1 only | 300 | 6 (deleting the context ended them, `KernelStopped`) | 43 |
| patches 1 and 2 | 600 | 0 | 0 |
| patches 1 and 2, after review | 600 | 0 | 0 |

With both patches the kernel left 10 and 19 of the requests unread in the two
runs; one wake-up each made it read them, those runs took about 0.5 s longer,
and every run returned its own result.

What this does not cover:

- **A kernel that stays stuck.** After four wake-ups (7.5 s) execd stops
  trying, and the run waits as before. Every stalled request seen needed one.
- **An aborted request.** A kernel that aborts a request replies with status
  `aborted` and no execution count, and `finalizeExecution` keeps waiting
  (unchanged upstream behaviour). ipykernel aborts only requests queued behind
  one that failed, so this needs a run sent while an earlier run on the same
  context is still executing.
- **Kernel-wide events between connecting and registering.** A `dead` or
  `restarting` status that reaches a run between its connect and its handler
  registration (microseconds) goes to the previous run's handlers and is
  lost to this run.

`components/execd/pkg/jupyter/execute` and `pkg/runtime/jupyter.go` are
unchanged between this tag and upstream `main` on 2026-10-20.

Drop the message matching and the reader change when an upstream release
matches a run's messages to its request. Drop the wake-up when the kernels
execd drives run ipykernel 7.4.0 or later. `opensandbox/code-interpreter:v1.1.0`
can select Python 3.10 to 3.14, each with ipykernel 7.3.0, and ipykernel 7.4.0
requires Python 3.11 or later, so that cannot happen while the image keeps its
Python 3.10 environment.

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
