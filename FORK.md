# Patch branch

Base: upstream tag `release-1.1.1` (commit `531700ca`), the stable 1.1.1
umbrella release.

This branch replaces the previous patch branch, which sat on the release
candidate `release-1.1.1-rc.1` (`e5f9102f`). The patches are re-cut onto the
stable tag because the controller at that tag writes the commit Job this
committer runs in, and upstream ships the two together. The previous branch is
kept unchanged so that images built from it stay reproducible.

Between `release-1.1.1-rc.1` and this tag the image-committer's own sources
changed only in tests, but `kubernetes/go.mod` moves
`github.com/containerd/containerd` from v1.7.35 to v1.7.36 (a CVE fix), and the
committer links it. So the binary differs and the image must be rebuilt on
this tag; the rc.1 build is a rollback target, not an equivalent.

The controller's commit Job keeps the same command-line arguments,
environment and registry-credential mount, and reads the same termination
message. The controller sets `CONTAINERD_SOCKET`, `SOURCE_POD_UID` (when the
source pod is known), `SNAPSHOT_REGISTRY_INSECURE` (when its flag is on) and
the QEMU-only variables. The committer also reads
`SOURCE_IMAGE_REGISTRY_INSECURE`, which the controller does not set; only an
image-committer pod-template overlay can.

Two controller changes touch snapshot targets, neither on the wire:

- Snapshot image names now go through `--snapshot-image-uri-template`. Left
  empty, the default template produces the same
  `<registry>/<sandbox>-<container>:<tag>` names as before. Names are now
  validated before the Job is created (parsed, tag required, no digest, no
  duplicate targets), and an invalid one fails the snapshot with
  `InvalidSnapshotImage`.
- A snapshot that re-enters `Pending` while its commit Job exists now finds
  that Job before resolving targets again, so it no longer re-resolves and
  re-persists them. Retries of the Job's pod always reused the same
  arguments.

So the controller at this tag and the committer built from this branch speak
the same contract.

## Patches carried here

1. **Skip the containers a snapshot is never restored from.** A snapshot is
   restored from the container named `sandbox`, but every pod container is
   still handed to the committer, so a stateless sidecar is committed and
   pushed per sandbox and nothing ever reads the result. `egress` and the
   `execd-installer` init container are dropped in both the commit and the
   `unpause` path, before anything is paused. `SNAPSHOT_SKIP_CONTAINERS`
   overrides the list; an empty value restores commit-everything behaviour.
   The list may never contain `sandbox` — the committer fails closed rather
   than report a snapshot that cannot be restored.

   Kept in `pkg/imagecommitter/cli/skip.go` rather than inlined, so upstream
   changes to `cli.go` do not conflict with it on the next re-cut.

   Upstream at this tag still commits every container, sidecar included, so
   the patch is still needed.

2. **`GOPROXY` is an `ARG`.** The hard-coded value is unreachable outside
   China — the TLS handshake fails — so the image cannot be built elsewhere
   at all. The upstream value stays the default, so the upstream build is
   unchanged. At this tag `kubernetes/Dockerfile` already takes `GOPROXY` as
   an `ARG`; `Dockerfile.image-committer` still hard-codes it.

Drop patch 1 when upstream stops committing containers a snapshot cannot
restore from. Drop patch 2 when the proxy is configurable upstream.

Known caveat: with a container skipped, the controller's own `spec.pause`
resume cannot rebuild the skipped container from the snapshot. At this tag
it fails safely with a `ResumeFailed` condition instead of an image pull
back-off. Snapshot restore through the lifecycle server selects `sandbox`
only and is unaffected.

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
