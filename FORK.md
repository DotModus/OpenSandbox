# Patch branch

Base: upstream tag `release-1.1.1-rc.1` (commit `e5f9102f`), the umbrella
release candidate for the 1.1 line.

This branch replaces the previous patch branch, which sat on upstream commit
`1ea4e385` on `main` because no release then contained the committer
rewrite and the `4eed1897` base-content re-fetch fix. Both are in the 1.1
line, so the patches are re-cut onto a release tag as that branch intended.
The previous branch is kept unchanged so that images built from it stay
reproducible.

Between `1ea4e385` and this tag the image-committer changed only in license
headers, removal of a dead legacy result writer, and a QEMU-path pause flag.
Its command-line arguments, environment (`CONTAINERD_SOCKET`,
`SNAPSHOT_REGISTRY_INSECURE`, `SOURCE_POD_UID`,
`SOURCE_IMAGE_REGISTRY_INSECURE`), registry-credential mount and termination
message are unchanged, so the controller at this tag and the committer built
from this branch speak the same contract.

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
   unchanged.

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
