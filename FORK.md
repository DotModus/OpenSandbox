# Patch branch

Base: upstream commit `1ea4e385` on `main`, **not** a release tag.

This branch previously sat on `k8s/image-committer/v0.1.1`. It was re-cut
because the committer was rewritten upstream after that tag: it no longer
shells out to `nerdctl` for the rootfs commit path, and `4eed1897`
("recover missing snapshot image content") fixes a real failure where a node
holds a container's unpacked snapshot but not the packed content of its
base image, so the push cannot assemble a manifest. That fix cannot be taken
on its own — the package it lives in did not exist at the old tag.

`1ea4e385` is the newest upstream commit that touches this component. Later
commits on `main` do not, so taking more would add churn without adding fix.
There is no release containing this yet; re-cut onto the tag when one ships.

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

2. **`GOPROXY` is an `ARG`.** The hard-coded value is unreachable outside
   China — the TLS handshake fails — so the image cannot be built elsewhere
   at all. The upstream value stays the default, so the upstream build is
   unchanged.

Drop patch 1 when upstream stops committing containers a snapshot cannot
restore from. Drop patch 2 when the proxy is configurable upstream.

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
