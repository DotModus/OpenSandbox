# Patch branch

Base tag: `k8s/image-committer/v0.1.1`

Concern: a snapshot is only ever restored from the container named `sandbox`,
but `image-committer` commits and pushes every container it is given and fails
the job on any push error.

Landed: `kubernetes/cmd/image-committer` skips the containers a snapshot is
never restored from (`egress`, `execd-installer`) in both the commit and the
`unpause` path, and records them in the termination message.
`SNAPSHOT_SKIP_CONTAINERS` overrides the list; an empty value restores
upstream commit-everything behaviour. Drop this branch when upstream ships its
own skip-or-tolerate.

## Rules for this branch

- Do not merge or fast-forward upstream `main` into it.
- Do not add unrelated work; one branch, one concern.
- Never open a pull request or issue against `opensandbox-group`. `gh pr
  create` from a fork defaults to the parent.
- **Never reference anything downstream of this repository.** No product
  names, private repository names, internal tracker or document paths,
  registries, environments, deployments, or customer names — in code,
  comments, documentation, file names, branch names, commit messages, or
  pull request text. This is a public repository. The patch is public and
  that is fine; where it is consumed is not.
