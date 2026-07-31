# Postgres patch series

Patches applied on top of the pinned Postgres source in `vendor/postgres`.
They are applied in filename order by `scripts/pg-apply-patches.sh`, so names are `NNNN-short-description.patch` with a four digit sequence number.

The series is empty right now.
The first entries will be the smgr routing patch and the WAL hook patch from milestone 1.

To add a patch:

1. `make pg-patch` to get the tree to the current series state.
2. Edit files under `vendor/postgres`. If the patch adds new files, `git -C vendor/postgres add -N` them so they show up in diffs and the apply script's dirty check protects them.
3. `git -C vendor/postgres diff > patches/NNNN-short-description.patch` with the next free number.
4. `make pg-patch` again to prove the series still applies from scratch, then `make pg-build`.

Rules:

- Patches never get edited in place once merged, a fix is a new patch. That keeps old commits rebuildable.
- Keep each patch to one concern with a comment block at the top of the diff explaining why it exists.
- The pinned commit only moves in a dedicated PR that also proves the whole series still applies, see docs/postgres.md.
