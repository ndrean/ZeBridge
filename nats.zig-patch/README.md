# nats.zig-patch (retired)

The ordered series (`01-` … `41-`) that lived here is now the commit history of the fork
[ndrean/nats.zig](https://github.com/ndrean/nats.zig), branch `zebridge`: upstream `b8ffdd9` plus one
commit per patch, in the same order, with `NATS_ZIG_NOTES.md` (the ledger) at its root. The `nats.zig`
submodule pins a commit of that branch; nothing is applied at build time. See `.gitmodules`.

The patches, `check-series.sh` and `new-patch.sh` are in this directory's git history before the
commit that retired them.

## pending/

A patch for the fork that is not on it yet. `pending/42-nats.zig-subject-length.patch` is the fork's
`zebridge 42` (a subject may be longer than 255 bytes; NATS_ZIG_NOTES.md entry 43 in the fork, and
NOTES.md "Identifiers are measured in bytes"). It applies to the fork's `zebridge` branch at `da738dc`:

```sh
git -C nats.zig checkout -b zebridge da738dc          # the pinned commit
git -C nats.zig am ../nats.zig-patch/pending/42-nats.zig-subject-length.patch
git -C nats.zig tag zebridge/b8ffdd9-2
git -C nats.zig remote add fork https://github.com/ndrean/nats.zig.git
git -C nats.zig push fork zebridge zebridge/b8ffdd9-2
git add nats.zig && git rm nats.zig-patch/pending/42-nats.zig-subject-length.patch   # bump the gitlink
```

Until then the build derives a tenant limit of 46 bytes from the old 255-byte check
(`bridge --limits`); with the patch the same code says 128, with no other change.
