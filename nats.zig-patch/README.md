# nats.zig-patch (retired)

The ordered series (`01-` … `41-`) that lived here is now the commit history of the fork
[ndrean/nats.zig](https://github.com/ndrean/nats.zig), branch `zebridge`: upstream `b8ffdd9` plus one
commit per patch, in the same order, with `NATS_ZIG_NOTES.md` (the ledger) at its root. The `nats.zig`
submodule pins a commit of that branch; nothing is applied at build time. See `.gitmodules`.

The patches, `check-series.sh` and `new-patch.sh` are in this directory's git history before the
commit that retired them.

Nothing is pending: `zebridge 42` (a subject may be longer than 255 bytes) is on the fork's `zebridge` branch (`315fb29`, tag `zebridge/b8ffdd9-2`).
