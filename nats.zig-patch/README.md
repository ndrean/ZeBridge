# nats.zig-patch (retired)

The ordered series (`01-` … `41-`) that lived here is now the commit history of the fork
[ndrean/nats.zig](https://github.com/ndrean/nats.zig), branch `main`: upstream `b8ffdd9` plus one
commit per patch, in the same order, with `NATS_ZIG_NOTES.md` (the ledger) at its root. The `nats.zig`
submodule pins a commit of that branch; nothing is applied at build time. See `.gitmodules`.

The patches, `check-series.sh` and `new-patch.sh` are in this directory's git history before the
commit that retired them.

Nothing is pending: `zebridge 43` (Zig 0.17.1 in CI, the floor at 0.17.0), `zebridge 44` (a push handler's deinit no longer frees the message before its auto-ack) and `zebridge 45` (the benchmarks on `std.Io.Threaded`, without zio) are on the fork's `main` (merge `135c0da`). The last tag is still `zebridge/b8ffdd9-2` (`315fb29`, `zebridge 42`).
