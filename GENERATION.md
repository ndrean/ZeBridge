# Shared rows and the gap check

A tenant table can hold rows of the open tenant (`_default`). Those rows travel on the
public stream, `CDC_PUBLIC`, not on the tenant's own stream. So a client of tenant `acme`
reads that table from two streams, and keeps a **position** on each: the last message it
applied.

Each stream drops old messages after a while. When a client comes back, it checks each
position against the stream's first message: if messages it never applied were dropped,
that is a **gap**, and the client must reload the table from a snapshot.

A client that never had a reason to read `CDC_PUBLIC` keeps position 0 there. Once the
stream has dropped its first messages, position 0 looks like a gap, although nothing was
missed. Each snapshot therefore records two **cuts**, the stream sequences it covers:
`cutoff_seq` on the tenant's stream, and `shared_cutoff_seq` on `CDC_PUBLIC`. A client
seeded from that snapshot resumes `CDC_PUBLIC` at the shared cut (`streamResume`, a rule
of libzb's core that zb-client-ts runs as WebAssembly), and reloads only when the shared
cut itself has fallen off the stream.

```mermaid
sequenceDiagram
    autonumber
    participant A as Alice's client
    participant P as CDC_PUBLIC
    participant G as Producer

    G->>G: cut g8: cutoff_seq 39 on CDC_acme<br/>+ shared_cutoff_seq 6390 on CDC_PUBLIC
    A->>A: seeded from g8: records shared cut 6390 for site_survey
    P->>P: first message moves to 6276
    A->>P: reconnect: gap check
    P-->>A: first = 6276
    Note over A: site_survey is covered: 6390 ≥ 6276 − 1<br/>nothing after its snapshot was dropped
    A->>A: no reload: resume at once
    P->>P: later, first passes 6391 (the shared cut falls off)
    G->>G: next tick: an empty delta with a fresh shared cut
    Note over A,G: the coverage follows the stream's pruning
```

How the chain and the stream fit together in general: [OPERATIONS, Catching up: the chain
and the stream](OPERATIONS.md#catching-up-the-chain-and-the-stream). The rules a client
follows: [PROTOCOL §6](PROTOCOL.md#6-seeding--generation-chains-). Tested by
`scripts/scenarios/gen_follow.py` (by hand) and `shared_gap.py`.
