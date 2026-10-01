# A region answers its own phones: the leaf node

A regional nats-server as a LEAF of the hub, in a container on OrbStack, and the POI
service beside it answering the phones of that region from its own replica. This is how to run it, and what it measured.

## The hub

`scripts/native/nats-server-jwt.conf` listens for leaf links on 7422. A reload does not
open the port; restart the server once.

## The leaf

    examples/08-map/leaf/leaf.sh          # nats-leaf.conf from the hub's conf, then the container
    examples/08-map/leaf/leaf.sh stop

Operator mode with the hub's operator and account JWTs (a phone's creds are verified at
the edge), no JetStream, one remote into the hub with `scripts/native/creds/leaf-nantes.creds`
— a user minted under the account's SERVICE key, because the link carries every
principal's traffic. Phones connect to `nats://127.0.0.1:4223`; monitoring on 8223;
`curl :8222/leafz` on the hub shows the link.

## The responder of the region

    examples/08-map/poi_service.py --url nats://127.0.0.1:4222 --serve-url nats://127.0.0.1:4223 \
        --db /tmp/pois-leaf.duckdb --label leaf

Two connections on purpose: the replica follows the hub (`--url`; the JetStream API does
not cross a leaf link from a standalone hub), the answers go out on the leaf
(`--serve-url`). The hub's own responder runs as before, `--label hub`.

## Who answers

    examples/08-map/leaf/who_answers.py --url nats://127.0.0.1:4223 --asks 50   # a phone on the leaf
    examples/08-map/leaf/who_answers.py --url nats://127.0.0.1:4222 --asks 50   # a phone on the hub

Fifty asks, tallied by the `answered_by` label every answer carries. Measured: a phone
on the leaf is answered by the leaf's responder 50 of 50; stop it, the hub's answers
across the link 50 of 50; start it again, back to the leaf 50 of 50. A phone on the hub
is answered by the hub's. Median 133–136 ms everywhere: the DuckDB query, not the link.
