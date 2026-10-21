# @zebridge/client

The ZeBridge client for browsers and Node: a local SQLite replica of PostgreSQL tables,
kept in sync over NATS JetStream, that keeps working offline and sends its writes back
when the connection returns. It needs a running [ZeBridge](https://github.com/ndrean/zebridge)
bridge.

```sh
npm install @zebridge/client
```

```ts
import { ZeBridge } from '@zebridge/client';

const zb = new ZeBridge({ bridgeUrl: 'https://bridge.example.com', invite: code, tables: ['orders'] });
await zb.connect();                       // enroll once, seed the tables, follow the changes
zb.onChange('orders', refresh);           // a table changed: re-read it
const open = await zb.query('SELECT * FROM orders WHERE status = ?', 'open');
await zb.mutate('orders', 'UPDATE', { id: 7 }, { status: 'done' });  // applied locally at once, sent when online
```

In a browser the replica lives in OPFS (the page needs cross-origin isolation); in Node it is a file, through `better-sqlite3` and `@nats-io/transport-node` version 3.4.0 or later, which you install next to it. The package picks the right one by itself: `import` from `@zebridge/client` in both.

On a seeding leaf, pass `jsDomain: 'hub'` and `seedDomain: '<leaf-domain>'`. Generation manifests and object bytes are read from the leaf first, with hub fallback when its copy is missing, stale or incomplete. Schemas, membership, CDC and writes use `jsDomain`. Enrollment persists ordered native and WebSocket endpoint lists, leaf first and hub second. Connection failures try the next endpoint with the same identity, replica and queued writes; recovered leaves do not interrupt a healthy hub connection. An explicit comma-separated `natsUrl` replaces the enrolled list. A live leaf whose hub link is broken does not trigger a connection switch yet.

- [Clients](https://github.com/ndrean/zebridge/blob/main/CLIENTS.md): the configuration, the API, how writes resolve
- [README](https://github.com/ndrean/zebridge#readme): the project, the bridge, the database side

Apache-2.0.
