# ZeBridge Flutter Consumer

This is a Flutter implementation of a ZeBridge consumer following the rules outlined in `PROTOCOLE.md` / `PROTOCOL.md`.

## Features
- **NATS Connection**: Connects to the local NATS server using WebSockets (`ws://localhost:8080`).
- **Schema Management**: Listens to the NATS JetStream `schemas` KV bucket, dynamic creation of SQLite tables, tracking dropped and suspended tables.
- **Local DB**: Uses `sqflite` to persist tables safely. 
- **Change Data Capture (CDC)**: Subscribes to the `cdc.>` stream. Applies `INSERT`, `UPDATE`, and `DELETE` (applying last-write-wins semantics via SQLite `ON CONFLICT`).
- **Gap Detection**: Tracks the global `lsn` and `seq` to detect gaps in the stream, triggering snapshot requests for synchronization.
- **Outbox Pattern Ready**: Listens to the `mutation_ack.alice.>` topic for outbox sync confirmation.

## Project Structure
- `lib/main.dart`: Entry point.
- `lib/src/data/db_manager.dart`: Wrapper around `sqflite` providing `upsertRows`, table rebuilds, and `_zebridge_sync` state tracking.
- `lib/src/data/sync_manager.dart`: Manages the NATS JetStream connection, schema syncing, snapshot handling, and CDC application.
- `lib/src/ui/app.dart`: The simple status-tracking UI.

## Getting Started

1. Get dependencies:
   ```bash
   flutter pub get
   ```

2. Run the application:
   ```bash
   flutter run
   ```

Note: Ensure your local `nats-server` and `ZeBridge` instances are running on the default ports described in the protocol before starting the app.
