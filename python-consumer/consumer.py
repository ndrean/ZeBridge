import asyncio
import msgpack
from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy

async def run():
    nc = NATS()
    print("Connecting to NATS...")
    await nc.connect("nats://bridge_user:bridge_secure_password@localhost:4222")
    js = nc.jetstream()

    table_name = "users"

    # 1. Fetch Schema
    try:
        kv = await js.key_value("schemas")
        schema_entry = await kv.get(table_name)
        schema = msgpack.unpackb(schema_entry.value)
        print(f"✅ Schema fetched for {table_name}: {schema}")
    except Exception as e:
        print(f"⚠️ Schema not found or KV error: {e}")

    # 2. Subscribe to CDCs (This represents the client joining and filtering)
    print(f"Subscribing to CDC Stream for {table_name}...")
    
    # In a real sync, we would get Snapshot-Time and use opt_start_time.
    # For this test snippet, we just deliver everything in the stream to see the overlap logic.
    sub = await js.subscribe(
        f"cdc.{table_name}.>", 
        stream="CDC",
        config=ConsumerConfig(
            deliver_policy=DeliverPolicy.NEW
        )
    )
    
    print("🎧 Listening for CDCs...")
    
    # Simulate our known snapshot LSN (e.g., we pretend our local DB is at LSN 1000)
    # Any CDC with LSN <= 1000 will be ignored.
    SIMULATED_SNAPSHOT_LSN = 0 
    
    while True:
        try:
            msg = await sub.next_msg()
            # We attempt to unpack msgpack, fallback to JSON if you are testing with --json
            try:
                payload = msgpack.unpackb(msg.data)
            except:
                import json
                payload = json.loads(msg.data.decode())
            # ZeBridge publishes batches as an array of events
            if not isinstance(payload, list):
                payload = [payload]

            for event in payload:
                lsn_str = event.get('lsn')
                print(f"[{event.get('operation')}] LSN: {lsn_str} Data: {event.get('data')}")
            
            await msg.ack()
        except Exception as e:
            print("❌ Failed to decode CDC:", e)

if __name__ == '__main__':
    asyncio.run(run())
