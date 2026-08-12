import asyncio
import msgpack
from nats.aio.client import Client as NATS
from nats.js.api import ConsumerConfig, DeliverPolicy

async def run():
    nc = NATS()
    print("Connecting to NATS...")
    await nc.connect("nats://bridge_user:bridge_secure_password@localhost:4222")
    js = nc.jetstream()

    table_name = "test_types"

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
            deliver_policy=DeliverPolicy.ALL # Delivers the last 15 minutes of CDCs
        )
    )
    
    print("🎧 Listening for CDCs...")
    
    # Simulate our known snapshot LSN (e.g., we pretend our local DB is at LSN 1000)
    # Any CDC with LSN <= 1000 will be ignored.
    SIMULATED_SNAPSHOT_LSN = 0 
    
    async for msg in sub:
        try:
            # We attempt to unpack msgpack, fallback to JSON if you are testing with --json
            try:
                payload = msgpack.unpackb(msg.data)
            except:
                import json
                payload = json.loads(msg.data.decode())
                
            lsn_str = payload.get('lsn')
            
            # Very basic LSN drop logic (Requires parsing PG LSNs to compare in reality, 
            # but usually it's just string comparison or converting "X/Y" to integers)
            # For logging, we just print everything.
            print(f"[{payload.get('operation')}] LSN: {lsn_str} Data: {payload.get('data')}")
            
            await msg.ack()
        except Exception as e:
            print("❌ Failed to decode CDC", e)

if __name__ == '__main__':
    asyncio.run(run())
