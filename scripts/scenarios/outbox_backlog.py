#!/usr/bin/env python3
"""One real device per client implementation, 5,000 durable writes, automatic recovery.

The 60-second drain budget catches a fixed per-pass pacing regression (the old
8-per-5s implementation would need at least 52 minutes). This is a functional
latency guard, not a claim about saturated bridge throughput.
"""
import asyncio, os
import outbox_recovery

async def main():
    overrides = {'ZB_OUTBOX_STORM_CLIENTS':'1','ZB_OUTBOX_PENDING':'5000','ZB_OUTBOX_DRAIN_SECONDS':'60','ZB_OUTBOX_CLIENTS':'native','ZB_OUTBOX_CDC':'0','ZB_OUTBOX_LEGACY_PUMP':'0','ZB_OUTBOX_IDLE_TAIL':'0'}
    saved = {k:os.environ.get(k) for k in overrides}
    code = 0
    try:
        os.environ.update(overrides)
        for mode,idle in (('native','0'),('native','1'),('ts','0')):
            os.environ['ZB_OUTBOX_CLIENTS'] = mode
            os.environ['ZB_OUTBOX_IDLE_TAIL'] = idle
            print('\n5,000-write single-device backlog: '+mode+(' with idle CDC tail' if idle=='1' else ''), flush=True)
            code |= await outbox_recovery.main()
    finally:
        for k,v in saved.items():
            if v is None: os.environ.pop(k,None)
            else: os.environ[k] = v
    return code

if __name__ == '__main__': raise SystemExit(asyncio.run(main()))
