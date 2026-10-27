-- Runs once, on the first start of an empty data volume (docker-entrypoint-initdb.d).
-- A setting made with ALTER SYSTEM, not a `-c` flag: the command line would win over it, and
-- scripts/scenarios/slot_loss.py lowers it live to invalidate a slot on purpose.
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
