#!/bin/sh
# Runs once, with postgres-init.sql, on the first start of an empty data volume.
# POSTGRES_HOST_AUTH_METHOD=trust writes `host all all all trust`, which does not cover a
# physical replication connection (the `replication` keyword is not a database): standby.py
# takes a pg_basebackup of this server from the host, and was refused without this line.
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
