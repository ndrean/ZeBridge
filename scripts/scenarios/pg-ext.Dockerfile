# PostgreSQL with PostGIS and pgvector, for the scenarios whose master tables need them (duckdb_replica).
# The Compose dev image is plain postgres; this one is built and started by the scenario itself.
# The official Debian image, not postgis/postgis: that one is amd64 only, and this one carries the
# PostgreSQL apt repository, so both extensions are packages, native on arm64.
FROM postgres:18
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-18-postgis-3 postgresql-18-pgvector \
 && rm -rf /var/lib/apt/lists/*
