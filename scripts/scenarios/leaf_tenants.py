#!/usr/bin/env python3
"""Leaf namespace and membership guards, in a disposable local PostgreSQL database.

The generated reader/writer roles are unique and dropped with the fixture. The
ordinary native PostgreSQL admin is used only to create and remove that fixture.
"""
import os
import pathlib
import subprocess
import uuid

import zb

BRIDGE = pathlib.Path(os.environ.get("ZB_BRIDGE_BIN", zb.ROOT / "zig-out/bin/bridge"))


def main():
    name = "zb_leaf_" + uuid.uuid4().hex[:10]
    reader, writer = name + "_reader", name + "_writer"
    base = [zb._psql_binary(), "-X", "-h", "127.0.0.1", "-p", "5432", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-Atq"]
    failures = 0

    def sql(statement, database=name, check=True):
        result = subprocess.run([*base, "-d", database], input=statement, text=True, capture_output=True)
        if check and result.returncode:
            raise RuntimeError(result.stderr[-2500:])
        return result

    def check(ok, label):
        nonlocal failures
        (zb.ok if ok else zb.bad)(label)
        failures += not ok

    sql(f'CREATE DATABASE "{name}"', "postgres")
    try:
        env = dict(os.environ, DATABASE_READER_URL=f"postgres://{reader}:fixture@127.0.0.1:5432/{name}",
                   DATABASE_WRITER_URL=f"postgres://{writer}:fixture@127.0.0.1:5432/{name}", TARGET_DB=name)
        env.pop("BRIDGE_CDC_PUBLICATION", None)
        rendered = subprocess.run([str(BRIDGE), "--init-sql"], env=env, capture_output=True, text=True, check=True).stdout
        sql(rendered)
        sql(rendered)  # Existing deployments take the same idempotent installation path.
        sql("INSERT INTO zebridge_leaf_scopes (leaf_id, tenant_ids) VALUES ('lf-edge1', ARRAY['acme'])")
        sql("INSERT INTO zebridge_user_tenants VALUES ('lf-edge1.alice', 'acme')")
        sql("SELECT zebridge_invite('lf-edge1.phone', 'acme')")
        check(sql("SELECT count(*) FROM zebridge_user_tenants WHERE principal = 'lf-edge1.alice'").stdout.strip() == "1",
              "registered leaf principals can receive only allowed memberships and invites")
        for statement, label in (
            ("INSERT INTO zebridge_leaf_scopes (leaf_id, tenant_ids) VALUES ('canada', ARRAY['acme'])", "a leaf id without the reserved prefix is refused by PostgreSQL"),
            ("SELECT zebridge_invite('lf-canada', 'acme')", "a reserved leaf id cannot be invited as a plain principal"),
            ("INSERT INTO zebridge_user_tenants VALUES ('lf-canada', 'acme')", "direct membership cannot create a plain principal in the reserved namespace"),
            ("SELECT zebridge_invite('canada.alice', 'acme')", "an unprefixed dotted principal cannot be invited"),
            ("INSERT INTO zebridge_user_tenants VALUES ('lf-edge1.alice', 'other')", "cross-tenant membership is refused"),
            ("UPDATE zebridge_user_tenants SET tenant_id = 'other' WHERE principal = 'lf-edge1.alice'", "changing a membership cannot escape the scope"),
            ("SELECT zebridge_invite('lf-edge1.phone', 'other')", "cross-tenant invitations are refused"),
            ("SELECT zebridge_invite('lf-unknown.phone', 'acme')", "unregistered leaf namespaces are refused"),
            ("SELECT zebridge_invite('lf-edge1', 'acme')", "a leaf namespace cannot also be a hub principal"),
            ("UPDATE zebridge_leaf_scopes SET tenant_ids = ARRAY['other'] WHERE leaf_id = 'lf-edge1'", "a scope cannot strand an existing membership"),
            ("DELETE FROM zebridge_leaf_scopes WHERE leaf_id = 'lf-edge1'", "an active scope cannot be silently deleted"),
            (f"SET ROLE {writer}; UPDATE zebridge_leaf_scopes SET tenant_ids = ARRAY['other']", "the bridge writer cannot change leaf policy"),
            ("INSERT INTO zebridge_leaf_scopes (leaf_id, tenant_ids) VALUES ('lf-bad', ARRAY['acme,other'])", "malformed tenant lists are refused"),
            ("SELECT * FROM zebridge_grant_edge_writes('public.zebridge_leaf_scopes'::regclass)", "leaf policy cannot be exposed to edge mutations"),
        ):
            result = sql(statement, check=False)
            check(result.returncode != 0, label)
        # Simulate a legacy roster predating prefix enforcement. Only this
        # disposable fixture's superuser bypasses the new membership trigger.
        sql("SET session_replication_role = replica; INSERT INTO zebridge_user_tenants VALUES ('lf-hub_alice', 'other'); SET session_replication_role = origin")
        check(sql("INSERT INTO zebridge_leaf_scopes (leaf_id, tenant_ids) VALUES ('lf-hub_alice', ARRAY['acme'])", check=False).returncode != 0,
              "a valid leaf id with a legacy hub-principal membership is refused")
        sql("INSERT INTO zebridge_principal_keys (user_pubkey, principal, tenant_id) VALUES ('fixture_key', 'lf-historical', 'acme')")
        check(sql("INSERT INTO zebridge_leaf_scopes (leaf_id, tenant_ids) VALUES ('lf-historical', ARRAY['acme'])", check=False).returncode != 0,
              "a namespace with previously issued hub credentials is refused")
        sql("DELETE FROM zebridge_user_tenants WHERE principal = 'lf-edge1.alice'; DELETE FROM zebridge_leaf_scopes WHERE leaf_id = 'lf-edge1'")
        check(True, "an idle leaf scope can be retired explicitly")
    finally:
        sql(f'DROP DATABASE "{name}" WITH (FORCE)', "postgres")
        for role in (reader, writer):
            sql(f'DROP ROLE IF EXISTS "{role}"', "postgres")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
