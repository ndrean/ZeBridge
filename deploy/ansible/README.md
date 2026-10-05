# ZeBridge on a VPS, with Ansible

One playbook sets up a Debian server running ZeBridge behind Cloudflare:

* NATS with TLS: `nats.<domain>:4222` for phones and services (Let's Encrypt), and a websocket on 8080 for browsers through Cloudflare (Cloudflare's origin certificate);
* the bridge and the sweeper, as systemd services;
* HAProxy for `bridge.<domain>`: Cloudflare only, `/enroll`, `/renew` and `/status`;
* Alloy and the NATS exporter, sending metrics and logs to Grafana Cloud;
* the airports responder of `examples/10-airports` (optional);
* routing (`examples/13-routing`, optional): Valhalla in a container on loopback, and `zb-respond` as a systemd service answering `query._default.route`, `.matrix` and `.tour`. The tiles are built on your machine and copied as they are;
* and, last, `bridge --diagnose` as the service user: the run fails unless it is clean.

It can be run again at any time: each step checks before it changes anything.

## Before the first run

**On your machine.** Ansible (`brew install ansible`, or `pipx install ansible`), and the Linux builds:

```sh
deploy/build-linux.sh            # x86_64; `aarch64` for an ARM server
```

**At Cloudflare** (once, by hand):

* DNS records for the server's IPv4 and IPv6: `bridge` and `ws` proxied (orange), `nats` DNS only (grey). Delete records the registrar left (a parking address, for example).
* SSL/TLS: **Full (strict)**; an origin certificate for `bridge.<domain>` and
  `ws.<domain>` (or `*.<domain>`).
* Rules → Origin Rules: Hostname **equals** `ws.<domain>` → Destination Port **8080**.
* An API token with Zone → DNS → Edit, on this zone only (for Let's Encrypt).

**In the database** (the DBA, from their machine): the init SQL, the publication and the tables, as SUPABASE_TEST.md or the main README says. The playbook never connects as an administrator.

**The settings**, in `deploy/ansible`:

```sh
cp inventory.example.yml inventory.yml                       # the server's address and user
cp vault.example.yml group_vars/zebridge/vault.yml           # the secrets, filled in
ansible-vault encrypt group_vars/zebridge/vault.yml
```

and `group_vars/zebridge/vars.yml` for the domain, the slot, the publication and the e-mail Let's Encrypt writes to. `inventory.yml` and `vault.yml` are git-ignored.

## Run

```sh
cd deploy/ansible
ansible-playbook site.yml --ask-vault-pass
```

The NATS configuration is generated once, by `bridge --init-nats operator`: a later run keeps it, since a new operator would lock out every enrolled device. On a server set up by hand, the playbook adopts what is there, as long as the files are where the main README puts them (`/etc/zebridge`): hand-written `tls {}` and `leafnodes {}` blocks are kept, and so are the secret files already there (next section). Try it first with `--check --diff`: it changes nothing and shows every difference.

**Secrets.** Each secret from `vault.yml` is written only where its file is missing: a server keeps its own certificates, tokens and database URLs, and the vault matters for a new server. To replace them from the vault (a rotation), add `-e zb_rewrite_secrets=true`.
A run that would write a placeholder from `vault.example.yml` stops before writing anything.

## Leaf nodes

`leaf.yml` sets up the hosts of the inventory's `leaves` group (see `inventory.example.yml`: each one names its credentials, `leaf_name`, and its DNS name, `leaf_host`, with A and AAAA records, DNS only):

```sh
ansible-playbook leaf.yml --ask-vault-pass
```

It reads the trust block (operator, accounts) of the hub's `nats-server.conf` on every run, so a leaf follows each `--init-nats --update` on the hub. It mints a leaf's credentials on the hub (`bridge --mint-leaf`) only when the leaf has none; `-e zb_rewrite_secrets=true` mints new ones. Then the Let's Encrypt certificate for `leaf_host`, the server with TLS on 4222 and a websocket (`leaf_ws_port`, `leaf_allowed_origins` in `group_vars/leaves/vars.yml`), the firewall, and a last check that the link is up (`/leafz`). The hub's side (its domain, `leafnodes` on 7422, the firewall for the leaves' addresses) is in `site.yml`: `zb_js_domain` and `zb_leaf_addresses` in `group_vars/zebridge/vars.yml`.

## After the first run

* Move `/etc/zebridge/operator.store` off the server (a password manager, an offline disk): it signs everything. The playbooks need it only for what they create once: the airports and routing responders' creds, a leaf's creds, and a JetStream domain added to a running stack. Put it back for those runs.
* Check from outside: `https://bridge.<domain>/status` answers 200, and a direct connection to the server on 443 is refused (403).
* Grafana Cloud: `bridge_connected{environment="production"}` is 1, and `{unit="zebridge.service"}` shows the bridge's log.

## What each step guards against

The order and the checks come from setting a server up by hand:

* `--check` runs the steps that only read (Cloudflare's lists, the installed nats-server's version, the diagnosis), so a dry run sees what a real one would;

* the Let's Encrypt certificate is copied where NATS reads it on every run, not only after a renewal (the renewal hook alone left NATS without a certificate);
* every edit of `nats-server.conf` passes `nats-server -t` before it is written, and HAProxy's file passes `haproxy -c` (a mistyped field stops NATS entirely);
* once 4222 speaks TLS, the bridge and the services dial `tls://nats.<domain>:4222`, kept on the machine by `/etc/hosts` (TLS checks the name, not the address);
* Cloudflare's address lists are joined line by line (the IPv4 list ends without a newline);
* the diagnosis runs as `zebridge`, with both env files loaded in the same shell, so a permission or a missing variable shows here rather than at the next restart.
