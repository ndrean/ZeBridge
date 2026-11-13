# Enrollment with an OIDC token (proposal)

**Status: a design, parked.** Invites stay the way to enroll. This is taken up when an app wants to grant a membership once and let its users enroll their own devices ([when it is worth it](#when-it-is-worth-it)). When it lands, the user-facing part moves into [AUTHENTICATION.md](AUTHENTICATION.md) and this file keeps the reasons.

An app that already signs its users in with an OpenID Connect provider (Auth0, Cognito, Entra ID, Google, Keycloak, Zitadel…) holds a signed statement of who the user is: the **ID token**. Today it must still ask its backend for an invite and pass the code to the library ([Users: invites](AUTHENTICATION.md#users-invites)). This proposal lets the library hand the ID token to `/enroll` instead, and the bridge checks it itself. Invites stay exactly as they are.

## Table of contents

- [When it is worth it](#when-it-is-worth-it)
- [What changes, what does not](#what-changes-what-does-not)
- [The flow](#the-flow)
- [Configuration](#configuration)
- [Checking the token](#checking-the-token)
- [Binding the token to the device key](#binding-the-token-to-the-device-key)
- [Who the user is: the principal](#who-the-user-is-the-principal)
- [Where the user may go: the tenants](#where-the-user-may-go-the-tenants)
- [A lost reply](#a-lost-reply)
- [Renew, revoke, purge](#renew-revoke-purge)
- [The signing keys (JWKS)](#the-signing-keys-jwks)
- [Clients](#clients)
- [What this does not cover](#what-this-does-not-cover)
- [Tests](#tests)
- [Later: claim mode](#later-claim-mode)
- [Open questions](#open-questions)

## When it is worth it

The issuer is a setting of the deployment, not of a tenant: Alice of `acme` and Bob of `globex` both sign in with Google, so their tokens carry the same issuer. The token says who vouched for the user; the roster still says which tenants they may use, and the app's backend writes it, as it does for an invite.

So for the first device, OIDC and an invite cost the same decision ("Alice may join acme"). The difference is on the next device:

| | Alice's first phone | Alice's second phone |
| --- | --- | --- |
| invite (today) | the backend issues a one-time code; the phone redeems it | the backend issues and delivers another code |
| OIDC, roster | the backend registers Alice's `(issuer, subject)` and grants `acme`; the phone enrolls with a key-bound sign-in | Alice signs in on the new phone; the bridge finds the grant and enrolls it |

An invite authorizes one device. An OIDC grant authorizes the person to enroll future devices through sign-in, for as long as the grant stands. Some apps want that; others want an explicit approval per device. An app whose login response already carries an invite gains little.

## What changes, what does not

| | invite (today) | OIDC token (proposed) |
| --- | --- | --- |
| proof presented at `/enroll` | a one-time 122-bit code, made by your backend | an ID token signed by your provider, bound to the device key |
| your backend at enrollment | creates the invite, ships the code to the app | **not involved**: it granted the identity and its memberships once, beforehand |
| who decides the tenant | the invite row | the roster (`zebridge_user_tenants`), written by your backend ([tenants](#where-the-user-may-go-the-tenants)) |
| device key, JWT, `/renew`, revoke, purge | — | **unchanged**: after `/enroll` the two paths are the same |
| trust added | your backend's inviter login | your provider, and the issuer and audience written in the bridge's config |
| what one grant authorizes | one device | every device the user signs in on, until the grant is removed |

The steady state does not move: the device keeps its own key, renews by signing with it, and never calls the provider or your backend again. Only the step that turns "the app knows who this is" into "this key belongs to principal X in tenants Y" changes.

## The flow

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant A as App + ZeBridge library
    participant IdP as OIDC provider
    participant B as Bridge
    participant PG@{ "type" : "database" }

    A->>A: generate the device key pair (seed stays here)
    A->>A: attempt = 32 random bytes; store (seed, attempt) before sign-in
    A->>A: nonce = enrollmentNonce() = b64url(sha256("zebridge-enroll:" + user_pubkey + ":" + attempt))
    U->>IdP: sign in (the app's usual flow, with that nonce)
    IdP-->>A: ID token (iss, sub, aud, exp, nonce, …)
    A->>B: POST /enroll?user_pubkey=U…&attempt=…  Authorization: Bearer <id_token>
    B->>B: verify signature (cached JWKS), iss, aud, exp, nonce = f(user_pubkey, attempt)
    B->>PG: one transaction: spend the token, resolve (iss, sub) → principal,<br/>memberships from the roster, record the device key
    B->>B: mint the JWT for this key (tagged with the tenants)
    B-->>A: the /enroll payload, as today
```

Steps 6 onwards are the current `/enroll` (`src/http_server.zig`, `handleEnroll`): one transaction on the writer connection, `rosterTenants`, `jwt_mint.mint`, `putTenantMapping`, `identityPayload`. Only how the principal and its memberships are found differs.

## Configuration

Set in `.env.bridge`, next to the bridge's other settings. No setting means OIDC enrollment is off: `POST /enroll` with a bearer token answers `404 {"error":"oidc enrollment not configured"}`.

| setting | example | meaning |
| --- | --- | --- |
| `OIDC_ISSUER` | `https://acme.eu.auth0.com/` | the `iss` the token must carry, compared byte for byte. Discovery reads `<issuer>/.well-known/openid-configuration` |
| `OIDC_AUDIENCE` | `k3J…` (the app's client id) | the `aud` the token must contain. One audience in v1 |
| `OIDC_JWKS_URL` | — | optional: overrides the `jwks_uri` from discovery |
| `OIDC_ALGS` | `RS256,ES256` (default) | accepted algorithms. `none` and `HS*` are never accepted, whatever is written here |
| `OIDC_MAX_TOKEN_AGE_SECONDS` | `3600` | refuse a token whose `iat` is older, even if `exp` is later |
| `OIDC_JWKS_MAX_STALE_SECONDS` | `86400` | how long keys from the last successful fetch stay usable when refreshes fail ([JWKS](#the-signing-keys-jwks)) |

The normal setup is the issuer and the audience. One issuer and one audience per bridge in v1; several apps (iOS, Android, web with different client ids) and several issuers are [open questions](#open-questions). Key binding is mandatory and has no setting.

`bridge --diagnose` gains: discovery reachable, JWKS loaded (key count, algorithms, age of the last successful fetch).

## Checking the token

In this order, each failure a `401` with a short reason and a `🎟️ oidc enrollment refused: <reason>` warning (the log line the scenario greps):

1. Three base64url parts; header `alg` in `OIDC_ALGS`; `typ`, if present, is `JWT`. Never take the algorithm from anything but this allow-list (no `alg: none`, no RS/HS confusion).
2. `kid` found in the cached JWKS, with a key type matching `alg`. Unknown `kid`: one refresh of the JWKS (rate-limited, below), then refuse.
3. Signature: RS256 with `std.crypto.Certificate.rsa.PKCS1v1_5Signature.verify` and `rsa.PublicKey.fromBytes(e, n)` from the JWK; ES256 with `std.crypto.sign.ecdsa.EcdsaP256Sha256` (raw `r‖s`, 64 bytes). Both exist in Zig 0.17's standard library; no new dependency.
4. `iss` equals `OIDC_ISSUER`. `aud` (string or array) contains `OIDC_AUDIENCE`; if `aud` holds several values, `azp` is present and equals `OIDC_AUDIENCE`.
5. `exp` in the future, `nbf` and `iat` not in the future, each with 60 s of skew; `iat` not older than `OIDC_MAX_TOKEN_AGE_SECONDS`.
6. `nonce` ([binding](#binding-the-token-to-the-device-key)).
7. `sub` present, 1–255 bytes.
8. **Spent once.** `sha256(token)` is inserted into `zebridge_oidc_spent (token_hash PRIMARY KEY, user_pubkey, expires_at)` in the enrollment's transaction; a conflict refuses ([a lost reply](#a-lost-reply) says what the device does then). `expires_at` is the end of the token's whole acceptance window, `min(exp, iat + OIDC_MAX_TOKEN_AGE_SECONDS)` plus the 60 s skew, so a row is never reaped while the token could still pass steps 1–7. The sweeper reaps rows past it. Without this, a token copied off the device could enroll a second key for as long as it lives.

The token arrives in the `Authorization` header of a `POST`, never in the query string: the current `GET /enroll?code=…` puts its secret in URLs, which proxies log. A one-time code spent at once is little loss in a log; an ID token valid an hour is more. A browser then sends a CORS preflight (`OPTIONS`) for the header, so the server answers it on `/enroll`.

## Binding the token to the device key

An ID token is a bearer token. Steps 1–7 prove *who signed in*, not *which device is asking*. Spending it once (step 8) stops a second use, but not a first use by whoever got it first (a malicious SDK, a log, a compromised proxy).

So the device key exists before the sign-in, and the app puts it into the token through OIDC's own replay guard, the `nonce` ([OIDC Core, ID token validation](https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation)), together with a fresh random value per enrollment attempt:

```
attempt = 32 random bytes, new for each enrollment attempt
nonce   = base64url(sha256("zebridge-enroll:" + user_pubkey + ":" + attempt))
```

A nonce of the public key alone would be the same at every sign-in of that device; the attempt makes each one distinct. The library stores the seed and the attempt **before** it launches the sign-in, sends both public values with the token, and reuses the same attempt if the reply is lost. The bridge recomputes the nonce from the request. A stolen token then enrolls nothing but the key it was made for, and that key's seed never left the device.

**The contract is one exact claim.** `/enroll` consumes the ID token of the configured issuer, and its `nonce` claim must equal the computed value byte for byte. No variant is accepted (no hashed nonce, no claim of another name), and there is no setting to turn the binding off. A flow in which the app cannot choose the nonce of *that* token is not supported in v1. Firebase is an example: in its Apple flow the app sends a hashed nonce to Apple and Firebase checks it, which says what is in Apple's token, not in the Firebase ID token issued after it. Supporting such a flow needs its own documented claim first.

The libraries expose the value (`enrollmentNonce()`) and generate and keep the device key and the attempt before it, so the app passes it to its sign-in SDK. Most native and web SDKs accept a nonce (AppAuth, Auth0, MSAL, Google Credential Manager). This also qualifies the opening promise: an app already signed in needs a **fresh** sign-in (or a silent one) that carries this nonce; a token cached from an earlier login does not satisfy it.

## Who the user is: the principal

OIDC's stable identity is the pair **(`iss`, `sub`)**; an email can change and be reused. A principal is `[A-Za-z0-9_-]+` (`zebridge_invites_principal_check`) and must not start with the leaf prefix. `sub` often fits nothing of that: Auth0's `auth0|65a…`, Google's 21 digits are fine, an email is not. So the bridge never derives the principal by cleaning up a claim (two subjects could clean up to the same name). It keeps a mapping table:

```sql
CREATE TABLE public.zebridge_identities (
    issuer    text NOT NULL,
    subject   text NOT NULL,
    principal text NOT NULL UNIQUE,   -- same CHECK as zebridge_invites.principal; no leaf namespace
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (issuer, subject)
);
```

The row is created in advance by the app's backend or the DBA, with a principal they choose (the app's own user id), through a new `SECURITY DEFINER` function granted to `zebridge_inviter`:

```sql
SELECT zebridge_grant_identity(p_issuer, p_subject, p_principal, p_tenant);
```

It inserts the identity row (or checks it maps to the same principal) and the membership, refusing a revoked principal like `zebridge_invite` does. It is done once per *user*, at sign-up or when they join a tenant, not per device and not on the login path, so the backend is not in the device's onboarding.

The bridge's writer role gets `SELECT` on `zebridge_identities`, nothing more: in v1 only `zebridge_grant_identity` creates a row, and remapping an identity to another principal is an admin act.

## Where the user may go: the tenants

The JWT's tenant tags must equal the roster, or a client writes rows it cannot read back (§10fn). So the **roster stays the only truth** the mint reads (`rosterTenants`), and in v1 only your backend (or the DBA) writes it, through `zebridge_grant_identity`.

The token proves identity, nothing else. The principal's memberships are whatever `zebridge_user_tenants` holds. No membership: `403 {"error":"no tenant membership"}`, and **no key is recorded**. This matters with a public provider: anyone with a Google account can get a token for your client id, and the roster turns each such stranger away before anything is written.

Letting the provider decide the tenants is [deferred](#later-claim-mode).

## A lost reply

If PostgreSQL commits the enrollment and the reply is lost, the device holds a registered key but no JWT. It does not replay the enrollment; it **recovers the same registration** through the path that already exists:

1. The device key and the attempt are stored before the `POST`.
2. With no reply, the library calls the signed `/renew` with that key (`src/http_server.zig`, the renew handler). If the key is registered, `/renew` returns current credentials after checking revocation, purge and current memberships: proof that the device still holds the seed, and no second sign-in.
3. If `/renew` does not know the key, the transaction did not commit, the token was not spent, and the library sends the same `POST` again with the same attempt.
4. A spent token presented again with the **same** key answers `409 {"error":"already enrolled: renew"}`; with any **other** key it is refused. A retry never binds the token to another key, never recreates a deleted membership and never undoes a purge, since recovery reads the current state through `/renew`.

## Renew, revoke, purge

Unchanged, because enrollment ends with the same row in `zebridge_principal_keys` and the same JWT.

- `/renew` signs with the device key; no ID token, no provider call. A device enrolled with OIDC renews offline for as long as an invited one.
- `bridge --revoke <principal>`: the principal is dead for good (§10dm). An OIDC enrollment for an identity mapped to a revoked principal is refused like an invite for it (the same `NOT EXISTS … revoked_at` arm). So revoking bans the person's *provider identity*, which is what an operator means. Letting them back is an admin `UPDATE zebridge_identities SET principal = '<new>'`, the same "new principal" rule as today.
- `--purge` reaches the device through `/renew`, as today.
- **A refusal from `/renew` is terminal.** The library does not sign the user in again on its own after a revocation, a purge or a lost membership; it reports the refusal, as today. Enrolling again is the app's explicit decision, after the backend has granted again.

## The signing keys (JWKS)

- Fetched at boot (discovery, then `jwks_uri`), refreshed every 6 h and on an unknown `kid`, at most once a minute. A failed refresh keeps the keys in hand and logs a warning, for at most `OIDC_JWKS_MAX_STALE_SECONDS` after the last successful fetch. Past that, OIDC enrollment answers a retryable `503 {"error":"provider keys stale"}` until a refresh succeeds; enrolled devices keep renewing through `/renew`. Keys rotated out at the provider are dropped at the next successful refresh.
- Fetched off the request path, by a small thread using `std.http.Client` (already used by `archive_s3.zig`). It registers its `errdefer should_stop.store(true)` **after** its `defer join` (CLAUDE.md, "A bridge that is up but silent").
- Provider unreachable at boot: OIDC enrollment answers `503 {"error":"provider keys not loaded"}` until the first fetch succeeds; invites, `/renew` and replication are not affected. The bridge does not refuse to start over its provider.
- Bounded: at most 16 keys, 8 KiB per key, 64 KiB per document; RSA moduli 2048–4096 bits.

## Clients

Both twins, the same words in their log lines:

```ts
const zb = new ZeBridge({ bridgeUrl, tables, idToken: () => signIn({ nonce: zb.enrollmentNonce() }) });
await zb.connect();
```

- `idToken` is a function, not a string: the library calls it only when it has no identity (first start, or after a refused `/renew`), so the app signs in only when needed, with the nonce of the key the library now holds.
- libzb: `"id_token"` in `zb_client_connect`'s options, and `zb_client_enrollment_nonce()` in the C ABI, which generates and stores the device key and a fresh attempt if there is none yet. The wrappers (`zb-python`, `zb-dart`, `zb-android`, `zb-react-native`, Swift) pass both through.
- `invite` and `idToken` together: refused at configuration, so an app never wonders which one enrolled it.
- With no reply to `/enroll`, the library recovers through `/renew` first ([a lost reply](#a-lost-reply)).
- After `/renew` answers `403` (revoked, purged, or no membership left), the refusal is final and reported, as today; the library does not call `idToken` again by itself.

## What this does not cover

- **Leaf principals** (`lf-<leaf>.<device>`): enrollment through a leaf stays invite-only. A leaf scope's tenant list is an admin registration, and a provider claim must not reach into it.
- **Services and sensors**: they keep minted `.creds` ([Services and sensors](AUTHENTICATION.md#services-and-sensors)). A provider's `client_credentials` token is a machine identity whose rotation the provider owns; mapping it onto a ten-year `.creds` is a different design.
- **Access tokens.** Only ID tokens, whose audience is the app. An access token's audience is an API, and accepting one would let any API in the same provider tenant enroll devices.
- **NATS auth callout.** Considered and set aside: it would accept the provider's token at every NATS connect, putting a callout service on the connect path and losing the offline renewal.

## Tests

Each fails without its part of the change (CLAUDE.md: restore the old code to prove it).

- Unit (`src/oidc.zig`): RS256 and ES256 vectors from RFC 7515/7520; `alg: none`, `HS256` with the RSA modulus as the secret (the confusion attack), wrong `kid`, wrong `iss`, `aud` missing ours, `azp` foreign, expired, `nbf`/`iat` in the future, too old, a nonce of another key or another attempt, a hashed nonce, oversized JWKS, a modulus under 2048 bits, keys past their stale age.
- SQL: `zebridge_grant_identity` refuses a revoked principal, a leaf namespace, a second principal for the same `(iss, sub)`; the spent table refuses a replay inside its transaction.
- Scenario `scripts/scenarios/oidc_enroll.py`, private stack, with a fake provider: a 60-line Python HTTP server serving discovery and a JWKS, signing tokens with a key it generates (needs `cryptography` in the scenario venv). Cases: a pre-provisioned identity enrolls and syncs its tenant; a stranger is refused and leaves no key row; a replayed token; a token for another key's nonce; key rotation at the provider (new `kid` mid-run); provider down at boot (invites still work, OIDC `503` then recovery); keys past their stale age (`503`, then recovery, `/renew` unaffected); revoke, then the same identity refused; `/renew` after the provider is gone, still working. Two acceptance cases for [a lost reply](#a-lost-reply): commit → lose the reply → restart the app → recovered through `/renew` without another sign-in; commit → lose the reply → revoke → the retry is refused and stays refused. The same flow with a zb-client-ts client and a libzb client.

## Later: claim mode

v1 is one issuer, one audience, mandatory key binding, and memberships pre-provisioned in the roster. Letting a token claim (Auth0's `org_id`, Entra's `tid`, Keycloak groups) open tenants is deferred, for reasons beyond configuration size:

- **A pattern does not establish entitlement.** A tenant-name pattern proves a claimed name is well formed; it cannot prove who was entitled to assign it (a self-service group, a provider-side admin of another customer).
- **Removal does not follow.** The bridge would add memberships and never remove them, and devices renew without the provider, so removing someone at the provider leaves their ZeBridge access in place until the roster is updated.

Claim mode needs its own decision: who grants access, who removes it, and when a removal takes effect.

## Open questions

- **Several audiences** (one client id per platform), with the `azp` rule for multi-valued `aud`.
- **Several issuers.** A table (`zebridge_oidc_issuers`) instead of env settings, so a B2B app adds a customer's Entra tenant without a restart. Each row needs its own audience and tenant rules; the JWKS thread then holds one cache per issuer.
- **Pre-provisioning by verified email.** B2B admins invite `bob@acme.com`, not a `sub` they cannot know yet. `zebridge_grant_email(email, principal, tenant)`, bound to `(iss, sub)` at the first enrollment whose token has that email and `email_verified: true`, then never by email again. Worth it, but email binding has known traps (providers that do not verify, an address reassigned), so it is a separate decision.
- **Should `GET /enroll?code=` move to `POST` too**, for the same reason as the token? A compatible change (accept both for a release), independent of this proposal.
