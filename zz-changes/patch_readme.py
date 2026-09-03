import re

with open("README.md", "r") as f:
    text = f.read()

replacement = """### Client to NATS dance (Authentication)

ZeBridge **completely decouples** your application's authentication (passwords, OAuth, session cookies) from the data-sync authentication (NATS). It does not know or care how you authenticate your users. It only handles the minting of **NATS JWTs**, which act as cryptographically secure database credentials for your edge clients.

A client never gets a password to connect to NATS. It gets a **`.creds` file** (in memory or on disk), which holds two things:
* a **user JWT** — public. It says "this public key belongs to `omar`, tenant `kilo`", and it is cryptographically signed by the Account's scoped signing key.
* a **user seed** — private. The client's own key. It never leaves the device.

**Step by step:**

1. **App Authentication (Your Backend):** Your web/mobile backend authenticates the user however you prefer (passwords, biometrics, Google OAuth).
2. **The Invite (Your Backend):** Once authenticated, your backend executes a query to authorize that user's device: `INSERT INTO zebridge_invites (code, tenant_id, expires_at)`. It hands this secure random `code` down to the client.
3. **The NKey (Edge Client):** The client app locally generates a cryptographic **NKey pair** (a public key and a private seed). *The private seed never leaves the device.*
4. **The Handshake (Edge Client to ZeBridge):** The client makes an HTTP request to the bridge's endpoint, providing the invite code and its *public* key:
   ```txt
   GET /enroll?code=<invite>&user_pubkey=U...
   ```
5. **The Minting (ZeBridge):** 
   * ZeBridge redeems the invite in PostgreSQL (stamps `used_at`) and permanently maps the user identity: `INSERT INTO zebridge_user_tenants (principal, tenant_id)`.
   * ZeBridge then acts as a **Delegated Signer**. Because you provided it with a NATS Scoped Signing Key via the `ZB_SIGNING_SEED` environment variable, it mints a NATS 2.0 JWT embedding the client's public key, restricts their subjects to their specific `tenant_id`, signs it, and returns `{"jwt":"..."}` to the client.
6. **The Credential Assembly (Edge Client):** The client app takes the JWT it received from the bridge and combines it with the private seed it already generated in step 3 to create the standard `.creds` file format. (Browser: memory/sessionStorage. Mobile: secure keychain).
7. **Connection to NATS:** The client connects to NATS presenting this `.creds` format. The NATS server sends a cryptographic challenge (a nonce). The client signs the nonce with its private seed. The NATS server verifies the signature, verifies the JWT was officially signed by the `ZB_SIGNING_SEED`, and grants access. **No secret ever crosses the wire.**
8. **Expiration:** Minted JWTs live 24 h (`enroll_jwt_ttl_seconds`). After that, the client quietly asks your backend for a new invite code and enrolls again.

The consumer boundary is one model for **every** consumer type — webapp, mobile, or microservice. The JWT and its verification are identical everywhere; only the transport (WebSocket for the browser, TLS-TCP for native) and the credential storage differ.

**Who holds what:**

| who | holds | can |
| --- | --- | --- |
| the client | its own seed + its JWT | be itself |
| ZeBridge | the account scoped signing seed (`ZB_SIGNING_SEED`) | mint client JWTs for others |
| the NATS server | the operator JWT + account public key (`ZB_ACCOUNT_PUB`) | trust what the bridge signed |"""

# The regex replaces everything from "### Client to NATS dance" up to (but not including) "\n## Requirements" or EOF.
text = re.sub(r'### Client to NATS dance\n.*?\| the NATS server \| the operator JWT \| trust what the account signed \|', replacement, text, flags=re.DOTALL)

with open("README.md", "w") as f:
    f.write(text)
