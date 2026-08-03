# Account Sign-In Operations

Haze account mode protects the operator panel with PASETO v4.local sessions, stateful Redis leases, HMAC-peppered Argon2id password hashes, TOTP MFA, IP-bound sessions, CIDR policies, and tamper-evident audit logs.

## Required services and secrets

Account mode fails closed unless every configured key and Redis are available. Install Redis on Debian or Ubuntu, bind it to loopback, enable protected mode, and use an ACL or password when other local users are not fully trusted.

```bash
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server
redis-cli ping
```

Generate every key independently. Do not reuse a value between variables.

```bash
openssl rand -base64 32
openssl rand -base64 32
openssl rand -base64 32
openssl rand -base64 32
```

Place the results in the private runtime `.env` file:

```dotenv
HAZE_PASETO_V4_LOCAL_KEY=<first independent key>
HAZE_PASSWORD_PEPPER=<second independent key>
HAZE_MFA_ENCRYPTION_KEY=<third independent key>
HAZE_AUDIT_HMAC_KEY=<fourth independent key>
HAZE_REDIS_URL=redis://127.0.0.1:6379/0
HAZE_BOOTSTRAP_ADMIN_USERNAME=admin
HAZE_BOOTSTRAP_ADMIN_PASSWORD=<one-time strong bootstrap password>
```

Protect the file and remove the bootstrap password after the first administrator record has been created.

```bash
chmod 600 .env
```

The password pepper and MFA encryption key are recovery-critical. Losing the pepper makes every stored password unverifiable. Losing the MFA key makes enrolled TOTP seeds unreadable. Back up all four keys separately from the account database, with access controls at least as strict as the database backup.

## TLS and proxy trust

Account sign-in and authenticated requests require HTTPS. Account cookies are always `Secure`, `HttpOnly`, and `SameSite=Strict`. Either enable the built-in TLS listener or terminate TLS at a reverse proxy whose address is listed in `webpanel.authentication.trusted_proxy_cidrs`.

Haze accepts `X-Forwarded-Proto: https` only when the direct peer is a configured trusted proxy. Never add a broad client network to `trusted_proxy_cidrs`. Add only the exact proxy host or proxy subnet.

The admin WebSocket is same-origin only. Public audio and receiver WebSockets retain their separate route policies.

## Database and session state

SQLite deployments store accounts in the configured Haze database, normally `runtime/state/haze.db`. PostgreSQL deployments use the configured Haze DSN. Redis stores active session leases, their account index, and short-lived atomic sliding-window entries for sign-in and alert-origination limits. Sign-in slots are reserved atomically before password verification so concurrent guesses cannot bypass the window. Missing leases, Redis errors, credential-version mismatches, or IP changes reject the session.

TOTP seeds are encrypted with XChaCha20-Poly1305 and bound to the owning account ID as authenticated data. Copying an encrypted seed to another account does not produce a usable enrollment.

Saving an account policy advances that account's authentication version and revokes its active sessions. This makes role, CIDR, persistent-session, password-policy, and origination changes effective without a stale-session window.

The default policies are:

- 12-hour maximum regular session
- 15-minute idle timeout for browser-close sessions
- optional 30-day persistent session only when the account permits it
- five sign-in attempts per username and IP in 15 minutes, with account lock after five failed password or MFA checks
- two alert generation or origination requests per second per account
- 90-day password expiry for newly created accounts
- optional TOTP enrollment when `enforce_mfa` is disabled, with password-only sign-in allowed for accounts that have not completed enrollment
- mandatory TOTP enrollment when `enforce_mfa` is enabled, while accounts with active MFA continue to require TOTP regardless of the global setting

Keep system time synchronized because TOTP validation uses 30-second time steps with replay prevention.

```bash
timedatectl status
sudo timedatectl set-ntp true
```

## Sole-administrator recovery

If the only administrator is locked, stop the service and run the local recovery command with the same private environment and configuration used by the service. The command unlocks the account, resets its failure window, advances its authentication version, revokes every session, and writes requested and completed audit events.

```bash
cd /srv/haze-weather-radio
sudo systemctl stop haze-weather-radio.service
sudo -u rai ./bin/haze-web --config ./config.yaml --unlock-account admin
sudo systemctl start haze-weather-radio.service
```

Do not expose this command through the web panel or a remote shell wrapper. Filesystem access to the runtime and its secret environment is the recovery authority.

## Audit integrity

Audit files are written under `logs/access`, `logs/alerts`, and `logs/webpanel`. Every account log is an HMAC-SHA256 chain. Each category also has an HMAC-signed `integrity.sig` checkpoint. Haze verifies existing chains and checkpoints at startup and before append, then fails closed when it detects a mismatch.

These logs are tamper-evident, not magically tamper-proof against a host administrator who can delete the logs, checkpoints, application, and keys together. Forward audit events or periodic signed checkpoints to a separate append-only or WORM system when deletion resistance is required.

Do not expose `.env`, `runtime/state`, Redis, or unrestricted audit directories through Samba. If operators need audit access from Windows, publish a read-only, separately authorized export or a redacted panel view.

## Cryptographic note

PASETO v4.local does not negotiate algorithms. The standard v4.local construction uses XChaCha20-based encryption plus a keyed BLAKE2b authentication tag. It is not ChaCha20-Poly1305. Haze uses the standard `aidanwoods.dev/go-paseto` v4.local implementation rather than implementing the construction itself.
