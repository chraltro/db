# Security Model

dp is a **single-machine data platform**. This document honestly describes what security exists, what's good, what's weak, and what the intended threat model is.

## Threat Model

dp is designed as a **local-first, single-user or small-team tool**. The primary deployment is:

- One machine (laptop, server, or VM)
- One DuckDB file (`warehouse.duckdb`)
- Optional web UI on localhost or LAN
- No cloud deployment, no multi-tenant access

**dp is NOT designed for:**

- Internet-facing deployments without a reverse proxy
- Multi-tenant SaaS scenarios
- Zero-trust network environments
- Regulatory compliance (SOC2, HIPAA, PCI) out of the box

## Authentication

### How it works

Auth is **opt-in** via `dp serve --auth`. Without `--auth`, the web UI has no authentication — anyone who can reach the port can read/write everything.

When enabled:

1. Users are stored in `_dp_internal.users` inside the DuckDB warehouse
2. Passwords are hashed with **PBKDF2-SHA256** (100,000 iterations, random 32-byte salt)
3. Login returns a **bearer token** (URL-safe random, 32 bytes)
4. Tokens expire after **30 days**
5. Tokens are stored in `_dp_internal.tokens`
6. Login endpoint has **rate limiting** (5 attempts per IP per 60 seconds)

### What's good

- PBKDF2 with high iteration count is solid for password hashing
- Random salts per user prevent rainbow table attacks
- Token generation uses `secrets.token_urlsafe` (cryptographically secure)
- Rate limiting on login prevents brute force
- Expired tokens are cleaned up opportunistically

### What's weak

- **Tokens are stored in plain text** in DuckDB — anyone with file access to `warehouse.duckdb` can extract them
- **No HTTPS** — dp serves plain HTTP. Tokens are transmitted in the clear. You must use a reverse proxy (nginx, Caddy) for TLS in production.
- **No session invalidation on password change** — changing a user's password does not revoke existing tokens
- **No CSRF protection** — the API uses bearer tokens, which mitigates CSRF for API calls, but the web UI loads from the same origin
- **Single-process token store** — rate limit state is in-memory and resets on restart
- **Rate limiting uses `request.client.host`** — behind a reverse proxy, all requests appear to come from the proxy's IP unless the proxy sets `X-Forwarded-For` and dp is configured to trust it (it currently isn't). This effectively makes the rate limit per-proxy, not per-user.

### Roles and Permissions

| Role | read | write | execute | manage_users | manage_secrets |
|------|------|-------|---------|--------------|----------------|
| admin | yes | yes | yes | yes | yes |
| editor | yes | yes | yes | no | no |
| viewer | yes | no | no | no | no |

- `read`: View tables, query data, browse files, see run history
- `write`: Edit SQL models, modify files, update config
- `execute`: Run transforms, execute ingest/export scripts, run streams
- `manage_users`: Create/delete users, change roles
- `manage_secrets`: View/modify .env secrets

Every API endpoint calls `_require_permission(request, "read"|"write"|"execute")` when auth is enabled.

## Script Execution

### The honest truth about `exec()`

Ingest and export scripts are **arbitrary Python** executed with `exec()`. There is **no sandboxing**. A script can:

- Read/write any file the dp process can access
- Make network requests
- Import any installed Python module
- Execute system commands
- Access environment variables (including secrets loaded from `.env`)

This is by design — dp scripts need full Python capabilities to connect to databases, call APIs, and transform data. The security boundary is:

**Only run scripts you trust.** Treat `ingest/` and `export/` like any other code in your project.

### Mitigations

- Scripts run with a **5-minute timeout** (configurable) to prevent hangs
- Scripts run in a **daemon thread** so the main process can recover from timeouts
- `stdout`/`stderr` are captured and logged (not displayed to end users by default)
- Script output is **masked** against `.env` secret values before logging
- Scripts prefixed with `_` are skipped (convention for disabled scripts)

### What could go wrong

- A malicious script in `ingest/` could exfiltrate data, install malware, or delete files
- The `db` connection passed to scripts has **full DuckDB access** — scripts can drop tables, modify metadata, or corrupt the warehouse
- Notebook cells (`.dpnb`) have the same execution privileges as Python scripts

## Secrets Management

### How secrets are stored

Secrets live in `.env` at the project root — a plain text file with `KEY="value"` entries. dp's `init` command adds `.env` to `.gitignore`.

```
# .env
POSTGRES_PASSWORD="my-secret-password"
STRIPE_API_KEY="sk_live_..."
```

Secrets are referenced in `project.yml` via `${VARIABLE_NAME}` syntax and resolved at runtime.

### What's good

- Secrets are **separated from config** — `project.yml` contains `${REFERENCES}`, not values
- `.env` is in `.gitignore` by default
- `list_secrets()` never returns values, only masked previews (`my****rd`)
- `mask_output()` scrubs secret values from script output before logging
- Secret params in connectors are automatically stored in `.env` (not `project.yml`)

### What's weak

- **`.env` is plain text** on disk — anyone with filesystem access can read it
- **Secrets are loaded into `os.environ`** — any Python code in the process can read them
- **No encryption at rest** — consider full-disk encryption if secrets are sensitive
- **No audit log** for secret access — you can't tell who read a secret
- **No secret rotation** support — changing a secret is a manual `.env` edit

## SQL Injection Protection

### Identifier validation

All user-provided identifiers (table names, schema names, column names) are validated against a strict regex:

```
^[A-Za-z_][A-Za-z0-9_]*$
```

This is enforced by `validate_identifier()` in:
- Connector script generation (`connector.py`)
- CDC engine (`cdc.py`)
- Transform model discovery (`transform/`)
- Notebook cell execution
- API endpoints that accept table/schema names

### Where SQL is safely constructed

- **Transform models**: SQL is read from `.sql` files verbatim — dp does not interpolate user input into model SQL
- **Seeds**: CSV paths are escaped with `'` → `''` before use in `read_csv_auto()`
- **Import wizard**: File paths are escaped, table names are validated
- **Auth queries**: All user-facing queries use parameterized `?` placeholders

### Where to be careful

- **Generated connector scripts** interpolate validated identifiers into SQL strings (not parameterized). The identifiers are validated before interpolation, but the pattern is inherently riskier than parameterized queries.
- **DuckDB `ATTACH`** commands embed connection strings directly — connection strings with special characters could theoretically cause issues, though DuckDB's parser is the last line of defense.

## Network Exposure

### `dp serve` (web UI)

- Default: **binds to 127.0.0.1:3000** (localhost only) — not accessible from other machines
- Use `--host 0.0.0.0` to expose to the network (required for remote access)
- Without `--auth`: **no authentication** — anyone who can reach the port can read, write, and execute
- CORS is configured to allow the dev server (`localhost:5173`) and the same origin

### Recommendations for production use

1. **Always use `--auth`** when serving to a network
2. **Use a reverse proxy** (nginx, Caddy, Traefik) for HTTPS
3. **Restrict network access** with firewall rules — dp should only be accessible to trusted users
4. **Don't expose dp to the internet** — it's not hardened for hostile traffic

### What the API exposes

When the web UI is running, these operations are available via HTTP:

| Endpoint | Risk | Auth required |
|----------|------|---------------|
| GET /api/tables | Read schema info | read |
| POST /api/query | Execute arbitrary SQL | execute |
| POST /api/stream/* | Run full pipelines | execute |
| POST /api/files/write | Write any project file | write |
| POST /api/connectors/setup | Set up external connections | write |
| GET /api/secrets | List secret keys (masked) | manage_secrets |

The `/api/query` endpoint is the most powerful — it executes arbitrary SQL, including DDL. The `execute` permission is required, but once granted, there are no further restrictions on what SQL can be run.

## DuckDB File Security

### What DuckDB provides

- **Single-writer** — only one process can write at a time (WAL mode)
- **No network access** — DuckDB is an embedded database with no listener
- **Read-only connections** available — external DB attachments use `READ_ONLY` flag

### What DuckDB does NOT provide

- **No row-level security** — any connection can read any table
- **No column-level encryption** — data is stored in plain DuckDB format
- **No access controls** — DuckDB has no built-in user/permission system
- **File permissions are your only protection** — use OS-level file permissions to restrict access to `warehouse.duckdb`

### Concurrency

- DuckDB supports **multiple readers** but only **one writer** at a time
- The web UI and CLI share the same `.duckdb` file
- Long-running transforms will block API writes
- There is no connection pooling — each operation opens and closes a connection

## Recommendations

### For personal/development use

dp's default security is fine. Run `dp serve` on localhost, don't share the port, and you're good.

### For team/LAN use

1. Enable auth: `dp serve --auth`
2. Create users with appropriate roles: `dp user create analyst --role viewer`
3. Put a reverse proxy in front for HTTPS
4. Set filesystem permissions on `warehouse.duckdb` and `.env`

### For anything more

dp is not the right tool for internet-facing, multi-tenant, or compliance-sensitive deployments. Consider:

- **Rill Data** or **Evidence** for published dashboards
- **dbt Cloud** or **Dagster Cloud** for managed pipelines
- A proper data warehouse (Snowflake, BigQuery, Redshift) for multi-user access control
