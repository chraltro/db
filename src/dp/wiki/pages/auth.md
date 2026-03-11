# Authentication and Authorization

dp includes built-in authentication with role-based access control (RBAC). When enabled, users must log in to access the web UI and API. Roles control what actions each user can perform.

## Enabling Authentication

Start the server with the `--auth` flag:

```bash
dp serve --auth
```

Without `--auth`, all endpoints are accessible without authentication (suitable for local development).

## Initial Setup

On first launch with `--auth`, no users exist. The web UI presents a setup screen to create the first admin user. Alternatively, use the API:

```bash
curl -X POST http://localhost:3000/api/auth/setup \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "your-password", "role": "admin"}'
```

This creates the first admin user and returns a token:

```json
{
  "token": "abc123...",
  "username": "admin",
  "role": "admin"
}
```

The setup endpoint only works when no users exist. After the first user is created, additional users must be created by an admin.

## Roles and Permissions

dp defines three roles with cumulative permissions:

| Role | Permissions | Description |
|------|------------|-------------|
| `viewer` | `read` | Browse tables, view DAG, run queries (read-only) |
| `editor` | `read`, `write`, `execute` | All viewer permissions plus run pipelines, edit files, manage data |
| `admin` | `read`, `write`, `execute`, `manage_users`, `manage_secrets` | Full access including user management and secrets |

### Permission Details

- **read** -- View tables, run SELECT queries, browse files, view DAG, view history
- **write** -- Edit files, save models, create versions, manage masking policies
- **execute** -- Run pipelines, execute scripts, trigger transforms, import data
- **manage_users** -- Create, update, delete users
- **manage_secrets** -- View, set, delete secrets in `.env`

## Authentication Flow

### Login

```bash
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "your-password"}'
```

Returns:

```json
{
  "token": "abc123...",
  "username": "admin"
}
```

### Using Tokens

Include the token in the `Authorization` header for all subsequent requests:

```bash
curl http://localhost:3000/api/tables \
  -H "Authorization: Bearer abc123..."
```

### Token Lifetime

Tokens expire after 30 days by default. After expiration, the user must log in again to get a new token.

### Check Auth Status

```bash
curl http://localhost:3000/api/auth/status
```

Returns whether auth is enabled and whether initial setup is needed:

```json
{
  "auth_enabled": true,
  "needs_setup": false
}
```

### Get Current User

```bash
curl http://localhost:3000/api/auth/me \
  -H "Authorization: Bearer abc123..."
```

Returns:

```json
{
  "username": "admin",
  "role": "admin",
  "display_name": "Admin User"
}
```

## User Management

All user management operations require the `admin` role.

### List Users

```bash
curl http://localhost:3000/api/users \
  -H "Authorization: Bearer <admin-token>"
```

Returns:

```json
[
  {
    "username": "admin",
    "role": "admin",
    "display_name": "Admin User",
    "created_at": "2025-01-15 06:00:00",
    "last_login": "2025-01-15 12:30:00"
  }
]
```

### Create User

```bash
curl -X POST http://localhost:3000/api/users \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "analyst",
    "password": "secure-password",
    "role": "viewer",
    "display_name": "Data Analyst"
  }'
```

Username constraints: alphanumeric, underscores, dots, and hyphens. Password minimum: 4 characters.

### Update User

```bash
curl -X PUT http://localhost:3000/api/users/analyst \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{"role": "editor", "display_name": "Senior Analyst"}'
```

You can update `role`, `password`, and `display_name` independently.

### Delete User

```bash
curl -X DELETE http://localhost:3000/api/users/analyst \
  -H "Authorization: Bearer <admin-token>"
```

Deleting a user also revokes all their active tokens.

## Password Security

Passwords are stored using PBKDF2 with SHA-256 and 100,000 iterations:

- A 32-byte random salt is generated per user
- The password is hashed with `PBKDF2(SHA256, password, salt, 100000)`
- Only the hash and salt are stored -- passwords are never stored in plaintext

## Rate Limiting

The login endpoint is rate-limited to prevent brute-force attacks. Each client IP is limited in its login attempt frequency.

## Secrets Management

Admin users can manage secrets (`.env` variables) through the API:

### List Secrets

```bash
curl http://localhost:3000/api/secrets \
  -H "Authorization: Bearer <admin-token>"
```

Returns keys with masked values (never exposes actual secret values in the API response).

### Set a Secret

```bash
curl -X POST http://localhost:3000/api/secrets \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{"key": "DB_PASSWORD", "value": "new-password"}'
```

### Delete a Secret

```bash
curl -X DELETE http://localhost:3000/api/secrets/DB_PASSWORD \
  -H "Authorization: Bearer <admin-token>"
```

## Data Storage

Auth data is stored in the DuckDB database under `_dp_internal`:

- `_dp_internal.users` -- User accounts (username, password hash, salt, role)
- `_dp_internal.tokens` -- Active authentication tokens with expiration

## Related Pages

- [Masking](masking) -- Data masking with role-based exemptions
- [Configuration](configuration) -- Project configuration
- [API Reference](api-reference) -- Full API endpoint reference
