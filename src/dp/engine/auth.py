"""Authentication and user management.

Simple token-based auth with role-based permissions.
Users stored in DuckDB _dp_internal schema.
Roles: admin (full), editor (run + query), viewer (read-only).
"""

from __future__ import annotations

import hashlib
import os
import secrets
from pathlib import Path

import duckdb

from dp.engine.database import connect


def _hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    """Hash a password with PBKDF2. Returns (hash_hex, salt_hex)."""
    if salt is None:
        salt = os.urandom(32)
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return key.hex(), salt.hex()


def _verify_password(password: str, stored_hash: str, stored_salt: str) -> bool:
    """Verify password against stored hash."""
    key = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(stored_salt), 100_000
    )
    return key.hex() == stored_hash


def ensure_auth_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create auth tables if they don't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.users (
            username     VARCHAR PRIMARY KEY,
            password_hash VARCHAR NOT NULL,
            password_salt VARCHAR NOT NULL,
            role         VARCHAR NOT NULL DEFAULT 'viewer',
            display_name VARCHAR,
            created_at   TIMESTAMP DEFAULT current_timestamp,
            last_login   TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.tokens (
            token        VARCHAR PRIMARY KEY,
            username     VARCHAR NOT NULL,
            created_at   TIMESTAMP DEFAULT current_timestamp,
            expires_at   TIMESTAMP
        )
    """)


def create_user(
    conn: duckdb.DuckDBPyConnection,
    username: str,
    password: str,
    role: str = "viewer",
    display_name: str | None = None,
) -> dict:
    """Create a new user."""
    ensure_auth_tables(conn)
    if role not in ("admin", "editor", "viewer"):
        raise ValueError(f"Invalid role: {role}. Must be admin, editor, or viewer.")

    # Check if user exists
    existing = conn.execute(
        "SELECT username FROM _dp_internal.users WHERE username = ?", [username]
    ).fetchone()
    if existing:
        raise ValueError(f"User '{username}' already exists")

    pw_hash, pw_salt = _hash_password(password)
    conn.execute(
        """
        INSERT INTO _dp_internal.users (username, password_hash, password_salt, role, display_name)
        VALUES (?, ?, ?, ?, ?)
        """,
        [username, pw_hash, pw_salt, role, display_name or username],
    )
    return {"username": username, "role": role, "display_name": display_name or username}


def authenticate(conn: duckdb.DuckDBPyConnection, username: str, password: str) -> str | None:
    """Authenticate user, return token or None."""
    ensure_auth_tables(conn)
    row = conn.execute(
        "SELECT password_hash, password_salt FROM _dp_internal.users WHERE username = ?",
        [username],
    ).fetchone()
    if not row:
        return None
    if not _verify_password(password, row[0], row[1]):
        return None

    # Generate token
    token = secrets.token_urlsafe(32)
    conn.execute(
        "INSERT INTO _dp_internal.tokens (token, username) VALUES (?, ?)",
        [token, username],
    )
    conn.execute(
        "UPDATE _dp_internal.users SET last_login = current_timestamp WHERE username = ?",
        [username],
    )
    return token


def validate_token(conn: duckdb.DuckDBPyConnection, token: str) -> dict | None:
    """Validate a token and return user info, or None."""
    ensure_auth_tables(conn)
    row = conn.execute(
        """
        SELECT t.username, u.role, u.display_name
        FROM _dp_internal.tokens t
        JOIN _dp_internal.users u ON t.username = u.username
        WHERE t.token = ?
        """,
        [token],
    ).fetchone()
    if not row:
        return None
    return {"username": row[0], "role": row[1], "display_name": row[2]}


def list_users(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """List all users (no passwords)."""
    ensure_auth_tables(conn)
    rows = conn.execute(
        """
        SELECT username, role, display_name, created_at, last_login
        FROM _dp_internal.users
        ORDER BY created_at
        """
    ).fetchall()
    return [
        {
            "username": r[0],
            "role": r[1],
            "display_name": r[2],
            "created_at": str(r[3]) if r[3] else None,
            "last_login": str(r[4]) if r[4] else None,
        }
        for r in rows
    ]


def update_user(
    conn: duckdb.DuckDBPyConnection,
    username: str,
    role: str | None = None,
    password: str | None = None,
    display_name: str | None = None,
) -> bool:
    """Update user fields. Returns True if found."""
    ensure_auth_tables(conn)
    existing = conn.execute(
        "SELECT username FROM _dp_internal.users WHERE username = ?", [username]
    ).fetchone()
    if not existing:
        return False

    if role:
        if role not in ("admin", "editor", "viewer"):
            raise ValueError(f"Invalid role: {role}")
        conn.execute(
            "UPDATE _dp_internal.users SET role = ? WHERE username = ?",
            [role, username],
        )
    if password:
        pw_hash, pw_salt = _hash_password(password)
        conn.execute(
            "UPDATE _dp_internal.users SET password_hash = ?, password_salt = ? WHERE username = ?",
            [pw_hash, pw_salt, username],
        )
    if display_name:
        conn.execute(
            "UPDATE _dp_internal.users SET display_name = ? WHERE username = ?",
            [display_name, username],
        )
    return True


def delete_user(conn: duckdb.DuckDBPyConnection, username: str) -> bool:
    """Delete a user and their tokens."""
    ensure_auth_tables(conn)
    existing = conn.execute(
        "SELECT username FROM _dp_internal.users WHERE username = ?", [username]
    ).fetchone()
    if not existing:
        return False
    conn.execute("DELETE FROM _dp_internal.tokens WHERE username = ?", [username])
    conn.execute("DELETE FROM _dp_internal.users WHERE username = ?", [username])
    return True


def revoke_tokens(conn: duckdb.DuckDBPyConnection, username: str) -> int:
    """Revoke all tokens for a user."""
    ensure_auth_tables(conn)
    before = conn.execute(
        "SELECT COUNT(*) FROM _dp_internal.tokens WHERE username = ?", [username]
    ).fetchone()[0]
    conn.execute("DELETE FROM _dp_internal.tokens WHERE username = ?", [username])
    return before


def has_any_users(conn: duckdb.DuckDBPyConnection) -> bool:
    """Check if any users exist (for initial setup)."""
    ensure_auth_tables(conn)
    row = conn.execute("SELECT COUNT(*) FROM _dp_internal.users").fetchone()
    return row[0] > 0


ROLE_PERMISSIONS = {
    "admin": {"read", "write", "execute", "manage_users", "manage_secrets"},
    "editor": {"read", "write", "execute"},
    "viewer": {"read"},
}


def has_permission(role: str, permission: str) -> bool:
    """Check if a role has a specific permission."""
    return permission in ROLE_PERMISSIONS.get(role, set())
