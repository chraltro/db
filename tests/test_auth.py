"""Tests for authentication and user management."""

from pathlib import Path

import duckdb

from dp.engine.auth import (
    authenticate,
    create_user,
    delete_user,
    ensure_auth_tables,
    has_any_users,
    has_permission,
    list_users,
    update_user,
    validate_token,
)


def _get_conn():
    conn = duckdb.connect(":memory:")
    ensure_auth_tables(conn)
    return conn


def test_create_and_authenticate():
    """Create a user and authenticate."""
    conn = _get_conn()
    create_user(conn, "alice", "password123", "admin")
    token = authenticate(conn, "alice", "password123")
    assert token is not None
    assert len(token) > 20

    # Wrong password
    bad = authenticate(conn, "alice", "wrong")
    assert bad is None

    # Non-existent user
    bad = authenticate(conn, "bob", "password123")
    assert bad is None
    conn.close()


def test_validate_token():
    """Tokens validate to user info."""
    conn = _get_conn()
    create_user(conn, "alice", "pass", "editor", "Alice Smith")
    token = authenticate(conn, "alice", "pass")
    user = validate_token(conn, token)
    assert user["username"] == "alice"
    assert user["role"] == "editor"
    assert user["display_name"] == "Alice Smith"

    # Invalid token
    assert validate_token(conn, "bogus") is None
    conn.close()


def test_list_and_delete_users():
    """List users and delete."""
    conn = _get_conn()
    create_user(conn, "alice", "pass1", "admin")
    create_user(conn, "bob", "pass2", "viewer")
    users = list_users(conn)
    assert len(users) == 2

    assert delete_user(conn, "bob") is True
    assert delete_user(conn, "bob") is False
    assert len(list_users(conn)) == 1
    conn.close()


def test_update_user():
    """Update user role and password."""
    conn = _get_conn()
    create_user(conn, "alice", "pass", "viewer")

    update_user(conn, "alice", role="admin")
    users = list_users(conn)
    assert users[0]["role"] == "admin"

    update_user(conn, "alice", password="newpass")
    token = authenticate(conn, "alice", "newpass")
    assert token is not None

    # Old password fails
    assert authenticate(conn, "alice", "pass") is None
    conn.close()


def test_has_any_users():
    """has_any_users returns False for empty, True after creation."""
    conn = _get_conn()
    assert has_any_users(conn) is False
    create_user(conn, "alice", "pass")
    assert has_any_users(conn) is True
    conn.close()


def test_permissions():
    """Role permissions work correctly."""
    assert has_permission("admin", "read") is True
    assert has_permission("admin", "manage_users") is True
    assert has_permission("editor", "execute") is True
    assert has_permission("editor", "manage_users") is False
    assert has_permission("viewer", "read") is True
    assert has_permission("viewer", "write") is False
    assert has_permission("viewer", "execute") is False
