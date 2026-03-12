"""Tests for column-level data masking."""

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Unit tests for masking functions
# ---------------------------------------------------------------------------


def test_mask_hash():
    from havn.engine.masking import mask_hash

    result = mask_hash("hello@example.com")
    assert isinstance(result, str)
    assert len(result) == 8
    # Deterministic
    assert mask_hash("hello@example.com") == result
    assert mask_hash(None) is None


def test_mask_redact():
    from havn.engine.masking import mask_redact

    assert mask_redact("sensitive") == "***"
    assert mask_redact(12345) == "***"
    assert mask_redact(None) is None


def test_mask_null():
    from havn.engine.masking import mask_null

    assert mask_null("anything") is None
    assert mask_null(42) is None
    assert mask_null(None) is None


def test_mask_partial():
    from havn.engine.masking import mask_partial

    # Show first 2, last 3
    assert mask_partial("hello@example.com", show_first=2, show_last=3) == "he************com"
    # Show first only
    assert mask_partial("secret", show_first=2, show_last=0) == "se****"
    # Show last only
    assert mask_partial("secret", show_first=0, show_last=2) == "****et"
    # Short string where show_first + show_last >= len
    assert mask_partial("ab", show_first=1, show_last=1) == "ab"
    assert mask_partial(None) is None


def test_apply_mask():
    from havn.engine.masking import apply_mask

    assert apply_mask("test", "redact") == "***"
    assert apply_mask("test", "null") is None
    assert apply_mask("test", "hash") == apply_mask("test", "hash")
    assert len(apply_mask("test", "hash")) == 8
    assert apply_mask("hello", "partial", {"show_first": 1, "show_last": 1}) == "h***o"
    # Unknown method passes through
    assert apply_mask("test", "unknown") == "test"


# ---------------------------------------------------------------------------
# Integration tests with DuckDB
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path):
    """Create an in-memory DuckDB with test data and masking table."""
    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    db.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
    db.execute("CREATE SCHEMA IF NOT EXISTS gold")
    db.execute("""
        CREATE TABLE gold.customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            status VARCHAR
        )
    """)
    db.execute("""
        INSERT INTO gold.customers VALUES
        (1, 'Alice', 'alice@example.com', 'active'),
        (2, 'Bob', 'bob@example.com', 'inactive'),
        (3, 'Charlie', 'charlie@example.com', 'active')
    """)
    from havn.engine.masking import ensure_masking_table

    ensure_masking_table(db)
    yield db
    db.close()


def test_create_and_list_policies(conn):
    from havn.engine.masking import create_policy, list_policies

    policy = create_policy(
        conn,
        schema_name="gold",
        table_name="customers",
        column_name="email",
        method="redact",
    )
    assert policy["id"]
    assert policy["method"] == "redact"
    assert policy["schema_name"] == "gold"
    assert policy["exempted_roles"] == ["admin"]

    policies = list_policies(conn)
    assert len(policies) == 1
    assert policies[0]["column_name"] == "email"


def test_get_and_update_policy(conn):
    from havn.engine.masking import create_policy, get_policy, update_policy

    policy = create_policy(
        conn,
        schema_name="gold",
        table_name="customers",
        column_name="email",
        method="redact",
    )

    fetched = get_policy(conn, policy["id"])
    assert fetched["method"] == "redact"

    updated = update_policy(conn, policy["id"], method="hash")
    assert updated["method"] == "hash"


def test_delete_policy(conn):
    from havn.engine.masking import create_policy, delete_policy, list_policies

    policy = create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
    )
    assert delete_policy(conn, policy["id"]) is True
    assert list_policies(conn) == []
    assert delete_policy(conn, "nonexistent") is False


def test_invalid_method(conn):
    from havn.engine.masking import create_policy

    with pytest.raises(ValueError, match="Unknown masking method"):
        create_policy(
            conn, schema_name="gold", table_name="customers",
            column_name="email", method="encrypt",
        )


def test_apply_masking_basic(conn):
    from havn.engine.masking import apply_masking, create_policy

    create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
    )

    columns = ["id", "name", "email"]
    rows = [
        [1, "Alice", "alice@example.com"],
        [2, "Bob", "bob@example.com"],
    ]

    # Viewer should see masked data
    result = apply_masking(columns, rows, "viewer", conn, schema="gold", table="customers")
    assert result[0][2] == "***"
    assert result[1][2] == "***"
    # Other columns untouched
    assert result[0][1] == "Alice"


def test_apply_masking_admin_exempt(conn):
    from havn.engine.masking import apply_masking, create_policy

    create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
    )

    columns = ["id", "name", "email"]
    rows = [[1, "Alice", "alice@example.com"]]

    # Admin should see unmasked data
    result = apply_masking(columns, rows, "admin", conn, schema="gold", table="customers")
    assert result[0][2] == "alice@example.com"


def test_apply_masking_conditional(conn):
    from havn.engine.masking import apply_masking, create_policy

    # Only mask email when status == 'inactive'
    create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
        condition_column="status", condition_value="inactive",
    )

    columns = ["id", "name", "email", "status"]
    rows = [
        [1, "Alice", "alice@example.com", "active"],
        [2, "Bob", "bob@example.com", "inactive"],
    ]

    result = apply_masking(columns, rows, "viewer", conn, schema="gold", table="customers")
    # Active user — not masked
    assert result[0][2] == "alice@example.com"
    # Inactive user — masked
    assert result[1][2] == "***"


def test_apply_masking_adhoc_query(conn):
    """Ad-hoc queries (no schema/table) match on column name alone."""
    from havn.engine.masking import apply_masking, create_policy

    create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
    )

    columns = ["email", "cnt"]
    rows = [["alice@example.com", 5]]

    result = apply_masking(columns, rows, "viewer", conn)
    assert result[0][0] == "***"


def test_apply_masking_no_policies(conn):
    """No policies means rows pass through unchanged."""
    from havn.engine.masking import apply_masking

    columns = ["email"]
    rows = [["alice@example.com"]]
    result = apply_masking(columns, rows, "viewer", conn)
    assert result[0][0] == "alice@example.com"


def test_apply_masking_schema_mismatch(conn):
    """Exact schema/table matching should not mask wrong table."""
    from havn.engine.masking import apply_masking, create_policy

    create_policy(
        conn, schema_name="gold", table_name="customers",
        column_name="email", method="redact",
    )

    columns = ["email"]
    rows = [["alice@example.com"]]

    # Different table — should NOT mask
    result = apply_masking(columns, rows, "viewer", conn, schema="silver", table="orders")
    assert result[0][0] == "alice@example.com"


# ---------------------------------------------------------------------------
# API tests
# ---------------------------------------------------------------------------


@pytest.fixture
def project(tmp_path):
    """Create a minimal test project with data for masking tests."""
    (tmp_path / "project.yml").write_text("""
name: test
database:
  path: warehouse.duckdb
streams:
  test-stream:
    description: "Test"
    steps:
      - transform: [all]
""")
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "ingest").mkdir()
    (tmp_path / "export").mkdir()

    db = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    db.execute("CREATE SCHEMA IF NOT EXISTS gold")
    db.execute("""
        CREATE TABLE gold.customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
    """)
    db.execute("INSERT INTO gold.customers VALUES (1, 'Alice', 'alice@example.com')")
    db.execute("INSERT INTO gold.customers VALUES (2, 'Bob', 'bob@example.com')")
    from havn.engine.database import ensure_meta_table

    ensure_meta_table(db)
    db.close()
    return tmp_path


@pytest.fixture
def client(project):
    import havn.server.app as server_app

    server_app.PROJECT_DIR = project
    return TestClient(server_app.app)


def test_api_create_policy(client):
    resp = client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["method"] == "redact"
    assert "id" in data


def test_api_list_policies(client):
    client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "hash",
    })
    resp = client.get("/api/masking/policies")
    assert resp.status_code == 200
    policies = resp.json()
    assert len(policies) >= 1


def test_api_get_policy(client):
    create = client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })
    pid = create.json()["id"]

    resp = client.get(f"/api/masking/policies/{pid}")
    assert resp.status_code == 200
    assert resp.json()["id"] == pid


def test_api_update_policy(client):
    create = client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })
    pid = create.json()["id"]

    resp = client.put(f"/api/masking/policies/{pid}", json={"method": "hash"})
    assert resp.status_code == 200
    assert resp.json()["method"] == "hash"


def test_api_delete_policy(client):
    create = client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })
    pid = create.json()["id"]

    resp = client.delete(f"/api/masking/policies/{pid}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "deleted"

    # Should be gone
    resp = client.get(f"/api/masking/policies/{pid}")
    assert resp.status_code == 404


def test_api_delete_nonexistent(client):
    resp = client.delete("/api/masking/policies/nonexistent-id")
    assert resp.status_code == 404


def test_api_invalid_method(client):
    resp = client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "encrypt",
    })
    assert resp.status_code == 422  # Pydantic validation


# ---------------------------------------------------------------------------
# End-to-end: query masking via API
# ---------------------------------------------------------------------------


def test_query_masking_e2e(client):
    """Create a policy, then query — viewer sees masked data."""
    # Create a redact policy on email
    client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })

    # Query (auth disabled → user is admin by default, so exempted)
    resp = client.post("/api/query", json={"sql": "SELECT * FROM gold.customers ORDER BY id"})
    assert resp.status_code == 200
    data = resp.json()
    # Default local user is admin — should see unmasked
    assert data["rows"][0][2] == "alice@example.com"


def test_sample_masking_e2e(client):
    """Sample endpoint applies masking based on schema/table."""
    client.post("/api/masking/policies", json={
        "schema_name": "gold",
        "table_name": "customers",
        "column_name": "email",
        "method": "redact",
    })

    # Default user is admin — exempted
    resp = client.get("/api/tables/gold/customers/sample")
    assert resp.status_code == 200
    data = resp.json()
    # Admin sees unmasked
    emails = [row[2] for row in data["rows"]]
    assert all("@" in e for e in emails)
