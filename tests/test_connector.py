"""Tests for the data connector framework."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import yaml

# Register all connectors
import dp.connectors  # noqa: F401
import pytest

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    get_connector,
    list_configured_connectors,
    list_connectors,
    register_connector,
    remove_connector,
    setup_connector,
    validate_identifier,
)


# ---------------------------------------------------------------------------
# Identifier validation tests
# ---------------------------------------------------------------------------


def test_validate_identifier_valid():
    """Valid SQL identifiers should pass."""
    assert validate_identifier("landing") == "landing"
    assert validate_identifier("my_table") == "my_table"
    assert validate_identifier("_private") == "_private"
    assert validate_identifier("Table123") == "Table123"


def test_validate_identifier_invalid():
    """SQL injection attempts should be rejected."""
    with pytest.raises(ValueError):
        validate_identifier("")
    with pytest.raises(ValueError):
        validate_identifier("bobby; DROP TABLE--")
    with pytest.raises(ValueError):
        validate_identifier("table name")
    with pytest.raises(ValueError):
        validate_identifier("123starts_with_digit")
    with pytest.raises(ValueError):
        validate_identifier("table'name")


def test_setup_rejects_bad_table_names(tmp_path):
    """setup_connector should sanitize table names with unsafe characters."""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "ingest").mkdir()
    (project_dir / "project.yml").write_text("name: test\ndatabase:\n  path: w.duckdb\nconnections: {}\n")
    (project_dir / ".env").write_text("")

    result = setup_connector(
        project_dir=project_dir,
        connector_type="webhook",
        connection_name="test",
        config={"table_name": "events"},
        tables=["valid_table", "bad;table--"],
        target_schema="landing",
    )
    # Tables get sanitized: "bad;table--" -> "bad_table"
    assert result["status"] == "success"
    assert "bad_table" in result["tables"]
    assert "valid_table" in result["tables"]


def test_setup_rejects_bad_schema(tmp_path):
    """setup_connector should reject schemas with injection attempts."""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "ingest").mkdir()
    (project_dir / "project.yml").write_text("name: test\ndatabase:\n  path: w.duckdb\nconnections: {}\n")
    (project_dir / ".env").write_text("")

    result = setup_connector(
        project_dir=project_dir,
        connector_type="webhook",
        connection_name="test",
        config={"table_name": "events"},
        target_schema="landing; DROP TABLE--",
    )
    assert result["status"] == "error"
    assert "Invalid" in result["error"]


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


def test_list_connectors():
    """All 10 built-in connectors should be registered."""
    available = list_connectors()
    names = {c["name"] for c in available}
    assert "postgres" in names
    assert "mysql" in names
    assert "rest_api" in names
    assert "google_sheets" in names
    assert "csv" in names
    assert "s3_gcs" in names
    assert "stripe" in names
    assert "hubspot" in names
    assert "shopify" in names
    assert "webhook" in names
    assert len(names) >= 10


def test_get_connector():
    """get_connector should return an instance of the right connector."""
    connector = get_connector("postgres")
    assert connector.name == "postgres"
    assert connector.display_name == "PostgreSQL"
    assert isinstance(connector.params, list)
    assert len(connector.params) > 0


def test_get_unknown_connector():
    """get_connector should raise ValueError for unknown types."""
    try:
        get_connector("nonexistent_db")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "nonexistent_db" in str(e)


def test_register_custom_connector():
    """Custom connectors can be registered with the decorator."""

    @register_connector
    class TestCustomConnector(BaseConnector):
        name = "_test_custom"
        display_name = "Test Custom"
        description = "A test connector"
        params = [ParamSpec("url", "Test URL")]

        def test_connection(self, config):
            return {"success": True}

        def discover(self, config):
            return [DiscoveredResource(name="test_table")]

        def generate_script(self, config, tables, target_schema="landing"):
            return '# custom script\nprint("hello")\n'

    c = get_connector("_test_custom")
    assert c.display_name == "Test Custom"
    assert c.test_connection({})["success"] is True
    assert len(c.discover({})) == 1
    script = c.generate_script({}, ["test_table"])
    assert "hello" in script


# ---------------------------------------------------------------------------
# Connector metadata tests
# ---------------------------------------------------------------------------


def test_connector_metadata():
    """Each connector should have name, display_name, description, params."""
    for info in list_connectors():
        assert info["name"], f"Missing name for connector"
        assert info["display_name"], f"Missing display_name for {info['name']}"
        assert info["description"], f"Missing description for {info['name']}"
        assert isinstance(info["params"], list)


def test_connector_params_have_specs():
    """Each connector's params should have name and description."""
    for info in list_connectors():
        for p in info["params"]:
            assert "name" in p, f"Param missing name in {info['name']}"
            assert "description" in p, f"Param missing description in {info['name']}"
            assert "required" in p
            assert "secret" in p


# ---------------------------------------------------------------------------
# CSV connector tests (can run without external services)
# ---------------------------------------------------------------------------


def test_csv_connector_test_local_file(tmp_path):
    """CSV connector should validate that a local file exists."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

    connector = get_connector("csv")
    result = connector.test_connection({"path": str(csv_file)})
    assert result["success"] is True


def test_csv_connector_test_missing_file():
    """CSV connector should fail for a nonexistent file."""
    connector = get_connector("csv")
    result = connector.test_connection({"path": "/nonexistent/file.csv"})
    assert result["success"] is False


def test_csv_connector_discover(tmp_path):
    """CSV connector should derive table name from filename."""
    connector = get_connector("csv")
    resources = connector.discover({"path": str(tmp_path / "my-data.csv")})
    assert len(resources) == 1
    assert resources[0].name == "my_data"


def test_csv_connector_generate_script(tmp_path):
    """CSV connector should generate a valid ingest script."""
    csv_file = tmp_path / "sales.csv"
    csv_file.write_text("id,amount\n1,100\n2,200\n")

    connector = get_connector("csv")
    script = connector.generate_script(
        {"path": str(csv_file)},
        ["sales"],
        "landing",
    )
    assert "landing" in script
    assert "sales" in script
    assert "read_csv" in script


def test_csv_connector_script_execution(tmp_path):
    """Generated CSV script should run and load data into DuckDB."""
    # Create test CSV
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n")

    # Generate script
    connector = get_connector("csv")
    script = connector.generate_script(
        {"path": str(csv_file)},
        ["test_data"],
        "landing",
    )

    # Write and run the script
    script_path = tmp_path / "ingest_csv.py"
    script_path.write_text(script)

    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    from dp.engine.runner import run_script

    result = run_script(conn, script_path, "ingest")
    assert result["status"] == "success"

    # Verify data was loaded
    rows = conn.execute("SELECT COUNT(*) FROM landing.test_data").fetchone()
    assert rows[0] == 3

    names = conn.execute(
        "SELECT name FROM landing.test_data ORDER BY id"
    ).fetchall()
    assert [r[0] for r in names] == ["Alice", "Bob", "Carol"]

    conn.close()


# ---------------------------------------------------------------------------
# REST API connector tests
# ---------------------------------------------------------------------------


def test_rest_api_connector_discover():
    """REST API connector should return configured table name."""
    connector = get_connector("rest_api")
    resources = connector.discover({"table_name": "my_api_data"})
    assert len(resources) == 1
    assert resources[0].name == "my_api_data"


def test_rest_api_connector_generate_script():
    """REST API connector should generate a script with the URL."""
    connector = get_connector("rest_api")
    script = connector.generate_script(
        {"url": "https://api.example.com/v1/data", "table_name": "api_data"},
        ["api_data"],
        "landing",
    )
    assert "https://api.example.com/v1/data" in script
    assert "landing" in script
    assert "api_data" in script


# ---------------------------------------------------------------------------
# Google Sheets connector tests
# ---------------------------------------------------------------------------


def test_google_sheets_connector_discover():
    """Google Sheets connector should derive table name from sheet name."""
    connector = get_connector("google_sheets")
    resources = connector.discover({"spreadsheet_id": "abc123", "sheet_name": "Sales Data"})
    assert len(resources) == 1
    assert resources[0].name == "sales_data"


def test_google_sheets_connector_generate_script():
    """Google Sheets connector should embed spreadsheet_id in script."""
    connector = get_connector("google_sheets")
    script = connector.generate_script(
        {"spreadsheet_id": "abc123", "sheet_name": "Sheet1"},
        ["sheet1"],
        "landing",
    )
    assert "abc123" in script
    assert "landing" in script


# ---------------------------------------------------------------------------
# Webhook connector tests
# ---------------------------------------------------------------------------


def test_webhook_connector_test():
    """Webhook connector should validate table_name is provided."""
    connector = get_connector("webhook")
    assert connector.test_connection({"table_name": "events"})["success"] is True
    assert connector.test_connection({})["success"] is False


def test_webhook_connector_generate_script():
    """Webhook connector should generate inbox processing script."""
    connector = get_connector("webhook")
    script = connector.generate_script(
        {"table_name": "events"},
        ["events"],
        "landing",
    )
    assert "events_inbox" in script
    assert "landing" in script


# ---------------------------------------------------------------------------
# Stripe connector tests
# ---------------------------------------------------------------------------


def test_stripe_connector_discover():
    """Stripe connector should discover configured resources."""
    connector = get_connector("stripe")
    resources = connector.discover({"resources": "charges,customers"})
    names = [r.name for r in resources]
    assert "stripe_charges" in names
    assert "stripe_customers" in names
    assert len(names) == 2


def test_stripe_connector_generate_script():
    """Stripe connector script should contain pagination logic."""
    connector = get_connector("stripe")
    script = connector.generate_script(
        {"api_key": "${STRIPE_API_KEY}", "resources": "charges"},
        ["stripe_charges"],
        "landing",
    )
    assert "stripe.com" in script
    assert "has_more" in script
    assert "STRIPE_API_KEY" in script


# ---------------------------------------------------------------------------
# HubSpot connector tests
# ---------------------------------------------------------------------------


def test_hubspot_connector_discover():
    """HubSpot connector should discover configured objects."""
    connector = get_connector("hubspot")
    resources = connector.discover({"objects": "contacts,deals"})
    names = [r.name for r in resources]
    assert "hubspot_contacts" in names
    assert "hubspot_deals" in names


# ---------------------------------------------------------------------------
# Shopify connector tests
# ---------------------------------------------------------------------------


def test_shopify_connector_discover():
    """Shopify connector should discover configured resources."""
    connector = get_connector("shopify")
    resources = connector.discover({"resources": "orders,products"})
    names = [r.name for r in resources]
    assert "shopify_orders" in names
    assert "shopify_products" in names


def test_shopify_connector_generate_script():
    """Shopify connector script should use correct store URL."""
    connector = get_connector("shopify")
    script = connector.generate_script(
        {"store": "my-store", "access_token": "${SHOPIFY_TOKEN}", "resources": "orders"},
        ["shopify_orders"],
        "landing",
    )
    assert "my-store" in script
    assert "myshopify.com" in script


# ---------------------------------------------------------------------------
# Postgres connector tests
# ---------------------------------------------------------------------------


def test_postgres_connector_generate_script():
    """Postgres connector should generate DuckDB postgres extension script."""
    connector = get_connector("postgres")
    script = connector.generate_script(
        {
            "host": "db.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "admin",
            "password": "${PG_PASS}",
            "schema": "public",
        },
        ["users", "orders"],
        "landing",
    )
    assert "INSTALL postgres" in script
    assert "db.example.com" in script
    assert "mydb" in script
    assert '"users"' in script
    assert '"orders"' in script


# ---------------------------------------------------------------------------
# MySQL connector tests
# ---------------------------------------------------------------------------


def test_mysql_connector_generate_script():
    """MySQL connector should generate DuckDB mysql extension script."""
    connector = get_connector("mysql")
    script = connector.generate_script(
        {
            "host": "mysql.example.com",
            "port": 3306,
            "database": "shop",
            "user": "root",
            "password": "${MYSQL_PASS}",
        },
        ["products", "categories"],
        "landing",
    )
    assert "INSTALL mysql" in script
    assert "mysql.example.com" in script
    assert '"products"' in script


# ---------------------------------------------------------------------------
# S3/GCS connector tests
# ---------------------------------------------------------------------------


def test_s3_connector_generate_script():
    """S3 connector should set up httpfs and credentials."""
    connector = get_connector("s3_gcs")
    script = connector.generate_script(
        {
            "path": "s3://my-bucket/data/*.parquet",
            "aws_access_key_id": "${AWS_KEY}",
            "aws_secret_access_key": "${AWS_SECRET}",
            "aws_region": "us-west-2",
        },
        ["data"],
        "landing",
    )
    assert "INSTALL httpfs" in script
    assert "s3://my-bucket" in script
    assert "us-west-2" in script
    assert "read_parquet" in script


# ---------------------------------------------------------------------------
# Full setup flow tests (project.yml integration)
# ---------------------------------------------------------------------------


def _scaffold_project(tmp_path: Path) -> Path:
    """Create a minimal project structure."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    (project_dir / "ingest").mkdir()
    (project_dir / "transform" / "bronze").mkdir(parents=True)
    (project_dir / "project.yml").write_text(
        "name: test\ndatabase:\n  path: warehouse.duckdb\nconnections: {}\nstreams:\n  full-refresh:\n    description: test\n    steps:\n      - ingest: [all]\n"
    )
    (project_dir / ".env").write_text("")
    return project_dir


def test_setup_connector_csv(tmp_path):
    """setup_connector should create script, update project.yml, set secrets."""
    project_dir = _scaffold_project(tmp_path)

    # Create a CSV file to connect
    csv_file = project_dir / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

    result = setup_connector(
        project_dir=project_dir,
        connector_type="csv",
        connection_name="test_csv",
        config={"path": str(csv_file)},
        target_schema="landing",
    )

    assert result["status"] == "success"
    assert result["connection_name"] == "test_csv"

    # Script was created
    script_path = project_dir / "ingest" / "connector_test_csv.py"
    assert script_path.exists()
    script_content = script_path.read_text()
    assert "read_csv" in script_content

    # project.yml was updated
    yml = yaml.safe_load((project_dir / "project.yml").read_text())
    assert "test_csv" in yml["connections"]
    assert yml["connections"]["test_csv"]["type"] == "csv"
    assert "sync-test_csv" in yml["streams"]


def test_setup_connector_webhook(tmp_path):
    """setup_connector should work for webhook (no external service needed)."""
    project_dir = _scaffold_project(tmp_path)

    result = setup_connector(
        project_dir=project_dir,
        connector_type="webhook",
        connection_name="my_webhook",
        config={"table_name": "events"},
        target_schema="landing",
    )

    assert result["status"] == "success"
    script_path = project_dir / "ingest" / "connector_my_webhook.py"
    assert script_path.exists()


def test_remove_connector(tmp_path):
    """remove_connector should delete script and config."""
    project_dir = _scaffold_project(tmp_path)

    # Set up first
    csv_file = project_dir / "data.csv"
    csv_file.write_text("id\n1\n")
    setup_connector(
        project_dir=project_dir,
        connector_type="csv",
        connection_name="removable",
        config={"path": str(csv_file)},
    )

    # Verify setup worked
    assert (project_dir / "ingest" / "connector_removable.py").exists()

    # Now remove
    result = remove_connector(project_dir, "removable")
    assert result["status"] == "success"
    assert not (project_dir / "ingest" / "connector_removable.py").exists()

    yml = yaml.safe_load((project_dir / "project.yml").read_text())
    assert "removable" not in yml.get("connections", {})


def test_list_configured_connectors(tmp_path):
    """list_configured_connectors should show configured connections."""
    project_dir = _scaffold_project(tmp_path)

    csv_file = project_dir / "data.csv"
    csv_file.write_text("id\n1\n")
    setup_connector(
        project_dir=project_dir,
        connector_type="csv",
        connection_name="my_csv",
        config={"path": str(csv_file)},
    )

    connectors = list_configured_connectors(project_dir)
    assert len(connectors) >= 1
    names = [c["name"] for c in connectors]
    assert "my_csv" in names
    csv_entry = next(c for c in connectors if c["name"] == "my_csv")
    assert csv_entry["type"] == "csv"
    assert csv_entry["has_script"] is True


def test_setup_connector_with_secrets(tmp_path):
    """setup_connector should store secret params in .env.

    Uses webhook connector (no external service needed) with a secret param.
    """
    project_dir = _scaffold_project(tmp_path)

    result = setup_connector(
        project_dir=project_dir,
        connector_type="webhook",
        connection_name="secure_hook",
        config={"table_name": "events", "secret": "my_secret_value"},
        target_schema="landing",
    )

    assert result["status"] == "success"

    # Check .env has the secret
    env_content = (project_dir / ".env").read_text()
    assert "SECURE_HOOK_SECRET" in env_content
    assert "my_secret_value" in env_content

    # project.yml should reference the env var, not the raw secret
    yml = yaml.safe_load((project_dir / "project.yml").read_text())
    hook_conn = yml["connections"]["secure_hook"]
    assert "${SECURE_HOOK_SECRET}" in str(hook_conn.get("secret", ""))


# ---------------------------------------------------------------------------
# Generated script execution test (end-to-end with DuckDB)
# ---------------------------------------------------------------------------


def test_full_csv_pipeline(tmp_path):
    """Full end-to-end: setup CSV connector, run script, verify data."""
    project_dir = _scaffold_project(tmp_path)

    # Create test data
    csv_file = project_dir / "customers.csv"
    csv_file.write_text(
        "customer_id,name,email,amount\n"
        "1,Alice,alice@example.com,150.00\n"
        "2,Bob,bob@example.com,200.50\n"
        "3,Carol,carol@example.com,75.25\n"
    )

    # Setup connector
    result = setup_connector(
        project_dir=project_dir,
        connector_type="csv",
        connection_name="customers",
        config={"path": str(csv_file)},
        tables=["customers"],
        target_schema="landing",
    )
    assert result["status"] == "success"

    # Run the generated ingest script
    script_path = project_dir / "ingest" / "connector_customers.py"
    db_path = project_dir / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))

    from dp.engine.runner import run_script

    run_result = run_script(conn, script_path, "ingest")
    assert run_result["status"] == "success"

    # Verify data
    count = conn.execute("SELECT COUNT(*) FROM landing.customers").fetchone()[0]
    assert count == 3

    total = conn.execute("SELECT SUM(amount) FROM landing.customers").fetchone()[0]
    assert abs(total - 425.75) < 0.01

    conn.close()
