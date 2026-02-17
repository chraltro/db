"""Tests for secrets management."""

from pathlib import Path

from dp.engine.secrets import (
    delete_secret,
    list_secrets,
    load_env,
    mask_output,
    set_secret,
    _mask,
)


def test_load_env_missing_file(tmp_path):
    """load_env returns empty when no .env file."""
    result = load_env(tmp_path)
    assert result == {}


def test_load_env_basic(tmp_path):
    """load_env reads key=value pairs."""
    env = tmp_path / ".env"
    env.write_text('DB_HOST=localhost\nDB_PASS="secret123"\n')
    result = load_env(tmp_path)
    assert result["DB_HOST"] == "localhost"
    assert result["DB_PASS"] == "secret123"


def test_set_and_list_secrets(tmp_path):
    """set_secret creates .env entries; list_secrets masks them."""
    set_secret(tmp_path, "API_KEY", "sk-abc123")
    set_secret(tmp_path, "DB_PASS", "mypassword")

    secrets = list_secrets(tmp_path)
    assert len(secrets) == 2
    assert secrets[0]["key"] == "API_KEY"
    assert secrets[0]["is_set"] is True
    # Value should be masked
    assert "abc" not in secrets[0]["masked_value"] or secrets[0]["masked_value"].count("*") > 0


def test_delete_secret(tmp_path):
    """delete_secret removes from .env."""
    set_secret(tmp_path, "KEY1", "val1")
    set_secret(tmp_path, "KEY2", "val2")
    assert delete_secret(tmp_path, "KEY1") is True
    assert delete_secret(tmp_path, "KEY1") is False
    secrets = list_secrets(tmp_path)
    assert len(secrets) == 1
    assert secrets[0]["key"] == "KEY2"


def test_mask_function():
    """_mask hides middle of values."""
    assert _mask("") == "(empty)"
    assert _mask("ab") == "**"
    assert _mask("abcdef") == "ab**ef"
    assert _mask("abcdefgh") == "ab****gh"


def test_mask_output(tmp_path):
    """mask_output replaces secret values in text."""
    set_secret(tmp_path, "PASSWORD", "supersecret")
    text = "Connected with password supersecret to database."
    masked = mask_output(text, tmp_path)
    assert "supersecret" not in masked
    assert "***" in masked
