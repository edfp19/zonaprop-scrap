from urllib.parse import unquote, urlsplit

import pytest
from pydantic import SecretStr

from zona_oportunidades.shared.config import Settings


@pytest.mark.parametrize("padding", ["", "\n", "\r\n", " \n"])
def test_cloud_url_preserves_tls_and_is_hidden_from_settings_repr(monkeypatch, padding) -> None:
    url = "postgresql://reader:private-password@example.test/db?sslmode=require"
    monkeypatch.setenv("DATABASE_URL", padding + url + padding)
    settings = Settings(_env_file=None)
    assert settings.postgres_dsn == url
    assert "private-password" not in repr(settings)
    assert isinstance(settings.database_url, SecretStr)


def test_local_connection_encodes_password_and_database_name(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    settings = Settings(
        _env_file=None,
        postgres_user="user@name",
        postgres_password="p@ss:/?#%",
        postgres_db="teaching project",
    )
    parsed = urlsplit(settings.postgres_dsn)
    assert unquote(parsed.username) == "user@name"
    assert unquote(parsed.password) == "p@ss:/?#%"
    assert unquote(parsed.path) == "/teaching project"
