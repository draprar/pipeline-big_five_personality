from pathlib import Path
import os
import sys
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from scripts.load_to_sqlserver import build_db_url, load_local_env  # noqa: E402


def test_build_db_url_requires_config(monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    with pytest.raises(ValueError):
        build_db_url()


def test_build_db_url_from_db_url(monkeypatch):
    monkeypatch.setenv(
        "DB_URL", "mssql+pyodbc://host/Db?driver=ODBC+Driver+17+for+SQL+Server"
    )
    assert build_db_url().startswith("mssql+pyodbc://host/Db")


def test_build_db_url_from_components(monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "MYHOST\\SQLEXPRESS")
    monkeypatch.setenv("DB_NAME", "MyDb")
    monkeypatch.setenv("DB_TRUSTED", "yes")
    monkeypatch.setenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")
    url = build_db_url()
    assert url.startswith("mssql+pyodbc://MYHOST\\SQLEXPRESS/MyDb")


def test_load_local_env_does_not_override(monkeypatch, tmp_path: Path):
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "DB_NAME=FromFile\nDB_HOST=FromFile\\SQLEXPRESS\n", encoding="utf-8"
    )

    monkeypatch.setenv("DB_NAME", "FromEnv")
    load_local_env(env_file)

    assert os.getenv("DB_NAME") == "FromEnv"
    assert os.getenv("DB_HOST") == "FromFile\\SQLEXPRESS"
