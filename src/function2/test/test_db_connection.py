import os
import pytest
from unittest.mock import patch, MagicMock
from db_connection import get_hana_client
from sqlalchemy.exc import SQLAlchemyError


def test_get_hana_client_success(caplog):
    env_vars = {
        "HANA_SERVER_NODE": "hana.example.com",
        "HANA_PORT": "443",
        "HANA_USER": "test_user",
        "HANA_PASSWORD": "test_pass",
        "HANA_SCHEMA": "TEST_SCHEMA",
    }

    with patch.dict(os.environ, env_vars):
        with patch("db_connection.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            mock_conn = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn

            with caplog.at_level("INFO"):
                engine = get_hana_client()

            mock_create_engine.assert_called_once()
            mock_engine.connect.assert_called_once()
            assert engine == mock_engine
            assert "Initializing SAP HANA connection" in caplog.text
            assert "Successfully connected to SAP HANA" in caplog.text


@pytest.mark.parametrize(
    "env_vars",
    [
        {},  # all missing
        {"HANA_SERVER_NODE": "node_only"},  # partial
    ],
)
def test_get_hana_client_missing_env_vars(env_vars):
    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(ValueError) as exc_info:
            get_hana_client()
        # Match exact message from db_connection.py
        assert "Required HANA environment variables are missing" in str(
            exc_info.value)


def test_get_hana_client_sqlalchemy_error():
    env_vars = {
        "HANA_SERVER_NODE": "hana.example.com",
        "HANA_PORT": "443",
        "HANA_USER": "test_user",
        "HANA_PASSWORD": "test_pass",
        "HANA_SCHEMA": "TEST_SCHEMA",
    }

    with patch.dict(os.environ, env_vars):
        with patch(
            "db_connection.create_engine",
            side_effect=SQLAlchemyError("test error"),
        ):
            with pytest.raises(RuntimeError) as exc_info:
                get_hana_client()
            assert "HANA connection failed" in str(exc_info.value)
