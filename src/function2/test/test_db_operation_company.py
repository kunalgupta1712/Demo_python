import os
import uuid
from unittest.mock import patch, MagicMock
import db_operation_company as db


# Helper to mock engine and connection context
def mock_engine_context():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    return mock_engine, mock_conn


def test_insert_new_company():
    """Should insert a new company if it does not exist, and call ERP if flag is set."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_company.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch(
        "db_operation_company.register_company_as_customer", return_value="ERP123"
    ), patch(
        "uuid.uuid4", return_value=uuid.UUID("12345678123456781234567812345678")
    ):

        mock_get_client.return_value = mock_engine

        # Simulate no existing record
        mock_conn.execute.return_value.fetchone.return_value = None

        companies = [
            {
                "accountId": "A1",
                "accountName": "Acme Corp",
                "crmToErpFlag": True,
                "status": "active",
            }
        ]

        result = db.insert_or_update_company(companies)

        assert result["inserted"] == 1
        assert result["updated"] == 0
        assert result["failed"] == []

        # Should have called ERP registration
        db.register_company_as_customer.assert_called_once_with(
            "A1", "Acme Corp", "active"
        )


def test_update_existing_company():
    """Should update company if it exists and call ERP if flag is set."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_company.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch(
        "db_operation_company.register_company_as_customer", return_value="ERP456"
    ):

        mock_get_client.return_value = mock_engine

        # Simulate existing record
        mock_conn.execute.return_value.fetchone.return_value = (
            "Old Name",
            True,
            "ERP999",
            "inactive",
        )

        companies = [
            {
                "accountId": "A2",
                "accountName": "Updated Co",
                "crmToErpFlag": True,
                "status": "active",
            }
        ]

        result = db.insert_or_update_company(companies)

        assert result["inserted"] == 0
        assert result["updated"] == 1
        assert result["failed"] == []

        db.register_company_as_customer.assert_called_once_with(
            "A2", "Updated Co", "active"
        )


def test_skip_company_missing_fields():
    """Should skip company if required fields are missing."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_company.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ):

        mock_get_client.return_value = mock_engine

        companies = [{"accountName": "No ID Corp"}]  # Missing accountId

        result = db.insert_or_update_company(companies)

        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert len(result["failed"]) == 1
        assert "Missing mandatory fields" in result["failed"][0]["error"]


def test_handle_partial_failure_in_update():
    """Should continue processing even if one company fails."""
    mock_engine, mock_conn = mock_engine_context()

    def failing_execute(query, params=None):
        if "A1" in str(params):
            raise Exception("Simulated DB failure")
        return MagicMock(fetchone=lambda: None)

    with patch("db_operation_company.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch(
        "db_operation_company.register_company_as_customer", return_value="ERP789"
    ):

        mock_get_client.return_value = mock_engine

        mock_conn.execute.side_effect = failing_execute

        companies = [
            {
                "accountId": "A1",
                "accountName": "Failing Co",
                "crmToErpFlag": True,
                "status": "active",
            },
            {
                "accountId": "A2",
                "accountName": "Good Co",
                "crmToErpFlag": True,
                "status": "active",
            },
        ]

        result = db.insert_or_update_company(companies)

        assert result["inserted"] + result["updated"] == 1
        assert len(result["failed"]) == 1
        assert result["failed"][0]["company"]["accountId"] == "A1"
        assert "Simulated DB failure" in result["failed"][0]["error"]


def test_does_not_call_erp_when_flag_false():
    """Should not call ERP if crmToErpFlag is False."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_company.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch("db_operation_company.register_company_as_customer") as mock_register:

        mock_get_client.return_value = mock_engine
        mock_conn.execute.return_value.fetchone.return_value = None

        companies = [
            {
                "accountId": "A3",
                "accountName": "No ERP Co",
                "crmToErpFlag": False,
                "status": "pending",
            }
        ]

        result = db.insert_or_update_company(companies)

        assert result["inserted"] == 1
        assert result["updated"] == 0
        assert result["failed"] == []
        mock_register.assert_not_called()
