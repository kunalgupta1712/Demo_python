import os
import uuid
from unittest.mock import patch, MagicMock
import db_operation_contact as db


# Helper to mock engine and connection context
def mock_engine_context():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    return mock_engine, mock_conn


def test_insert_new_contact():
    """Should insert new contact and call ERP if crmToErpFlag is True."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_contact.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch(
        "db_operation_contact.register_contact_as_erp", return_value="ERP_CONTACT_123"
    ), patch(
        "uuid.uuid4", return_value=uuid.UUID("12345678123456781234567812345678")
    ):

        mock_get_client.return_value = mock_engine

        # Simulate contact not existing
        mock_conn.execute.return_value.fetchone.return_value = None

        contacts = [
            {
                "contactId": "C1",
                "accountId": "A1",
                "accountName": "Company A",
                "firstName": "John",
                "lastName": "Doe",
                "email": "john@example.com",
                "crmToErpFlag": True,
                "department": "Sales",
                "country": "US",
                "cshmeFlag": True,
                "zipCode": "12345",
                "phoneNo": "555-1234",
                "status": "active",
            }
        ]

        result = db.insert_or_update_contact(contacts)

        assert result["inserted"] == 1
        assert result["updated"] == 0
        assert result["failed"] == []

        db.register_contact_as_erp.assert_called_once_with(
            "A1",
            "John",
            "Doe",
            "john@example.com",
            department="Sales",
            country="US",
            cshme_flag=True,
            phone_no="555-1234",
            status="active",
            contact_id="C1",
        )


def test_update_existing_contact():
    """Should update existing contact if data changed and call ERP."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_contact.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch(
        "db_operation_contact.register_contact_as_erp", return_value="ERP_CONTACT_456"
    ):

        mock_get_client.return_value = mock_engine

        # Simulate existing contact with different data
        mock_conn.execute.return_value.fetchone.return_value = MagicMock(
            _mapping={
                "accountName": "Old Company",
                "firstName": "Jane",
                "lastName": "Smith",
                "email": "jane@old.com",
                "crmToErpFlag": False,
                "erpContactPerson": None,
            }
        )

        contacts = [
            {
                "contactId": "C2",
                "accountId": "A2",
                "accountName": "New Co",
                "firstName": "Jane",
                "lastName": "Smith",
                "email": "jane@new.com",
                "crmToErpFlag": True,
                "department": "Marketing",
                "country": "US",
                "cshmeFlag": False,
                "zipCode": "54321",
                "phoneNo": "555-5678",
                "status": "active",
            }
        ]

        result = db.insert_or_update_contact(contacts)

        assert result["inserted"] == 0
        assert result["updated"] == 1
        assert result["failed"] == []

        db.register_contact_as_erp.assert_called_once()


def test_skip_contact_missing_fields():
    """Should skip contact if required fields are missing."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("db_operation_contact.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ):

        mock_get_client.return_value = mock_engine

        contacts = [{"accountId": "A3"}]  # Missing contactId

        result = db.insert_or_update_contact(contacts)

        assert result["inserted"] == 0
        assert result["updated"] == 0
        assert len(result["failed"]) == 1
        assert "Missing mandatory fields" in result["failed"][0]["error"]


def test_no_update_if_fields_same_but_erp_called():
    """Should not run UPDATE if fields are the same, but ERP call still happens."""
    mock_engine, mock_conn = mock_engine_context()

    existing_data = {
        "accountName": "Company B",
        "firstName": "Alice",
        "lastName": "Smith",
        "email": "alice@example.com",
        "crmToErpFlag": True,
        "erpContactPerson": "ERP789",
    }

    with patch("db_operation_contact.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch("db_operation_contact.register_contact_as_erp", return_value="ERP789"):

        mock_get_client.return_value = mock_engine
        mock_conn.execute.return_value.fetchone.return_value = MagicMock(
            _mapping=existing_data
        )

        contacts = [
            {
                "contactId": "C3",
                "accountId": "A3",
                "accountName": "Company B",
                "firstName": "Alice",
                "lastName": "Smith",
                "email": "alice@example.com",
                "crmToErpFlag": True,
                "department": "IT",
                "country": "DE",
                "cshmeFlag": False,
                "zipCode": "00000",
                "phoneNo": "555-9999",
                "status": "inactive",
            }
        ]

        result = db.insert_or_update_contact(contacts)

        assert result["updated"] == 1
        assert result["inserted"] == 0
        assert result["failed"] == []
        db.register_contact_as_erp.assert_called_once()


def test_handle_partial_failure():
    """Should continue processing even if one contact fails."""
    mock_engine, mock_conn = mock_engine_context()

    def failing_execute(query, params=None):
        if "C1" in str(params):
            raise Exception("Simulated DB failure")
        return MagicMock(fetchone=lambda: None)

    with patch("db_operation_contact.get_hana_client") as mock_get_client, patch.dict(
        os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}
    ), patch("db_operation_contact.register_contact_as_erp", return_value="ERP_OK"):

        mock_get_client.return_value = mock_engine
        mock_conn.execute.side_effect = failing_execute

        contacts = [
            {
                "contactId": "C1",
                "accountId": "A1",
                "accountName": "Bad Co",
                "crmToErpFlag": True,
            },
            {
                "contactId": "C2",
                "accountId": "A2",
                "accountName": "Good Co",
                "crmToErpFlag": True,
            },
        ]

        result = db.insert_or_update_contact(contacts)

        assert result["inserted"] + result["updated"] == 1
        assert len(result["failed"]) == 1
        assert result["failed"][0]["contact"]["contactId"] == "C1"
        assert "Simulated DB failure" in result["failed"][0]["error"]
