import os
import uuid
from datetime import datetime
from unittest.mock import patch, MagicMock
import erp_contactPerson_registration as erp_module


def mock_engine_context():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    return mock_engine, mock_conn


def test_register_new_erp_contact():
    """Should insert a new ERP contact when not found in DB."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("erp_contactPerson_registration.get_hana_client") as mock_get_client, \
         patch.dict(os.environ, {
             "HANA_SCHEMA": "TEST_SCHEMA",
             "ERP_CONTACTPERSONID_START": "2000000",
             "ERP_CONTACTPERSONID_END": "2999999",
         }), \
         patch("erp_contactPerson_registration.generate_sequential_id",
               return_value="CP1234567") as mock_id_gen, \
         patch("uuid.uuid4", return_value=uuid.UUID("12345678123456781234567812345678")), \
         patch("erp_contactPerson_registration.datetime") as mock_datetime:

        mock_get_client.return_value = mock_engine
        mock_datetime.utcnow.return_value = datetime(2025, 1, 1)

        # Simulate: Customer exists, Contact doesn't exist, then insert contact
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=("ERP_CUST_001",))),  # Customer lookup
            MagicMock(fetchone=MagicMock(return_value=None)),              # Contact lookup
            MagicMock(),                                                   # Insert contact
        ]

        contact_person_id = erp_module.register_contact_as_erp(
            account_id="CRM001",
            first_name="John",
            last_name="Doe",
            email="john.doe@example.com",
            department="IT",
            country="US",
            cshme_flag=True,
            phone_no="555-9999",
            status="active",
            contact_id="C123",
        )

        assert contact_person_id == "CP1234567"
        mock_id_gen.assert_called_once_with(
            id_type="contactPersonId", start_range=2000000, end_range=2999999
        )


def test_update_existing_erp_contact():
    """Should update ERP contact if it already exists."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("erp_contactPerson_registration.get_hana_client") as mock_get_client, \
         patch.dict(os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}), \
         patch("erp_contactPerson_registration.datetime") as mock_datetime:

        mock_get_client.return_value = mock_engine
        mock_datetime.utcnow.return_value = datetime(2025, 2, 2)

        # Simulate: Customer exists and contact exists, then update
        mock_conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=("ERP_CUST_002",))),  # Customer lookup
            MagicMock(fetchone=MagicMock(
                return_value=("CP_EXISTING", True, datetime(2024, 1, 1))
            )),  # Contact exists
            MagicMock(),  # Update contact
        ]

        contact_person_id = erp_module.register_contact_as_erp(
            account_id="CRM002",
            first_name="Jane",
            last_name="Smith",
            email="jane@example.com",
            department="Sales",
            country="UK",
            cshme_flag=False,
            phone_no="555-8888",
            status="inactive",
            contact_id="C999",
        )

        assert contact_person_id == "CP_EXISTING"  # Should return existing ID


def test_contact_skipped_if_no_customer():
    """Should return None if no ERP customer matches the CRM account."""
    mock_engine, mock_conn = mock_engine_context()

    with patch("erp_contactPerson_registration.get_hana_client") as mock_get_client, \
         patch.dict(os.environ, {"HANA_SCHEMA": "TEST_SCHEMA"}):

        mock_get_client.return_value = mock_engine
        mock_conn.execute.return_value.fetchone.return_value = None  # No customer found

        result = erp_module.register_contact_as_erp(
            account_id="INVALID_CRM",
            first_name="Ghost",
            last_name="User",
            email="ghost@example.com",
        )

        assert result is None


def test_missing_schema_raises_value_error():
    """Should raise error if HANA_SCHEMA is not set."""
    with patch.dict(os.environ, {}, clear=True):
        try:
            erp_module.register_contact_as_erp(
                account_id="ANY", first_name="A", last_name="B", email="test@test.com"
            )
        except ValueError as e:
            assert "HANA_SCHEMA is not set" in str(e)
        else:
            assert False, "Expected ValueError not raised"
