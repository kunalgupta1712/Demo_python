import os
import unittest
from unittest.mock import patch, MagicMock
from id_generation import generate_sequential_id

# Mock environment variable for consistent schema in most tests
os.environ["HANA_SCHEMA"] = "TEST_SCHEMA"


def mock_engine_context():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    return mock_engine, mock_conn


class TestIdGeneration(unittest.TestCase):

    @patch("id_generation.get_hana_client")
    def test_generate_sequential_id_empty_table(self, mock_get_client):
        """Test when the table is empty, it should return the start range as the new ID."""
        mock_engine, mock_conn = mock_engine_context()
        mock_get_client.return_value = mock_engine

        # Simulate no data in the table (MAX returns None)
        mock_conn.execute.return_value.fetchone.return_value = (None,)

        start_range = 1000
        end_range = 9999
        id_type = "customerId"

        new_id = generate_sequential_id(id_type, start_range, end_range)

        self.assertEqual(new_id, str(start_range))

    @patch("id_generation.get_hana_client")
    def test_generate_sequential_id_existing_max(self, mock_get_client):
        """Test when the table has existing IDs, it should return max + 1."""
        mock_engine, mock_conn = mock_engine_context()
        mock_get_client.return_value = mock_engine

        # Simulate existing max ID = 1005
        mock_conn.execute.return_value.fetchone.return_value = (1005,)

        start_range = 1000
        end_range = 9999
        id_type = "customerId"

        new_id = generate_sequential_id(id_type, start_range, end_range)

        self.assertEqual(new_id, "1006")  # Should return 1006 (max + 1)

    @patch("id_generation.get_hana_client")
    def test_generate_sequential_id_exceed_range(self, mock_get_client):
        """Test when the next ID exceeds the range, it should raise a ValueError."""
        mock_engine, mock_conn = mock_engine_context()
        mock_get_client.return_value = mock_engine

        # Simulate existing max ID = 9999 (equal to end_range)
        mock_conn.execute.return_value.fetchone.return_value = (9999,)

        start_range = 1000
        end_range = 9999
        id_type = "customerId"

        with self.assertRaises(ValueError) as context:
            generate_sequential_id(id_type, start_range, end_range)

        self.assertEqual(
            str(context.exception),
            "customerId exceeded maximum range (9999)"
        )

    @patch("id_generation.get_hana_client")
    def test_generate_sequential_id_invalid_id_type(self, mock_get_client):
        """Test when an unsupported id_type is passed, it should raise a ValueError."""
        mock_engine, mock_conn = mock_engine_context()
        mock_get_client.return_value = mock_engine

        start_range = 1000
        end_range = 9999
        invalid_id_type = "invalidIdType"

        with self.assertRaises(ValueError) as context:
            generate_sequential_id(invalid_id_type, start_range, end_range)

        self.assertEqual(
            str(context.exception),
            "Unsupported id_type: invalidIdType"
        )

    @patch("id_generation.get_hana_client")
    def test_generate_sequential_id_missing_schema(self, mock_get_client):
        """Test when the HANA_SCHEMA environment variable is missing, it should raise a ValueError."""
        with patch.dict(os.environ, {}, clear=True):  # Clear env vars
            with self.assertRaises(ValueError) as context:
                generate_sequential_id("customerId", 1000, 9999)

            self.assertEqual(
                str(context.exception),
                "Environment variable HANA_SCHEMA is not set."
            )


if __name__ == "__main__":
    unittest.main()
