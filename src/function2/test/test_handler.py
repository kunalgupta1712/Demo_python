import unittest
from unittest.mock import patch, mock_open
import handler



class TestHandler(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("handler.insert_or_update_company")
    @patch("handler.insert_or_update_contact")
    def test_full_successful_flow(
        self, mock_insert_contact, mock_insert_company,
        mock_json_load, mock_file
    ):
        # Setup
        mock_json_load.side_effect = [
            [
                {
                    "accountId": 1,
                    "accountName": "Acme Corp",
                    "crmToErpFlag": True,
                    "status": "active",
                }
            ],  # company data
            [
                {
                    "contactId": 1,
                    "accountId": 1,
                    "firstName": "John",
                    "lastName": "Doe",
                    "crmToErpFlag": True,
                    "email": "john@acme.com",
                }
            ],  # contact data
        ]
        mock_insert_company.return_value = {
            "inserted": 1, "updated": 0, "failed": []}
        mock_insert_contact.return_value = {
            "inserted": 1, "updated": 0, "failed": []}

        with patch("builtins.print") as mock_print:
            handler.main(event=None, context=None)

            # Assertions
            mock_insert_company.assert_called_once()
            mock_insert_contact.assert_called_once()
            mock_print.assert_any_call("Starting company data insertion...")
            mock_print.assert_any_call(
                "Company data insertion completed successfully.\n"
            )
            mock_print.assert_any_call("Starting contact data insertion...")
            mock_print.assert_any_call(
                "Contact data insertion completed successfully.")

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("handler.insert_or_update_company")
    def test_failed_company_inserts_should_skip_contacts(
        self, mock_insert_company, mock_json_load, mock_file
    ):
        mock_json_load.side_effect = [
            [
                {
                    "accountId": 1,
                    "accountName": "Bad Company",
                    "crmToErpFlag": True,
                    "status": "inactive",
                }
            ],  # company data
        ]
        mock_insert_company.return_value = {
            "inserted": 0,
            "updated": 0,
            "failed": [{"error": "DB error"}],
        }

        with patch("builtins.print") as mock_print:
            handler.main(event=None, context=None)

            # Company was called, contact should not be
            mock_insert_company.assert_called_once()
            mock_print.assert_any_call(
                "❌ Some company inserts/updates failed. "
                "Skipping contact data insertion."
            )

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    def test_no_company_data(self, mock_json_load, mock_file):
        mock_json_load.return_value = []  # Empty company data

        with patch("builtins.print") as mock_print:
            handler.main(event=None, context=None)
            mock_print.assert_any_call(
                "⚠️ No company data found in company_data.json\n")

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("handler.insert_or_update_company")
    def test_no_contact_data(
        self, mock_insert_company, mock_json_load, mock_file
    ):
        mock_json_load.side_effect = [
            [
                {
                    "accountId": 1,
                    "accountName": "Valid Company",
                    "crmToErpFlag": True,
                    "status": "active",
                }
            ],  # company data
            [],  # contact data
        ]
        mock_insert_company.return_value = {
            "inserted": 1, "updated": 0, "failed": []}

        with patch("builtins.print") as mock_print:
            handler.main(event=None, context=None)
            mock_print.assert_any_call(
                "⚠️ No contact data found in contact_data.json")

    @patch("builtins.open", side_effect=FileNotFoundError("File missing"))
    def test_missing_json_file(self, mock_open_file):
        with patch("builtins.print") as mock_print:
            with self.assertRaises(FileNotFoundError):
                handler.main(event=None, context=None)

            mock_open_file.assert_called()
            mock_print.assert_not_called()  # Error occurs before prints


if __name__ == "__main__":
    unittest.main()
