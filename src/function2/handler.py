import os
import json
from db_operation_company import insert_or_update_company
from db_operation_contact import insert_or_update_contact


def main(event, context):
    base_dir = os.path.dirname(__file__)

    # File paths
    company_file_path = os.path.join(base_dir, "company_data.json")
    contact_file_path = os.path.join(base_dir, "contact_data.json")

    # --- Step 1: Process Company Data ---
    with open(company_file_path, "r") as f:
        company_data = json.load(f)

    if company_data:
        print("Starting company data insertion...")
        result_company = insert_or_update_company(company_data)
        print(f"✅ Company DB Operation Result: {result_company}")
        print("Company data insertion completed successfully.\n")

        # Check for failed company inserts
        if result_company.get("failed"):
            print(
                "❌ Some company inserts/updates failed. "
                "Skipping contact data insertion."
            )
            return
    else:
        print("⚠️ No company data found in company_data.json\n")
        return  # skip contact insertion if no company data

    # --- Step 2: Process Contact Data ---
    with open(contact_file_path, "r") as f:
        contact_data = json.load(f)

    if contact_data:
        print("Starting contact data insertion...")
        result_contact = insert_or_update_contact(contact_data)
        print(f"✅ Contact DB Operation Result: {result_contact}")
        print("Contact data insertion completed successfully.")
    else:
        print("⚠️ No contact data found in contact_data.json")
