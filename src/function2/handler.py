import os
import json
from db_operation_company import insert_or_update_company
from db_operation_contact import insert_or_update_contact


def main(event, context):
    base_dir = os.path.dirname(__file__)

    # File paths
    company_file_path = os.path.join(base_dir, "company_data.json")
    contact_file_path = os.path.join(base_dir, "contact_data.json")

    # --- Process Company Data ---
    with open(company_file_path, "r") as f:
        company_data = json.load(f)

    if company_data:
        result_company = insert_or_update_company(company_data)
        print(f"Company DB Operation Result: {result_company}")
    else:
        print("No company data found in company_data.json")

    # --- Process Contact Data ---
    with open(contact_file_path, "r") as f:
        contact_data = json.load(f)

    if contact_data:
        result_contact = insert_or_update_contact(contact_data)
        print(f"Contact DB Operation Result: {result_contact}")
    else:
        print("No contact data found in contact_data.json")

