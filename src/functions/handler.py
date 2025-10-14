import os
import json
from db_operation import insert_or_update_users_bulk

def main(event, context):
    json_file_path = os.path.join(os.path.dirname(__file__), 'data.json')
    with open(json_file_path, 'r') as f:
        json_array = json.load(f)

    # Filter users whose userID starts with 'P'
    valid_users = [user for user in json_array if str(user.get("userId", "")).startswith("P")]

    # Optionally, log or print skipped users
    skipped_users = [user for user in json_array if not str(user.get("userId", "")).startswith("P")]
    if skipped_users:
        print(f"Skipped users (invalid userID): {[user.get('userId') for user in skipped_users]}")

    # Call the DB operation
    if valid_users:
        result = insert_or_update_users_bulk(valid_users)
        print(f"DB Operation Result: {result}")
    else:
        print("No valid users to process.")
