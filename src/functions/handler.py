import json
from db_operation import insert_or_update_users_bulk

def main(event, context):
    # event should contain the JSON payload sent via API
    try:
        # If the event body is a string, parse it
        if isinstance(event, str):
            json_array = json.loads(event)
        elif isinstance(event, dict):
            # If the event is a dict, check if payload is under 'body'
            if "body" in event:
                json_array = json.loads(event["body"])
            else:
                json_array = event
        else:
            raise ValueError("Unsupported event format")
    except Exception as e:
        print(f"Error parsing JSON input: {e}")
        return {"status": "error", "message": "Invalid JSON input"}

    # Filter users whose userID starts with 'P'
    valid_users = [user for user in json_array if str(user.get("userID", "")).startswith("P")]

    # Optionally, log or print skipped users
    skipped_users = [user for user in json_array if not str(user.get("userID", "")).startswith("P")]
    if skipped_users:
        print(f"Skipped users (invalid userID): {[user.get('userID') for user in skipped_users]}")

    # Call the DB operation
    if valid_users:
        result = insert_or_update_users_bulk(valid_users)
        print(f"DB Operation Result: {result}")
        return {"status": "success", "processed": len(valid_users)}
    else:
        print("No valid users to process.")
        return {"status": "success", "processed": 0}
