import os
import json
import logging
from typing import Dict, Any, Union, List

# Custom modules
from db_operation import insert_or_update_users_bulk

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event: Dict[str, Any], context=None) -> Dict[str, Union[int, str]]:
    """
    Main handler to accept user payload and pass it to DB operation.
    """

    # -----------------------------
    # Step 1: Validate environment variables
    # -----------------------------
    required_env_vars = [
        "HANA_SERVER_NODE",
        "HANA_PORT",
        "HANA_USER",
        "HANA_PASSWORD",
        "HANA_SCHEMA",
    ]
    missing_env_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_env_vars:
        error_msg = f"Missing environment variables: {', '.join(missing_env_vars)}"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"message": error_msg})}

    # -----------------------------
    # Step 2: Extract payload
    # -----------------------------
    users = event.get("body") or event.get("data")
    if not users:
        logger.error("No payload found in event.")
        return {"statusCode": 400, "body": json.dumps({"message": "No payload provided"})}

    # -----------------------------
    # Step 3: Parse JSON string if needed
    # -----------------------------
    if isinstance(users, str):
        try:
            users = json.loads(users)
        except json.JSONDecodeError:
            logger.error("Invalid JSON payload")
            return {"statusCode": 400, "body": json.dumps({"message": "Invalid JSON payload"})}

    # -----------------------------
    # Step 4: Normalize to list
    # -----------------------------
    if isinstance(users, dict):
        users = [users]
    elif not isinstance(users, list):
        logger.error("Payload must be a dict or list of dicts")
        return {"statusCode": 400, "body": json.dumps({"message": "Invalid payload format"})}

    if not users:
        logger.warning("Empty user list received")
        return {"statusCode": 400, "body": json.dumps({"message": "Empty user list"})}

    # -----------------------------
    # Step 5: Pass payload to db_operation service
    # -----------------------------
    try:
        db_result = insert_or_update_users_bulk(users)

        msg = f"Processed users: {db_result['inserted']} inserted, {db_result['updated']} updated."
        logger.info(msg)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": msg,
                "inserted": db_result["inserted"],
                "updated": db_result["updated"]
            }),
        }

    except Exception as e:
        logger.exception("Error processing user data in DB")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error processing users: {str(e)}"}),
        }
