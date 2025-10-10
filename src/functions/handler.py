import os
import json
import logging
from typing import Any, Dict, Union
from concurrent.futures import ThreadPoolExecutor

from db_operation import insert_or_update_users_bulk  # Your DB service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event: Any, context: Any = None) -> Dict[str, Union[int, str]]:
    """
    Kyma function handler for processing user data from API POST calls.
    Expects the payload in event['body'] as JSON (dict or list).
    """

    # Step 1: Validate environment variables
    required_env_vars = [
        "HANA_SERVER_NODE",
        "HANA_PORT",
        "HANA_USER",
        "HANA_PASSWORD",
        "HANA_SCHEMA",
    ]
    missing_env_vars = [v for v in required_env_vars if not os.environ.get(v)]
    if missing_env_vars:
        error_msg = f"Missing environment variables: {', '.join(missing_env_vars)}"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"message": error_msg})}

    # Step 2: Extract payload from event
    users: Union[Dict, list, None] = None
    if isinstance(event, dict):
        users = event.get("body") or event.get("data")
    else:
        logger.error(f"Unsupported event type: {type(event)}")
        return {"statusCode": 400, "body": json.dumps({"message": "Unsupported event type"})}

    # Step 3: Parse JSON if payload is string
    if isinstance(users, str):
        try:
            users = json.loads(users)
        except json.JSONDecodeError:
            logger.error("Invalid JSON in request body")
            return {"statusCode": 400, "body": json.dumps({"message": "Invalid JSON"})}

    # Step 4: Normalize to list
    if not isinstance(users, list):
        if users:
            logger.info("Received single user, converting to list")
            users = [users]
        else:
            users = []

    if not users:
        logger.error("No user records provided")
        return {"statusCode": 400, "body": json.dumps({"message": "No user records provided"})}

    # Step 5: Call db_operation to insert/update users
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(insert_or_update_users_bulk, users)
            result = future.result()

        success_msg = f"Processed {result['inserted']} inserts and {result['updated']} updates."
        logger.info(success_msg)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": success_msg,
                "inserted": result["inserted"],
                "updated": result["updated"]
            }),
        }

    except Exception as e:
        logger.exception("Error processing users")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error processing users", "error": str(e)}),
        }
