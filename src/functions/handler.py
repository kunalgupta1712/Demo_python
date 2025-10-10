import os
import json
import logging
from db_operation import insert_or_update_users_bulk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    """
    Kyma handler for API POST JSON payload (no CloudEvent support)
    """

    try:
        # event is expected to be a dict (from API tool)
        if isinstance(event, dict):
            payload = event.get("body") or event.get("data") or event
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": f"Unsupported event type: {type(event)}"})
            }

        # Parse JSON if string
        if isinstance(payload, str):
            payload = json.loads(payload)

        # Normalize to list
        if not isinstance(payload, list):
            payload = [payload]

        # Filter valid users
        valid_users = [u for u in payload if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in payload if not str(u.get("userID", "")).startswith("P")]

        if not valid_users:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid users to process"})
            }

        # Insert/update
        result = insert_or_update_users_bulk(valid_users)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Users processed successfully",
                "inserted": result.get("inserted", 0),
                "updated": result.get("updated", 0),
                "skipped": len(skipped_users)
            })
        }

    except Exception as e:
        logger.exception("Error processing request")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal Server Error", "error": str(e)})
        }
