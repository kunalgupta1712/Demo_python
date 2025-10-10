import os
import json
import logging
from db_operation import insert_or_update_users_bulk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    """
    Handler for SAP Kyma serverless Python function with CloudEvent.
    """

    try:
        # Extract payload from lib.ce.Event.data attribute
        payload = event.data

        # Log raw payload
        logger.info(f"Received payload from event.data: {payload}")

        # Parse payload if string
        if isinstance(payload, str):
            payload = json.loads(payload)
            logger.info(f"Parsed payload to JSON: {payload}")

        # Normalize to list
        if not isinstance(payload, list):
            payload = [payload]

        valid_users = [u for u in payload if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in payload if not str(u.get("userID", "")).startswith("P")]

        logger.info(f"Valid users: {valid_users}")
        logger.info(f"Skipped users: {skipped_users}")

        if not valid_users:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid users to process"})
            }

        # Insert or update the valid users
        result = insert_or_update_users_bulk(valid_users)

        logger.info(f"Insert/update result: {result}")

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
        logger.exception
