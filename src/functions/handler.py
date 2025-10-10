import os
import json
import logging
from db_operation import insert_or_update_users_bulk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    """
    Kyma Python handler for POST JSON payloads.
    Supports:
      - Plain dict (API testing)
      - CloudEvent (only if .data exists)
    """
    try:
        # Step 0: Log the incoming event
        logger.info(f"Received event: {event}")
        logger.info(f"Event type: {type(event)}")
        logger.info(f"Event attributes: {dir(event)}")

        payload = None

        # Step 1: Try CloudEvent first
        if hasattr(event, "data"):
            payload = getattr(event, "data")
            if payload is not None:
                logger.info("Detected CloudEvent with data")
            else:
                logger.warning("CloudEvent has no data attribute")

        # Step 2: Fallback to plain dict
        if payload is None and isinstance(event, dict):
            payload = event.get("body") or event.get("data") or event
            logger.info("Detected plain dict payload")

        # Step 3: If still None, unsupported event
        if payload is None:
            logger.error(f"Unsupported event type: {type(event)}")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": f"Unsupported event type: {type(event)}"})
            }

        # Step 4: Parse JSON string if needed
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                logger.error("Invalid JSON in payload")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid JSON payload"})
                }

        # Step 5: Normalize to list
        if not isinstance(payload, list):
            payload = [payload]

        # Step 6: Filter valid users (userID starts with 'P')
        valid_users = [u for u in payload if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in payload if not str(u.get("userID", "")).startswith("P")]

        if skipped_users:
            logger.warning(f"Skipped users: {[u.get('userID') for u in skipped_users]}")

        if not valid_users:
            logger.error("No valid users to process")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid users to process"})
            }

        # Step 7: Insert/update in DB
        result = insert_or_update_users_bulk(valid_users)
        logger.info(f"Inserted: {result.get('inserted', 0)}, Updated: {result.get('updated', 0)}")

        # Step 8: Return response
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
