import os
import json
import logging
from db_operation import insert_or_update_users_bulk

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    """
    Kyma Python handler that supports:
      - CloudEvents (lib.ce.Event)
      - Plain dict (API tool testing)
    Logs all steps for debugging.
    """
    try:
        logger.info(f"Received event: {event}")
        logger.info(f"Event type: {type(event)}")
        logger.info(f"Event attributes: {dir(event)}")

        # Step 1: Determine payload
        payload = None

        # Kyma CloudEvent
        if hasattr(event, "data"):
            payload = event.data
            logger.info("Detected CloudEvent payload")
        # Plain dict (local API testing)
        elif isinstance(event, dict):
            payload = event.get("body") or event.get("data") or event
            logger.info("Detected plain dict payload")
        else:
            logger.error(f"Unsupported event type: {type(event)}")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": f"Unsupported event type: {type(event)}"})
            }

        logger.info(f"Raw payload before parsing: {payload}")

        # Step 2: Parse JSON string if needed
        if isinstance(payload, str):
            payload = json.loads(payload)
            logger.info(f"Parsed JSON payload: {payload}")

        # Step 3: Normalize to list
        if not isinstance(payload, list):
            payload = [payload]
            logger.info("Normalized payload to list")

        logger.info(f"Payload count: {len(payload)}")
        logger.info(f"Payload content: {payload}")

        # Step 4: Filter valid users (userID starting with 'P')
        valid_users = [u for u in payload if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in payload if not str(u.get("userID", "")).startswith("P")]

        if skipped_users:
            logger.warning(f"Skipped users (invalid userID): {[u.get('userID') for u in skipped_users]}")

        if not valid_users:
            logger.error("No valid users to process")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid users to process"})
            }

        logger.info(f"Valid users to process: {valid_users}")

        # Step 5: Insert/update DB
        result = insert_or_update_users_bulk(valid_users)
        logger.info(f"DB Operation Result: {result}")

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
