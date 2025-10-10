import os
import json
import logging
from db_operation import insert_or_update_users_bulk

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    """
    Kyma Python handler supporting:
      - CloudEvents (lib.ce.Event) via event.req
      - Plain dict payloads (API tools like Postman)
    Preserves detailed logging for debugging.
    """

    try:
        logger.info(f"Received event: {event}")
        logger.info(f"Event type: {type(event)}")
        logger.info(f"Event attributes: {dir(event)}")

        # Step 1: Determine payload
        payload = None

        # CloudEvent (Python)
        if hasattr(event, "req"):
            try:
                # Try reading JSON directly
                payload = event.req.json
                logger.info("Detected CloudEvent payload via event.req.json")
            except Exception as e_json:
                # fallback: read raw body
                try:
                    raw_body = event.req.body.read()
                    payload = json.loads(raw_body)
                    logger.info("Detected CloudEvent payload via event.req.body")
                except Exception as e_body:
                    logger.exception("Failed to extract payload from CloudEvent")
                    return {
                        "statusCode": 400,
                        "body": json.dumps({
                            "message": "CloudEvent has no accessible data field",
                            "error_json": str(e_json),
                            "error_body": str(e_body)
                        })
                    }

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

        # Step 2: Parse JSON string if needed
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
                logger.info("Parsed payload from JSON string")
            except json.JSONDecodeError:
                logger.error("Invalid JSON payload")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Invalid JSON in payload"})
                }

        # Step 3: Normalize to list
        if not isinstance(payload, list):
            payload = [payload]
            logger.info("Normalized payload to list")

        logger.info(f"Payload received: {payload}")

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
