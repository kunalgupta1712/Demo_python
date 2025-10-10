import json
import logging
from lib import ce
from db_operation import insert_or_update_users_bulk

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(event, context=None):
    try:
        logger.info(f"Event type: {type(event)}")

        # Try to extract data from CloudEvent
        data = None
        if isinstance(event, ce.Event):
            logger.info("CloudEvent detected")

            # Try all known data attributes
            for attr in ["data", "_Event__data", "_data"]:
                if hasattr(event, attr):
                    data = getattr(event, attr)
                    if data:
                        logger.info(f"Data found in {attr}")
                        break

        # Fallback: treat as direct HTTP POST
        if data is None:
            logger.info("Falling back to HTTP request mode")
            if isinstance(event, dict):
                data = event.get("body") or event.get("data") or event
            elif isinstance(event, str):
                data = json.loads(event)
            else:
                raise TypeError(f"Unsupported event type: {type(event)}")

        # Parse if string
        if isinstance(data, str):
            data = json.loads(data)

        # Ensure it's a list
        if not isinstance(data, list):
            data = [data]

        # Filter valid users
        valid_users = [u for u in data if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in data if not str(u.get("userID", "")).startswith("P")]

        if not valid_users:
            return {"statusCode": 400, "body": json.dumps({"message": "No valid users to process"})}

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
        logger.exception("Error in handler")
        return {"statusCode": 400, "body": json.dumps({"message": str(e)})}
