import os
import json
import logging
from db_operation import insert_or_update_users_bulk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(event, context=None):
    try:
        # 🔍 Step 1: Log event info (to understand structure)
        logger.info(f"Event type: {type(event)}")
        logger.info(f"Event attributes: {dir(event)}")
        logger.info(f"Event content: {getattr(event, '__dict__', {})}")

        # 🔍 Step 2: Try to extract payload
        payload = None

        # Case 1: If CloudEvent object
        if hasattr(event, "data"):
            payload = getattr(event, "data", None)
        elif hasattr(event, "_Event__data"):
            payload = getattr(event, "_Event__data", None)
        elif hasattr(event, "_data"):
            payload = getattr(event, "_data", None)

        # Case 2: If payload still not found
        if not payload:
            if isinstance(event, dict):
                payload = event.get("body") or event.get("data") or event
            elif isinstance(event, str):
                payload = json.loads(event)

        # Case 3: Parse JSON string
        if isinstance(payload, str):
            payload = json.loads(payload)

        # Ensure list
        if not isinstance(payload, list):
            payload = [payload]

        # Filter valid users
        valid_users = [u for u in payload if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in payload if not str(u.get("userID", "")).startswith("P")]

        if not valid_users:
            return {"statusCode": 400, "body": json.dumps({"message": "No valid users to process"})}

        # Step 4: Insert/update in DB
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
        logger.exception("Error processing event")
        return {"statusCode": 400, "body": json.dumps({"message": str(e)})}
