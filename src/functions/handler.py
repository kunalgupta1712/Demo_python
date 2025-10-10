import json
import logging
from lib import ce
from db_operation import insert_or_update_users_bulk

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(event, context=None):
    """
    Handles CloudEvent (Kyma) and direct JSON input (from Bruno/Postman).
    Compatible with all known Kyma runtimes.
    """
    try:
        logger.info(f"Incoming event type: {type(event)}")

        data = None

        # ✅ Case 1: CloudEvent from Kyma
        if isinstance(event, ce.Event):
            logger.info("Processing as CloudEvent")

            # Try standard attributes
            if hasattr(event, "data"):
                data = getattr(event, "data", None)
            if data is None and hasattr(event, "Data"):
                data = event.Data()
            if data is None and hasattr(event, "get_data"):
                data = event.get_data()

            # ✅ Fallback: access internal private data fields
            if data is None and hasattr(event, "_Event__data"):
                data = getattr(event, "_Event__data")
            if data is None and hasattr(event, "_data"):
                data = getattr(event, "_data")

            if data is None:
                raise ValueError("CloudEvent has no accessible data field")

        # ✅ Case 2: Direct JSON dict (for Bruno/Postman)
        elif isinstance(event, dict):
            logger.info("Processing as direct JSON dict")
            data = event.get("body") or event.get("data") or event

        # ✅ Case 3: Raw JSON string
        elif isinstance(event, str):
            logger.info("Processing as raw JSON string")
            data = json.loads(event)

        else:
            raise TypeError(f"Unsupported event type: {type(event)}")

        # ✅ Ensure data is parsed JSON
        if isinstance(data, str):
            data = json.loads(data)

        # ✅ Ensure list format
        if not isinstance(data, list):
            data = [data]

        # ✅ Filter valid userIDs
        valid_users = [u for u in data if str(u.get("userID", "")).startswith("P")]
        skipped_users = [u for u in data if not str(u.get("userID", "")).startswith("P")]

        if skipped_users:
            logger.warning(f"Skipped users: {[u.get('userID') for u in skipped_users]}")

        if not valid_users:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "No valid users to process"})
            }

        # ✅ Perform DB insert/update
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
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(e)})
        }
