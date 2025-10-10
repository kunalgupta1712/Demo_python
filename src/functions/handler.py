import json
import logging
from lib import ce

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(event, context=None):
    """
    Handles both CloudEvent (from Kyma) and direct JSON input (from Bruno/Postman).
    """
    try:
        logger.info("Incoming event type: %s", type(event))

        # ✅ Case 1: Event is a CloudEvent (from Kyma)
        if isinstance(event, ce.Event):
            logger.info("Processing CloudEvent...")
            data = event.Data() if hasattr(event, "Data") else None

            if data is None:
                raise ValueError("CloudEvent has no data field")

        # ✅ Case 2: Event is a dict (from Bruno or local test)
        elif isinstance(event, dict):
            logger.info("Processing direct JSON dictionary...")
            data = event

        # ✅ Case 3: Event is a raw JSON string (from local invoke)
        elif isinstance(event, str):
            logger.info("Processing JSON string input...")
            data = json.loads(event)

        # ❌ Unknown input type
        else:
            raise TypeError(f"Unsupported event type: {type(event)}")

        # ✅ Now process data safely
        if not isinstance(data, list):
            raise ValueError("Payload must be a JSON array of users")

        results = []
        for user in data:
            user_id = user.get("userID")
            status = user.get("status")
            logger.info(f"Processing user {user_id} with status {status}")
            results.append({
                "userID": user_id,
                "processed": True,
                "status": status
            })

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Processed successfully", "results": results})
        }

    except Exception as e:
        logger.exception("Error processing event")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(e)})
        }
