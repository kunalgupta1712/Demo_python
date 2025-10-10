import json
import logging
from lib import ce

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



def main(event, context=None):
    """
    Handles both CloudEvent (Kyma) and direct JSON input (from Bruno/Postman).
    Compatible with all Kyma Python runtimes (different CloudEvent APIs).
    """
    try:
        logger.info("Incoming event type: %s", type(event))

        data = None

        # ✅ Case 1: CloudEvent from Kyma
        if isinstance(event, ce.Event):
            logger.info("Processing CloudEvent...")

            # Try all possible property/method variants
            if hasattr(event, "data"):
                data = event.data
            elif hasattr(event, "Data"):
                data = event.Data()
            elif hasattr(event, "get_data"):
                data = event.get_data()
            else:
                raise ValueError("CloudEvent has no accessible data field")

        # ✅ Case 2: Direct dict JSON (Bruno or local)
        elif isinstance(event, dict):
            logger.info("Processing direct JSON dictionary...")
            data = event

        # ✅ Case 3: Raw JSON string
        elif isinstance(event, str):
            logger.info("Processing raw JSON string...")
            data = json.loads(event)

        else:
            raise TypeError(f"Unsupported event type: {type(event)}")

        # ✅ Validate payload
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
            "body": json.dumps({
                "message": "Processed successfully",
                "results": results
            })
        }

    except Exception as e:
        logger.exception("Error processing event")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": str(e)})
        }
