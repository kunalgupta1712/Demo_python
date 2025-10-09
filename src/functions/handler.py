import os
import json
import logging
from typing import Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor

# Custom modules (your own project files)
from db_operation import insert_or_update_users_bulk  # Handles DB insert/update logic
# from event_publisher import publish_event             # Publishes event to event mesh or API

# Configure a logger for this module
logger = logging.getLogger(__name__)


def main(event: Dict[str, Any]) -> Dict[str, Union[int, str]]:
    """
    Main handler function for processing SuccessFactors/Fieldglass user data.

    Args:
        event: The event payload (as a dictionary) containing user data to process.

    Returns:
        A dictionary containing:
          - statusCode (HTTP-style integer)
          - body (JSON string with response message)
    """

    # ----------------------------------------------------------------------
    # Step 1: Validate that all required environment variables are set.
    # These hold DB credentials, schema names, and event configuration.
    # ----------------------------------------------------------------------
    required_env_vars = [
        "HANA_SERVER_NODE",
        "HANA_PORT",
        "HANA_USER",
        "HANA_PASSWORD",
        "HANA_SCHEMA",
        # "SP_USERS_LOAD_FUNCTION_NAME",  # Used for event naming
        # "EVENT_URL",                    # Target event destination endpoint
    ]

    # Check for any missing environment variables
    missing_env_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_env_vars:
        error_msg = f"Missing environment variables: {', '.join(missing_env_vars)}"
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": error_msg}),
        }

    # ----------------------------------------------------------------------
    # Step 2: Parse incoming payload (from event trigger or HTTP request)
    # event may come from Kyma Function, API Gateway, or CloudEvent.
    # ----------------------------------------------------------------------
    users = event.get("body") or event.get("data")

    # If the incoming data is a string, attempt to parse JSON
    if isinstance(users, str):
        try:
            users = json.loads(users)
        except json.JSONDecodeError:
            logger.error("Error parsing JSON in request body")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid JSON in request body."}),
            }

    # ----------------------------------------------------------------------
    # Step 3: Normalize data format.
    # Convert single record (dict) to a list for consistency.
    # ----------------------------------------------------------------------
    if not isinstance(users, list):
        if users:
            logger.info("Received a single user record. Wrapping it in a list.")
            users = [users]
        else:
            logger.warning("Received no user data. Converting to empty list.")
            users = []

    # ----------------------------------------------------------------------
    # Step 4: Validate user data list.
    # Reject empty lists — no work to perform.
    # ----------------------------------------------------------------------
    if not users:
        logger.error("No user records provided.")
        return {
            "statusCode": 400,
            "body": json.dumps(
                {"message": "Request body must contain a list of user records."}
            ),
        }

    # ----------------------------------------------------------------------
    # Step 5: Process the users and publish event **in parallel**.
    # This improves p

