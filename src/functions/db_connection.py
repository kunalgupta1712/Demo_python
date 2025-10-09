import os
from typing import Any
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_hana_client() -> Any:
    """
    Create and return a connection to SAP HANA database.

    Returns:
        SQLAlchemy engine object for HANA database connection

    Raises:
        Exception: If connection to HANA database fails
    """
    try:
        # Get connection parameters from environment variables
        server_node = os.environ.get("HANA_SERVER_NODE")
        port = os.environ.get("HANA_PORT")
        user = os.environ.get("HANA_USER")
        password = os.environ.get("HANA_PASSWORD")
        schema = os.environ.get("HANA_SCHEMA")

        if not all([server_node, port, user, password, schema]):
            raise ValueError(
                "Missing required environment variables for HANA connection"
            )

        # Create connection string for HANA
        # Using sqlalchemy-hana dialect
        connection_string = f"hana+hdbcli://{user}:{password}@{server_node}:{port}"

        # Create engine
        engine = create_engine(connection_string, connect_args={"encrypt": "true"})

        # Test connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM DUMMY"))
            logger.info("Connected to SAP HANA Cloud.")

        return engine

    except SQLAlchemyError as err:
        logger.error("HANA Connection Error: %s", err)
        raise RuntimeError(f"HANA connection failed: {err}") from err
    except Exception as err:
        logger.error("Unexpected error during HANA connection: %s", err)
        raise RuntimeError(f"HANA connection failed: {err}") from err
