import os
import logging
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_hana_client():
    try:
        # Read environment variables
        server_node = os.getenv("HANA_SERVER_NODE")
        port = os.getenv("HANA_PORT", "443")
        user = os.getenv("HANA_USER")
        password = os.getenv("HANA_PASSWORD")
        schema = os.getenv("HANA_SCHEMA")

        # Check if all required env vars are present
        if not all([server_node, user, password, schema]):
            raise ValueError("Required HANA environment variables are missing")

        # Basic log (do not expose password)
        logger.info("Initializing SAP HANA connection...")
        logger.info(
            "HANA_SERVER_NODE=%s, PORT=%s, USER=%s, SCHEMA=%s",
            server_node,
            port,
            user,
            schema,
        )

        # Encode password
        encoded_password = urllib.parse.quote_plus(password)

        # Build connection string
        connection_string = (
            f"hana+hdbcli://{user}:{encoded_password}@"
            f"{server_node}:{port}?currentSchema={schema}"
        )

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect():
            logger.info("✅ Successfully connected to SAP HANA")
            return engine

    except SQLAlchemyError as e:
        logger.exception("HANA Connection Error: %s", e)
        raise RuntimeError(f"HANA connection failed: {e}") from e
