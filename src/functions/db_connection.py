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

        # Basic log (do not expose password)
        logger.info("Initializing SAP HANA connection...")
        logger.info(f"HANA_SERVER_NODE={server_node}, PORT={port}, USER={user}, SCHEMA={schema}")

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
        with engine.connect() as conn:
            logger.info("✅ Successfully connected to SAP HANA")
            return engine

    except SQLAlchemyError as e:
        logger.exception(f"HANA Connection Error: {e}")
        raise RuntimeError(f"HANA connection failed: {e}") from e
