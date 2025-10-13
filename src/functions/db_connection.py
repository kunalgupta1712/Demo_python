import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_hana_client():
    try:
        # --- Read environment variables ---
        server_node = os.getenv("HANA_SERVER_NODE")
        port = os.getenv("HANA_PORT", "443")
        user = os.getenv("HANA_USER")
        password = os.getenv("HANA_PASSWORD")
        schema = os.getenv("HANA_SCHEMA")

        # --- Log env vars safely ---
        logger.info("Environment variables for HANA connection:")
        logger.info(f" 🌐 HANA_SERVER_NODE = {server_node}")
        logger.info(f" 🔌 HANA_PORT = {port}")
        logger.info(f" 👤 HANA_USER = {user}")
        logger.info(f" 🧾 HANA_SCHEMA = {schema}")

        # --- Build connection string ---
        connection_string = f"hana+hdbcli://{user}:{password}@{server_node}:{port}?currentSchema={schema}"

        # --- Log connection string safely (hide password) ---
        safe_connection_string = f"hana+hdbcli://{user}:********@{server_node}:{port}?currentSchema={schema}"
        logger.info(f"🔗 Final HANA connection string: {safe_connection_string}")

        # --- Create SQLAlchemy engine ---
        engine = create_engine(connection_string)

        # --- Test connection ---
        with engine.connect() as conn:
            logger.info("✅ Successfully connected to SAP HANA")
            return engine

    except SQLAlchemyError as e:
        logger.exception(f"❌ HANA Connection Error: {e}")
        raise RuntimeError(f"HANA connection failed: {e}") from e
