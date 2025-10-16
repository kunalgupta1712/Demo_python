import os
import logging
from sqlalchemy import text
from db_connection import get_hana_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def generate_sequential_id(id_type: str, start_range: int, end_range: int) -> str:
    """
    Generate a sequential, unique ID for a given ID type (customerId/contactPersonId).
    - Reads the max existing ID from the relevant table
    - Increments by 1 (starting from start_range if no rows exist)
    - Ensures the generated ID does not exceed the defined end_range
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    # Decide target table and column based on id_type
    if id_type == "customerId":
        table = f"{schema}.SPUSER_STAGING_ERP_CUSTOMERS"
        column = "customerId"
    elif id_type == "contactPersonId":
        table = f"{schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS"
        column = "contactPersonId"
    else:
        raise ValueError(f"Unsupported id_type: {id_type}")

    with engine.begin() as connection:
        # Get max existing ID from the table
        query = text(f"SELECT MAX({column}) FROM {table}")
        result = connection.execute(query).fetchone()
        max_id = result[0]

        if max_id is None:
            next_id = start_range
        else:
            try:
                next_id = int(max_id) + 1
            except ValueError:
                logger.warning(f"Invalid {column} value found in {table}: {max_id}")
                next_id = start_range

        # Check range validity
        if next_id > end_range:
            raise ValueError(f"{id_type} exceeded maximum range ({end_range})")

        logger.info(f"Generated new {id_type}: {next_id}")
        return str(next_id)
