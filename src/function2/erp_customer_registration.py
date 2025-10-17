import os
import uuid
import logging
from sqlalchemy import text
from db_connection import get_hana_client
from id_generation import generate_sequential_id  # 👈 Import the new ID generator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def register_company_as_customer(account_id: int, account_name: str):
    """
    Register a CRM company (account) as an ERP customer.

    - Generates a sequential customerId within the defined range.
    - Inserts into ERP_CUSTOMERS table.
    - Returns the newly created customerId (without updating CRM_COMPANY_ACCOUNTS).
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    start = int(os.getenv("ERP_CUSTOMERID_START", 1000000))
    end = int(os.getenv("ERP_CUSTOMERID_END", 9999999))

    # 🔹 Generate a sequential unique customerId using the helper service
    customer_id = generate_sequential_id(
        id_type="customerId",
        start_range=start,
        end_range=end
    )

    engine = get_hana_client()
    with engine.begin() as connection:
        # Check if this account already has a registered ERP customer
        existing_query = text(
            f"""
            SELECT customerId 
            FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            WHERE crmBpNo = :account_id
            """
        )
        existing = connection.execute(existing_query, {"account_id": account_id}).fetchone()

        if existing:
            logger.info(
                "Company with accountId=%s already registered as customerId=%s",
                account_id,
                existing[0],
            )
            return existing[0]

        # Insert new ERP customer record
        insert_query = text(
            f"""
            INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            (uuid, customerId, name, crmBpNo)
            VALUES (:uuid, :customerId, :name, :crmBpNo)
            """
        )

        connection.execute(
            insert_query,
            {
                "uuid": str(uuid.uuid4()),
                "customerId": customer_id,
                "name": account_name,
                "crmBpNo": account_id,
            },
        )

        logger.info(
            "Registered company (accountId=%s) as ERP customerId=%s",
            account_id,
            customer_id,
        )

    return customer_id
