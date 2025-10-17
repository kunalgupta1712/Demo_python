import os
import uuid
import logging
from datetime import datetime
from sqlalchemy import text
from db_connection import get_hana_client
from id_generation import generate_sequential_id

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def register_company_as_customer(account_id: int, account_name: str):
    """
    Register a CRM company (account) as an ERP customer.

    - Inserts into ERP_CUSTOMERS table if new.
    - Updates lastModified if record already exists.
    - Returns the newly created or existing customerId.
    - Adds created and lastModified timestamps:
        * created → set when the record is first inserted and never changes.
        * lastModified → updated each time the record is inserted or modified.
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()
    now_utc = datetime.utcnow()

    with engine.begin() as connection:
        # 🔹 Check if the CRM account already exists in ERP_CUSTOMERS
        existing_query = text(f"""
            SELECT customerId, created, name
            FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            WHERE crmBpNo = :account_id
        """)
        existing = connection.execute(existing_query, {"account_id": account_id}).fetchone()

        if existing:
            # 🔁 Record exists → update lastModified only if there is a change
            existing_customer_id, created, existing_name = existing

            if existing_name != account_name:
                update_query = text(f"""
                    UPDATE {schema}.SPUSER_STAGING_ERP_CUSTOMERS
                    SET name = :name,
                        lastModified = :lastModified
                    WHERE crmBpNo = :crmBpNo
                """)
                connection.execute(
                    update_query,
                    {
                        "name": account_name,
                        "lastModified": now_utc,
                        "crmBpNo": account_id,
                    },
                )
                logger.info(
                    "🔁 Updated ERP customer (accountId=%s, customerId=%s, lastModified=%s)",
                    account_id, existing_customer_id, now_utc
                )
            else:
                logger.info(
                    "ℹ️ No change detected for ERP customer (accountId=%s, customerId=%s)",
                    account_id, existing_customer_id
                )

            return existing_customer_id

        # 🆕 Insert new ERP customer record → generate ID only now
        start = int(os.getenv("ERP_CUSTOMERID_START", 1000000))
        end = int(os.getenv("ERP_CUSTOMERID_END", 9999999))

        customer_id = generate_sequential_id(
            id_type="customerId",
            start_range=start,
            end_range=end
        )

        insert_query = text(f"""
            INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            (uuid, customerId, name, crmBpNo, created, lastModified)
            VALUES (:uuid, :customerId, :name, :crmBpNo, :created, :lastModified)
        """)
        connection.execute(
            insert_query,
            {
                "uuid": str(uuid.uuid4()),
                "customerId": customer_id,
                "name": account_name,
                "crmBpNo": account_id,
                "created": now_utc,
                "lastModified": now_utc,
            },
        )

        logger.info(
            "✅ Registered new ERP customer (accountId=%s → customerId=%s, created=%s)",
            account_id, customer_id, now_utc
        )

    return customer_id
