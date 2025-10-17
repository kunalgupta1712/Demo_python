import os
import uuid
import logging
from datetime import datetime
from sqlalchemy import text
from db_connection import get_hana_client
from id_generation import generate_sequential_id

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def register_company_as_customer(account_id: int, account_name: str, status: str):
    """
    Register or update a CRM company as an ERP customer.

    - Generates customerId only for new records.
    - Inserts into ERP_CUSTOMERS if new.
    - Updates existing records with incoming data.
    - Includes 'status' field for both insert and update.
    - Adds created and lastModified timestamps:
        * created → set when first inserted, never changes.
        * lastModified → updated each insert/update.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()
    now_utc = datetime.utcnow()

    with engine.begin() as connection:
        # 🔹 Check if the CRM account already exists
        existing_query = text(f"""
            SELECT customerId, created
            FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            WHERE crmBpNo = :account_id
        """)
        existing = connection.execute(existing_query, {"account_id": account_id}).fetchone()

        if existing:
            # 🔄 Update existing record
            existing_customer_id, created = existing
            update_query = text(f"""
                UPDATE {schema}.SPUSER_STAGING_ERP_CUSTOMERS
                SET name = :name,
                    status = :status,
                    lastModified = :lastModified
                WHERE crmBpNo = :crmBpNo
            """)
            connection.execute(
                update_query,
                {
                    "name": account_name,
                    "status": status,
                    "lastModified": now_utc,
                    "crmBpNo": account_id,
                },
            )
            logger.info(
                "🔁 Updated ERP customer (accountId=%s, customerId=%s, status=%s, lastModified=%s)",
                account_id, existing_customer_id, status, now_utc
            )
            return existing_customer_id

        # 🆕 Insert new ERP customer record → generate customerId now
        start = int(os.getenv("ERP_CUSTOMERID_START", 1000000))
        end = int(os.getenv("ERP_CUSTOMERID_END", 9999999))

        customer_id = generate_sequential_id(
            id_type="customerId",
            start_range=start,
            end_range=end
        )

        insert_query = text(f"""
            INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            (uuid, customerId, name, crmBpNo, status, created, lastModified)
            VALUES (:uuid, :customerId, :name, :crmBpNo, :status, :created, :lastModified)
        """)
        connection.execute(
            insert_query,
            {
                "uuid": str(uuid.uuid4()),
                "customerId": customer_id,
                "name": account_name,
                "crmBpNo": account_id,
                "status": status,
                "created": now_utc,
                "lastModified": now_utc,
            },
        )
        logger.info(
            "✅ Registered new ERP customer (accountId=%s → customerId=%s, status=%s, created=%s)",
            account_id, customer_id, status, now_utc
        )

    return customer_id
