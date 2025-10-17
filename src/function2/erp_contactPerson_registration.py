import os
import uuid
import logging
from datetime import datetime
from sqlalchemy import text
from db_connection import get_hana_client
from id_generation import generate_sequential_id

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def register_contact_as_erp(
    account_id: int,
    first_name: str,
    last_name: str,
    email: str,
    department=None,
    country=None,
    cshme_flag=None,
    phone_no=None,
    status=None,
    contact_id=None
):
    """
    Registers a CRM contact as an ERP customer contact.

    - Finds the corresponding customerId from ERP_CUSTOMERS via crmBpNo = accountId
    - Generates sequential contactPersonId only for new records
    - Inserts into ERP_CUSTOMERS_CONTACTS (with createdAt & lastModified timestamps)
    - Updates existing records directly
    - Returns the contactPersonId
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()
    now_utc = datetime.utcnow()

    with engine.begin() as connection:
        # Find ERP Customer ID for given CRM Account
        query = text(f"""
            SELECT customerId 
            FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            WHERE crmBpNo = :account_id
        """)
        result = connection.execute(query, {"account_id": account_id}).fetchone()

        if not result:
            logger.warning(
                "⚠️ No ERP customer found for crmBpNo=%s — skipping ERP contact registration",
                account_id
            )
            return None

        customer_id = result[0]

        # Check if contact already exists
        existing_query = text(f"""
            SELECT contactPersonId, cshmeFlag, createdAt
            FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS
            WHERE crmBpNo = :account_id AND email = :email
        """)
        existing = connection.execute(existing_query, {"account_id": account_id, "email": email}).fetchone()

        if existing:
            contact_person_id, previous_flag, created_at = existing

            # Always update incoming record
            update_query = text(f"""
                UPDATE {schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS
                SET firstName = :firstName,
                    lastName = :lastName,
                    department = :department,
                    country = :country,
                    cshmeFlag = :cshmeFlag,
                    phoneNo = :phoneNo,
                    status = :status,
                    lastModified = :lastModified
                WHERE crmBpNo = :crmBpNo AND email = :email
            """)
            connection.execute(
                update_query,
                {
                    "firstName": first_name,
                    "lastName": last_name,
                    "department": department,
                    "country": country,
                    "cshmeFlag": cshme_flag,
                    "phoneNo": phone_no,
                    "status": status,
                    "lastModified": now_utc,
                    "crmBpNo": account_id,
                    "email": email
                },
            )

            logger.info(
                "🔁 Updated ERP contact: %s %s (Account=%s → ContactID=%s)",
                first_name, last_name, account_id, contact_person_id
            )

        else:
            # New record → generate contactPersonId
            start = int(os.getenv("ERP_CONTACTPERSONID_START", 2000000))
            end = int(os.getenv("ERP_CONTACTPERSONID_END", 2999999))

            contact_person_id = generate_sequential_id(
                id_type="contactPersonId",
                start_range=start,
                end_range=end
            )

            insert_query = text(f"""
                INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS (
                    uuid, contactPersonId, customerId, crmBpNo,
                    firstName, lastName, email, department, country,
                    cshmeFlag, phoneNo, status, createdAt, lastModified
                )
                VALUES (
                    :uuid, :contactPersonId, :customerId, :crmBpNo,
                    :firstName, :lastName, :email, :department, :country,
                    :cshmeFlag, :phoneNo, :status, :createdAt, :lastModified
                )
            """)
            connection.execute(
                insert_query,
                {
                    "uuid": str(uuid.uuid4()),
                    "contactPersonId": contact_person_id,
                    "customerId": customer_id,
                    "crmBpNo": account_id,
                    "firstName": first_name,
                    "lastName": last_name,
                    "email": email,
                    "department": department,
                    "country": country,
                    "cshmeFlag": cshme_flag,
                    "phoneNo": phone_no,
                    "status": status,
                    "createdAt": now_utc,
                    "lastModified": now_utc,
                },
            )

            logger.info(
                "✅ Registered new ERP contact: %s %s (Account=%s → ContactID=%s)",
                first_name, last_name, account_id, contact_person_id
            )

        # Log CloudEvent trigger condition 
            logger.info(
                "🟢 Send all users to main id store "
                "and the logic there will check whether "
                "a S user should be created, disabled, or enabled. based on cshmeFlag and status"
            )

    return contact_person_id
