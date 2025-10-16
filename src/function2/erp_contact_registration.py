import os
import uuid
import logging
from sqlalchemy import text
from db_connection import get_hana_client
from id_generation import generate_sequential_id  # 👈 Import the sequential generator

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
    - Generates sequential contactPersonId within defined range
    - Inserts into ERP_CUSTOMERS_CONTACTS
    - Returns the contactPersonId
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    # 🔹 Get start/end range from env vars with fallback defaults
    start = int(os.getenv("ERP_CONTACTPERSONID_START", 2000000))
    end = int(os.getenv("ERP_CONTACTPERSONID_END", 2999999))

    # 🔹 Generate sequential unique contactPersonId using helper service
    contact_person_id = generate_sequential_id(
        id_type="contactPersonId",
        start_range=start,
        end_range=end
    )

    engine = get_hana_client()

    with engine.begin() as connection:
        # 🔹 Find corresponding ERP customerId for this CRM account
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

        # 🔹 Insert ERP customer contact record
        insert_query = text(f"""
            INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS (
                uuid, contactPersonId, customerId, crmBpNo,
                firstName, lastName, email, department, country,
                cshmeFlag, phoneNo, status
            )
            VALUES (
                :uuid, :contactPersonId, :customerId, :crmBpNo,
                :firstName, :lastName, :email, :department, :country,
                :cshmeFlag, :phoneNo, :status
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
            },
        )

        logger.info(
            "✅ Registered ERP Contact: %s %s (Account=%s → ERP=%s, ContactID=%s)",
            first_name, last_name, account_id, customer_id, contact_person_id
        )

    return contact_person_id
