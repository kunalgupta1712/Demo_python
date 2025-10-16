import os
import uuid
import logging
import random
from sqlalchemy import text
from db_connection import get_hana_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def register_contact_as_erp(account_id: int, first_name: str, last_name: str,
                            email: str, department=None, country=None,
                            cshme_flag=None, phone_no=None, status=None,
                            contact_id=None):
    """
    Registers a CRM contact as an ERP customer contact and updates CRM_COMPANY_CONTACTS.erpContactPerson.
    
    - Finds the corresponding customerId from ERP_CUSTOMERS via crmBpNo = accountId
    - Generates contactPersonId in defined range
    - Inserts into ERP_CUSTOMERS_CONTACTS
    - Updates CRM_COMPANY_CONTACTS.erpContactPerson
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("HANA_SCHEMA environment variable is not set.")

    start = int(os.getenv("ERP_CONTACTPERSONID_START", 100000))
    end = int(os.getenv("ERP_CONTACTPERSONID_END", 999999))
    contact_person_id = str(random.randint(start, end))

    engine = get_hana_client()

    with engine.begin() as connection:
        # Get corresponding customerId from ERP_CUSTOMERS
        query = text(f"""
            SELECT customerId FROM {schema}.SPUSER_STAGING_ERP_CUSTOMERS
            WHERE crmBpNo = :account_id
        """)
        result = connection.execute(query, {"account_id": account_id}).fetchone()

        if not result:
            logger.warning(
                "No ERP customer found for crmBpNo=%s, skipping contact registration", account_id
            )
            return None

        customer_id = result[0]

        # Insert into ERP_CUSTOMERS_CONTACTS
        insert_query = text(f"""
            INSERT INTO {schema}.SPUSER_STAGING_ERP_CUSTOMERS_CONTACTS (
                uuid, contactPersonId, customerId, crmBpNo,
                firstName, lastName, email, department, country,
                cshmeFlag, phoneNo, status
            )
            VALUES (
                :uuid, :contactPersonId, :customerId, :crmBpNo,
                :firstName, :lastName, :email, :department, :country,
                :csmeFlag, :phoneNo, :status
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
                "csmeFlag": cshme_flag,
                "phoneNo": phone_no,
                "status": status,
            }
        )

        logger.info(
            "Registered contact %s %s (accountId=%s) as ERP contactPersonId=%s",
            first_name, last_name, account_id, contact_person_id
        )

        # --- Update CRM_COMPANY_CONTACTS.erpContactPerson ---
        if contact_id:
            update_query = text(f"""
                UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS
                SET erpContactPerson = :contactPersonId
                WHERE contactId = :contactId
            """)
            connection.execute(update_query, {
                "contactPersonId": contact_person_id,
                "contactId": contact_id,
            })
            logger.info(
                "Updated CRM_COMPANY_CONTACTS.erpContactPerson for contactId=%s to %s",
                contact_id, contact_person_id
            )

    return contact_person_id
