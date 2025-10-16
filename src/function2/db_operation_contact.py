import os
import uuid
import logging
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client
from erp_contact_registration import register_contact_as_erp

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def insert_or_update_contact(contacts: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update contacts in CRM_COMPANY_CONTACTS table.
    If crmToErpFlag=True (new or changed), register contact in ERP_CUSTOMERS_CONTACTS.
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("HANA_SCHEMA environment variable is not set.")

    engine = get_hana_client()
    inserted_count = 0
    updated_count = 0
    failed = []

    with engine.begin() as connection:
        for contact in contacts:
            account_id = contact.get("accountId")
            contact_id = contact.get("contactId")
            crm_to_erp_flag = contact.get("crmToErpFlag")

            if not account_id or not contact_id:
                logger.warning("Skipping invalid contact entry: %s", contact)
                failed.append({"contact": contact, "error": "Missing mandatory fields"})
                continue

            try:
                # Check if contact exists
                existing_query = text(f"""
                    SELECT crmToErpFlag FROM {schema}.CRM_COMPANY_CONTACTS
                    WHERE contactId = :contactId
                """)
                result = connection.execute(existing_query, {"contactId": contact_id}).fetchone()

                if result:
                    existing_flag = result[0]
                    update_query = text(f"""
                        UPDATE {schema}.CRM_COMPANY_CONTACTS
                        SET accountName = :accountName,
                            crmToErpFlag = :crmToErpFlag,
                            firstName = :firstName,
                            lastName = :lastName,
                            email = :email,
                            department = :department,
                            country = :country,
                            cshmeFlag = :cshmeFlag,
                            zipCode = :zipCode
                        WHERE contactId = :contactId
                    """)
                    connection.execute(
                        update_query,
                        {
                            "accountName": contact.get("accountName"),
                            "crmToErpFlag": crm_to_erp_flag,
                            "firstName": contact.get("firstName"),
                            "lastName": contact.get("lastName"),
                            "email": contact.get("email"),
                            "department": contact.get("department"),
                            "country": contact.get("country"),
                            "cshmeFlag": contact.get("cshmeFlag"),
                            "zipCode": contact.get("zipCode"),
                            "contactId": contact_id,
                        }
                    )
                    updated_count += 1

                    # Flag changed from False → True
                    if not existing_flag and crm_to_erp_flag:
                        register_contact_as_erp(
                            account_id,
                            contact.get("firstName"),
                            contact.get("lastName"),
                            contact.get("email"),
                            department=contact.get("department"),
                            country=contact.get("country"),
                            cshme_flag=contact.get("cshmeFlag"),
                        )

                else:
                    insert_query = text(f"""
                        INSERT INTO {schema}.CRM_COMPANY_CONTACTS (
                            uuid, contactId, accountId, accountName, crmToErpFlag,
                            firstName, lastName, email, department, country, cshmeFlag, zipCode
                        )
                        VALUES (
                            :uuid, :contactId, :accountId, :accountName, :crmToErpFlag,
                            :firstName, :lastName, :email, :department, :country, :cshmeFlag, :zipCode
                        )
                    """)
                    connection.execute(
                        insert_query,
                        {
                            "uuid": str(uuid.uuid4()),
                            "contactId": contact_id,
                            "accountId": account_id,
                            "accountName": contact.get("accountName"),
                            "crmToErpFlag": crm_to_erp_flag,
                            "firstName": contact.get("firstName"),
                            "lastName": contact.get("lastName"),
                            "email": contact.get("email"),
                            "department": contact.get("department"),
                            "country": contact.get("country"),
                            "cshmeFlag": contact.get("cshmeFlag"),
                            "zipCode": contact.get("zipCode"),
                        }
                    )
                    inserted_count += 1

                    # crmToErpFlag is True → register contact
                    if crm_to_erp_flag:
                        register_contact_as_erp(
                            account_id,
                            contact.get("firstName"),
                            contact.get("lastName"),
                            contact.get("email"),
                            department=contact.get("department"),
                            country=contact.get("country"),
                            cshme_flag=contact.get("cshmeFlag"),
                        )

            except Exception as e:
                logger.exception("Error processing contact %s: %s", contact_id, e)
                failed.append({"contact": contact, "error": str(e)})

    logger.info(
        "Contact Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count, updated_count, len(failed)
    )

    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed,
    }
