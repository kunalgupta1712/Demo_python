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
    Insert or update contacts in SPUSER_STAGING_CRM_COMPANY_CONTACTS table.
    If crmToErpFlag=True, register contact in ERP_CUSTOMERS_CONTACTS and update erpContactPerson.
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
                # Check existing contact
                existing_query = text(f"""
                    SELECT crmToErpFlag, erpContactPerson 
                    FROM {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS
                    WHERE contactId = :contactId
                """)
                result = connection.execute(existing_query, {"contactId": contact_id}).fetchone()

                if result:
                    existing_flag, existing_erp_contact = result
                    # Update existing record
                    update_query = text(f"""
                        UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS
                        SET accountName = :accountName,
                            crmToErpFlag = :crmToErpFlag,
                            firstName = :firstName,
                            lastName = :lastName,
                            email = :email,
                            department = :department,
                            country = :country,
                            cshmeFlag = :cshmeFlag,
                            zipCode = :zipCode,
                            phoneNo = :phoneNo,
                            status = :status
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
                            "phoneNo": contact.get("phoneNo"),
                            "status": contact.get("status"),
                            "contactId": contact_id,
                        }
                    )
                    updated_count += 1

                    # Register in ERP if required
                    if crm_to_erp_flag and not existing_erp_contact:
                        contact_person_id = register_contact_as_erp(
                            account_id,
                            contact.get("firstName"),
                            contact.get("lastName"),
                            contact.get("email"),
                            department=contact.get("department"),
                            country=contact.get("country"),
                            cshme_flag=contact.get("cshmeFlag"),
                            phone_no=contact.get("phoneNo"),
                            status=contact.get("status"),
                        )

                        if contact_person_id:
                            update_erp_contact = text(f"""
                                UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS
                                SET erpContactPerson = :erpContactPerson
                                WHERE contactId = :contactId
                            """)
                            connection.execute(
                                update_erp_contact,
                                {
                                    "erpContactPerson": contact_person_id,
                                    "contactId": contact_id,
                                }
                            )
                            logger.info(
                                "Updated erpContactPerson=%s for contactId=%s",
                                contact_person_id, contact_id
                            )

                else:
                    # Insert new contact
                    insert_query = text(f"""
                        INSERT INTO {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS (
                            uuid, contactId, accountId, accountName, crmToErpFlag,
                            firstName, lastName, email, department, country, 
                            cshmeFlag, zipCode, phoneNo, status
                        )
                        VALUES (
                            :uuid, :contactId, :accountId, :accountName, :crmToErpFlag,
                            :firstName, :lastName, :email, :department, :country, 
                            :cshmeFlag, :zipCode, :phoneNo, :status
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
                            "phoneNo": contact.get("phoneNo"),
                            "status": contact.get("status"),
                        }
                    )
                    inserted_count += 1

                    # Register and update erpContactPerson
                    if crm_to_erp_flag:
                        contact_person_id = register_contact_as_erp(
                            account_id,
                            contact.get("firstName"),
                            contact.get("lastName"),
                            contact.get("email"),
                            department=contact.get("department"),
                            country=contact.get("country"),
                            cshme_flag=contact.get("cshmeFlag"),
                            phone_no=contact.get("phoneNo"),
                            status=contact.get("status"),
                        )

                        if contact_person_id:
                            update_erp_contact = text(f"""
                                UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_CONTACTS
                                SET erpContactPerson = :erpContactPerson
                                WHERE contactId = :contactId
                            """)
                            connection.execute(
                                update_erp_contact,
                                {
                                    "erpContactPerson": contact_person_id,
                                    "contactId": contact_id,
                                }
                            )
                            logger.info(
                                "Updated erpContactPerson=%s for contactId=%s",
                                contact_person_id, contact_id
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
