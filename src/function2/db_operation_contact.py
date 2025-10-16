import os
import logging
import uuid
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client
from erp_contact_registration import register_contact_as_erp  # Your ERP registration service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_or_update_contact(contacts: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update contacts into CRM_COMPANY_CONTACTS table.
    Calls ERP registration for contacts with crmToErpFlag=True or changed from False->True.
    Updates erpContactPerson column in CRM_COMPANY_CONTACTS after registration.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()
    inserted_count = 0
    updated_count = 0
    failed_contacts = []

    with engine.begin() as connection:
        for contact in contacts:
            contact_id = contact.get("contactId")
            account_id = contact.get("accountId")

            try:
                # Check if contact exists
                existing = get_existing_contacts(connection, schema, [contact_id])
                if not existing:
                    # Insert new contact
                    insert_contacts_bulk(connection, schema, [contact])
                    inserted_count += 1
                else:
                    # Update existing contact
                    update_contacts_bulk(connection, schema, [contact])
                    updated_count += 1

                # ERP registration logic
                if contact.get("crmToErpFlag"):
                    # Only register if crmToErpFlag is True (insert or updated from False->True)
                    contact_person_id = register_contact_as_erp(
                        account_id,
                        contact.get("firstName"),
                        contact.get("lastName"),
                        contact.get("email"),
                        department=contact.get("department"),
                        country=contact.get("country"),
                        cshme_flag=contact.get("cshmeFlag"),
                    )

                    if contact_person_id:
                        # Update CRM_COMPANY_CONTACTS.erpContactPerson
                        update_erp_contact_query = text(f"""
                            UPDATE {schema}.CRM_COMPANY_CONTACTS
                            SET erpContactPerson = :contactPersonId
                            WHERE contactId = :contactId
                        """)
                        connection.execute(update_erp_contact_query, {
                            "contactPersonId": contact_person_id,
                            "contactId": contact_id,
                        })

            except Exception as e:
                logger.exception(
                    "Failed to insert/update contactId=%s: %s",
                    contact_id,
                    e,
                )
                failed_contacts.append({"contact": contact, "error": str(e)})

    logger.info(
        "Insert/Update Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count,
        updated_count,
        len(failed_contacts),
    )

    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed_contacts,
    }


def get_existing_contacts(connection, schema: str, contact_ids: List[int]) -> List[int]:
    """Return list of contactIds that already exist in CRM_COMPANY_CONTACTS."""
    if not contact_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(contact_ids))])
    query = f"SELECT contactId FROM {schema}.CRM_COMPANY_CONTACTS WHERE contactId IN ({placeholders})"
    params = {f"id_{i}": val for i, val in enumerate(contact_ids)}

    result = connection.execute(text(query), params)
    return [row[0] for row in result.fetchall()]


def insert_contacts_bulk(connection, schema: str, contacts: List[Dict[str, Any]]):
    """Insert new contacts into CRM_COMPANY_CONTACTS."""
    sql = f"""
        INSERT INTO {schema}.CRM_COMPANY_CONTACTS (
            uuid, contactId, accountId, accountName, crmToErpFlag,
            firstName, lastName, cshmeFlag, email, department,
            country, zipCode
        )
        VALUES (
            :uuid, :contactId, :accountId, :accountName, :crmToErpFlag,
            :firstName, :lastName, :csmeFlag, :email, :department,
            :country, :zipCode
        )
    """

    batch = []
    for c in contacts:
        batch.append({
            "uuid": str(uuid.uuid4()),
            "contactId": c.get("contactId"),
            "accountId": c.get("accountId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
            "firstName": c.get("firstName"),
            "lastName": c.get("lastName"),
            "csmeFlag": c.get("cshmeFlag"),
            "email": c.get("email"),
            "department": c.get("department"),
            "country": c.get("country"),
            "zipCode": c.get("zipCode"),
        })

    connection.execute(text(sql), batch)
    logger.info("Inserted %d contact(s)", len(contacts))


def update_contacts_bulk(connection, schema: str, contacts: List[Dict[str, Any]]):
    """Update existing contacts in CRM_COMPANY_CONTACTS."""
    sql = f"""
        UPDATE {schema}.CRM_COMPANY_CONTACTS
        SET accountName = :accountName,
            crmToErpFlag = :crmToErpFlag,
            firstName = :firstName,
            lastName = :lastName,
            cshmeFlag = :csmeFlag,
            email = :email,
            department = :department,
            country = :country,
            zipCode = :zipCode
        WHERE contactId = :contactId
    """

    batch = []
    for c in contacts:
        batch.append({
            "contactId": c.get("contactId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
            "firstName": c.get("firstName"),
            "lastName": c.get("lastName"),
            "csmeFlag": c.get("cshmeFlag"),
            "email": c.get("email"),
            "department": c.get("department"),
            "country": c.get("country"),
            "zipCode": c.get("zipCode"),
        })

    connection.execute(text(sql), batch)
    logger.info("Updated %d contact(s)", len(contacts))
