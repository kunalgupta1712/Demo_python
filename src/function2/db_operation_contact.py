import os
import logging
import uuid
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_or_update_contact(
    contacts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Insert or update records in CRM_COMPANY_CONTACTS table."""
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    inserted_count = 0
    updated_count = 0
    failed_records = []

    with engine.begin() as connection:
        for contact in contacts:
            contact_id = contact.get("contactId")
            if not contact_id:
                logger.warning(
                    "Skipping record with missing contactId: %s", contact
                )
                failed_records.append(
                    {"record": contact, "error": "Missing contactId"}
                )
                continue

            try:
                existing = get_existing_contacts(
                    connection, schema, [contact_id]
                )
                if not existing:
                    insert_contact(connection, schema, [contact])
                    inserted_count += 1
                else:
                    update_contact(connection, schema, [contact])
                    updated_count += 1
            except Exception as e:
                logger.exception(
                    "Failed to insert/update contactId=%s: %s",
                    contact_id,
                    e,
                )
                failed_records.append(
                    {"record": contact, "error": str(e)}
                )

    logger.info(
        "Contact Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count,
        updated_count,
        len(failed_records),
    )
    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed_records,
    }


def get_existing_contacts(connection, schema: str, contact_ids: List[int]):
    """Return list of existing contactIds."""
    if not contact_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(contact_ids))])
    query = (
        f"SELECT contactId FROM {schema}.CRM_COMPANY_CONTACTS "
        f"WHERE contactId IN ({placeholders})"
    )
    params = {f"id_{i}": val for i, val in enumerate(contact_ids)}
    result = connection.execute(text(query), params)
    return [row[0] for row in result.fetchall()]


def insert_contact(connection, schema: str, contacts: List[Dict[str, Any]]):
    """Insert new contact records."""
    sql = f"""
        INSERT INTO {schema}.CRM_COMPANY_CONTACTS (
            uuid, contactId, accountId, accountName, crmToErpFlag,
            firstName, lastName, cshmeFlag, email,
            department, country, zipCode
        )
        VALUES (
            :uuid, :contactId, :accountId, :accountName, :crmToErpFlag,
            :firstName, :lastName, :cshmeFlag, :email,
            :department, :country, :zipCode
        )
    """

    batch = [
        {
            "uuid": str(uuid.uuid4()),
            "contactId": c.get("contactId"),
            "accountId": c.get("accountId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
            "firstName": c.get("firstName"),
            "lastName": c.get("lastName"),
            "cshmeFlag": c.get("cshmeFlag"),
            "email": c.get("email"),
            "department": c.get("department"),
            "country": c.get("country"),
            "zipCode": c.get("zipCode"),
        }
        for c in contacts
    ]

    connection.execute(text(sql), batch)
    logger.info("Inserted %d contact record(s)", len(contacts))


def update_contact(connection, schema: str, contacts: List[Dict[str, Any]]):
    """Update existing contact records."""
    sql = f"""
        UPDATE {schema}.CRM_COMPANY_CONTACTS
        SET accountId = :accountId,
            accountName = :accountName,
            crmToErpFlag = :crmToErpFlag,
            firstName = :firstName,
            lastName = :lastName,
            cshmeFlag = :cshmeFlag,
            email = :email,
            department = :department,
            country = :country,
            zipCode = :zipCode
        WHERE contactId = :contactId
    """

    batch = [
        {
            "contactId": c.get("contactId"),
            "accountId": c.get("accountId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
            "firstName": c.get("firstName"),
            "lastName": c.get("lastName"),
            "cshmeFlag": c.get("cshmeFlag"),
            "email": c.get("email"),
            "department": c.get("department"),
            "country": c.get("country"),
            "zipCode": c.get("zipCode"),
        }
        for c in contacts
    ]

    connection.execute(text(sql), batch)
    logger.info("Updated %d contact record(s)", len(contacts))

