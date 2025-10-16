import os
import logging
import uuid
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_or_update_company(
    companies: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Insert or update records in CRM_COMPANY_ACCOUNTS table."""
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    inserted_count = 0
    updated_count = 0
    failed_records = []

    with engine.begin() as connection:
        for company in companies:
            account_id = company.get("accountId")
            if not account_id:
                logger.warning(
                    "Skipping record with missing accountId: %s", company
                )
                failed_records.append(
                    {"record": company, "error": "Missing accountId"}
                )
                continue

            try:
                existing = get_existing_accounts(
                    connection, schema, [account_id]
                )
                if not existing:
                    insert_company(connection, schema, [company])
                    inserted_count += 1
                else:
                    update_company(connection, schema, [company])
                    updated_count += 1
            except Exception as e:
                logger.exception(
                    "Failed to insert/update accountId=%s: %s",
                    account_id,
                    e,
                )
                failed_records.append(
                    {"record": company, "error": str(e)}
                )

    logger.info(
        "Company Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count,
        updated_count,
        len(failed_records),
    )
    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed_records,
    }


def get_existing_accounts(connection, schema: str, account_ids: List[int]):
    """Return list of existing accountIds."""
    if not account_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(account_ids))])
    query = (
        f"SELECT accountId FROM {schema}.CRM_COMPANY_ACCOUNTS "
        f"WHERE accountId IN ({placeholders})"
    )
    params = {f"id_{i}": val for i, val in enumerate(account_ids)}
    result = connection.execute(text(query), params)
    return [row[0] for row in result.fetchall()]


def insert_company(connection, schema: str, companies: List[Dict[str, Any]]):
    """Insert new company records."""
    sql = f"""
        INSERT INTO {schema}.CRM_COMPANY_ACCOUNTS (
            uuid, accountId, accountName, crmToErpFlag
        )
        VALUES (:uuid, :accountId, :accountName, :crmToErpFlag)
    """

    batch = [
        {
            "uuid": str(uuid.uuid4()),
            "accountId": c.get("accountId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
        }
        for c in companies
    ]

    connection.execute(text(sql), batch)
    logger.info("Inserted %d company record(s)", len(companies))


def update_company(connection, schema: str, companies: List[Dict[str, Any]]):
    """Update existing company records."""
    sql = f"""
        UPDATE {schema}.CRM_COMPANY_ACCOUNTS
        SET accountName = :accountName,
            crmToErpFlag = :crmToErpFlag
        WHERE accountId = :accountId
    """

    batch = [
        {
            "accountId": c.get("accountId"),
            "accountName": c.get("accountName"),
            "crmToErpFlag": c.get("crmToErpFlag"),
        }
        for c in companies
    ]

    connection.execute(text(sql), batch)
    logger.info("Updated %d company record(s)", len(companies))

