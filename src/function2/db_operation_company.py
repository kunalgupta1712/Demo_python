import os
import uuid
import logging
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client
from erp_customer_registration import register_company_as_customer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def insert_or_update_company(companies: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update companies into CRM_COMPANY_ACCOUNTS table.
    If crmToErpFlag is True (new or changed), register in ERP_CUSTOMERS.
    """

    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    inserted_count = 0
    updated_count = 0
    failed = []

    with engine.begin() as connection:
        for company in companies:
            account_id = company.get("accountId")
            account_name = company.get("accountName")
            crm_to_erp_flag = company.get("crmToErpFlag")

            if not account_id or not account_name:
                logger.warning("Skipping invalid company entry: %s", company)
                failed.append({"company": company, "error": "Missing mandatory fields"})
                continue

            try:
                # Check existing company
                existing_query = text(
                    f"SELECT crmToErpFlag FROM {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS "
                    f"WHERE accountId = :accountId"
                )
                result = connection.execute(existing_query, {"accountId": account_id}).fetchone()

                if result:
                    existing_flag = result[0]
                    update_query = text(f"""
                        UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS
                        SET accountName = :accountName,
                            crmToErpFlag = :crmToErpFlag
                        WHERE accountId = :accountId
                    """)
                    connection.execute(
                        update_query,
                        {
                            "accountName": account_name,
                            "crmToErpFlag": crm_to_erp_flag,
                            "accountId": account_id,
                        },
                    )
                    updated_count += 1

                    # If flag changed from False → True, trigger registration
                    if not existing_flag and crm_to_erp_flag:
                        register_company_as_customer(account_id, account_name)

                else:
                    insert_query = text(f"""
                        INSERT INTO {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS (
                            uuid, accountId, accountName, crmToErpFlag
                        )
                        VALUES (
                            :uuid, :accountId, :accountName, :crmToErpFlag
                        )
                    """)
                    connection.execute(
                        insert_query,
                        {
                            "uuid": str(uuid.uuid4()),
                            "accountId": account_id,
                            "accountName": account_name,
                            "crmToErpFlag": crm_to_erp_flag,
                        },
                    )
                    inserted_count += 1

                    # If crmToErpFlag is True on insert → register
                    if crm_to_erp_flag:
                        register_company_as_customer(account_id, account_name)

            except Exception as e:
                logger.exception("Error processing company %s: %s", account_id, e)
                failed.append({"company": company, "error": str(e)})

    logger.info(
        "Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count, updated_count, len(failed)
    )

    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed,
    }
