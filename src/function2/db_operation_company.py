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
    Insert or update companies in CRM_COMPANY_ACCOUNTS.
    - Always propagate all changes to ERP_CUSTOMERS via register_company_as_customer.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("HANA_SCHEMA is not set.")

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
                # Check existing CRM record
                existing_query = text(f"""
                    SELECT accountName, crmToErpFlag, erpNo 
                    FROM {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS
                    WHERE accountId = :accountId
                """)
                existing = connection.execute(existing_query, {"accountId": account_id}).fetchone()

                if existing:
                    existing_name, existing_flag, existing_erp_no = existing

                    # Update CRM record if any changes
                    if existing_name != account_name or existing_flag != crm_to_erp_flag:
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
                    else:
                        # No changes in CRM → still propagate to ERP if flag is True
                        updated_count += 1
                else:
                    # Insert new CRM record
                    insert_query = text(f"""
                        INSERT INTO {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS (
                            uuid, accountId, accountName, crmToErpFlag
                        ) VALUES (:uuid, :accountId, :accountName, :crmToErpFlag)
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
                    existing_erp_no = None  # For new insert, ERP needs registration

                # ✅ Always register/update ERP if crmToErpFlag=True
                if crm_to_erp_flag:
                    customer_id = register_company_as_customer(account_id, account_name)
                    # Update erpNo in CRM
                    update_erp_query = text(f"""
                        UPDATE {schema}.SPUSER_STAGING_CRM_COMPANY_ACCOUNTS
                        SET erpNo = :erpNo
                        WHERE accountId = :accountId
                    """)
                    connection.execute(
                        update_erp_query,
                        {"erpNo": customer_id, "accountId": account_id},
                    )

            except Exception as e:
                logger.exception("Error processing company %s: %s", account_id, e)
                failed.append({"company": company, "error": str(e)})

    logger.info("Company Summary: inserted=%d, updated=%d, failed=%d",
                inserted_count, updated_count, len(failed))
    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed,
    }
