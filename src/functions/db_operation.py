import os
import logging
import uuid
from typing import List, Dict, Any
from sqlalchemy import text
from db_connection import get_hana_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def insert_or_update_users_bulk(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update users in bulk into the SPUSER_STAGING_P_USERS table.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    try:
        with engine.begin() as connection:
            # Fetch existing userIds
            user_ids = [u["userId"] for u in users if "userId" in u]
            existing = get_existing_users(connection, schema, user_ids)

            # Split into new and existing users
            new_users = [u for u in users if u["userId"] not in existing]
            existing_users = [u for u in users if u["userId"] in existing]

            if new_users:
                insert_users_bulk(connection, schema, new_users)

            if existing_users:
                update_users_bulk(connection, schema, existing_users)

            logger.info(f"Inserted: {len(new_users)}, Updated: {len(existing_users)}")
            return {"inserted": len(new_users), "updated": len(existing_users)}

    except Exception as e:
        logger.exception("Error in bulk insert/update")
        raise e


def get_existing_users(connection, schema: str, user_ids: List[str]) -> List[str]:
    """
    Return list of userIds that already exist in SPUSER_STAGING_P_USERS.
    """
    if not user_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(user_ids))])
    query = f"SELECT userId FROM {schema}.SPUSER_STAGING_P_USERS WHERE userId IN ({placeholders})"
    params = {f"id_{i}": val for i, val in enumerate(user_ids)}

    result = connection.execute(text(query), params)
    return [row[0] for row in result.fetchall()]


def insert_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Insert new users into SPUSER_STAGING_P_USERS.
    Auto-generates userUuid (UUID v4).
    """
    sql = f"""
        INSERT INTO {schema}.SPUSER_STAGING_P_USERS (
            userUuid, userId, firstName, lastName, displayName, email,
            phoneNumber, country, zip, userName, status, userType,
            mailVerified, phoneVerified, created, lastModified, modifiedBy
        )
        VALUES (
            :userUuid, :userId, :firstName, :lastName, :displayName, :email,
            :phoneNumber, :country, :zip, :userName, :status, :userType,
            :mailVerified, :phoneVerified, :created, :lastModified, :modifiedBy
        )
    """

    batch = []
    for u in users:
        batch.append({
            "userUuid": str(uuid.uuid4()),
            "userId": u.get("userId"),
            "firstName": u.get("firstName"),
            "lastName": u.get("lastName"),
            "displayName": u.get("displayName"),
            "email": u.get("email"),
            "phoneNumber": u.get("phoneNumber"),
            "country": u.get("country"),
            "zip": u.get("zip"),
            "userName": u.get("userName"),
            "status": u.get("status"),
            "userType": u.get("userType"),
            "mailVerified": u.get("mailVerified"),
            "phoneVerified": u.get("phoneVerified"),
            "created": u.get("created"),
            "lastModified": u.get("lastModified"),
            "modifiedBy": u.get("modifiedBy")
        })

    connection.execute(text(sql), batch)
    logger.info(f"Inserted {len(users)} new users")


def update_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Update existing users in SPUSER_STAGING_P_USERS.
    """
    sql = f"""
        UPDATE {schema}.SPUSER_STAGING_P_USERS
        SET firstName = :firstName,
            lastName = :lastName,
            displayName = :displayName,
            email = :email,
            phoneNumber = :phoneNumber,
            country = :country,
            zip = :zip,
            userName = :userName,
            status = :status,
            userType = :userType,
            mailVerified = :mailVerified,
            phoneVerified = :phoneVerified,
            lastModified = :lastModified,
            modifiedBy = :modifiedBy
        WHERE userId = :userId
    """

    batch = []
    for u in users:
        batch.append({
            "userId": u.get("userId"),
            "firstName": u.get("firstName"),
            "lastName": u.get("lastName"),
            "displayName": u.get("displayName"),
            "email": u.get("email"),
            "phoneNumber": u.get("phoneNumber"),
            "country": u.get("country"),
            "zip": u.get("zip"),
            "userName": u.get("userName"),
            "status": u.get("status"),
            "userType": u.get("userType"),
            "mailVerified": u.get("mailVerified"),
            "phoneVerified": u.get("phoneVerified"),
            "lastModified": u.get("lastModified"),
            "modifiedBy": u.get("modifiedBy")
        })

    connection.execute(text(sql), batch)
    logger.info(f"Updated {len(users)} existing users")
