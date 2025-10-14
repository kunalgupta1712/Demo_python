import os
import logging
import uuid
from typing import List, Dict, Any
from sqlalchemy import text
from .db_connection import get_hana_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_or_update_users_bulk(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update users into SPUSER_STAGING_P_USERS table.
    Handles each user independently: errors for one user do not block others.
    Returns a summary dict: inserted count, updated count, failed userIds.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    inserted_count = 0
    updated_count = 0
    failed_users = []

    with engine.begin() as connection:
        for u in users:
            user_id = u.get("userId")
            if not user_id:
                logger.warning("Skipping user with missing userId: %s", u)
                failed_users.append({"user": u, "error": "Missing userId"})
                continue

            try:
                # Check if user exists
                existing = get_existing_users(connection, schema, [user_id])
                if not existing:
                    # Insert single user
                    insert_users_bulk(connection, schema, [u])
                    inserted_count += 1
                else:
                    # Update single user
                    update_users_bulk(connection, schema, [u])
                    updated_count += 1

            except Exception as e:
                logger.exception(
                    "Failed to insert/update userId=%s: %s",
                    user_id,
                    e,
                )
                failed_users.append({"user": u, "error": str(e)})

    logger.info(
        "Insert/Update Summary: inserted=%d, updated=%d, failed=%d",
        inserted_count,
        updated_count,
        len(failed_users),
    )
    return {
        "inserted": inserted_count,
        "updated": updated_count,
        "failed": failed_users,
    }


def get_existing_users(connection, schema: str, user_ids: List[str]) -> List[str]:
    """
    Return list of userIds that already exist in SPUSER_STAGING_P_USERS.
    """
    if not user_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(user_ids))])
    query = (
        f"SELECT userId FROM {schema}.SPUSER_STAGING_P_USERS "
        f"WHERE userId IN ({placeholders})"
    )

    params = {f"id_{i}": val for i, val in enumerate(user_ids)}

    result = connection.execute(text(query), params)
    return [row[0] for row in result.fetchall()]


def insert_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Insert new users into SPUSER_STAGING_P_USERS.
    Handles a list of users (usually one in the new per-user loop).
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
        batch.append(
            {
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
                "modifiedBy": u.get("modifiedBy"),
            }
        )

    connection.execute(text(sql), batch)
    logger.info("Inserted %d user(s)", len(users))


def update_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Update existing users in SPUSER_STAGING_P_USERS.
    Handles a list of users (usually one in the new per-user loop).
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
        batch.append(
            {
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
                "modifiedBy": u.get("modifiedBy"),
            }
        )

    connection.execute(text(sql), batch)
    logger.info("Updated %d user(s)", len(users))
