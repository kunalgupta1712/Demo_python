import os
import logging
import uuid
from typing import List, Dict, Any
from db_connection import get_hana_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def insert_or_update_users_bulk(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update users in bulk into the staging.PUser table.
    """
    schema = os.getenv("HANA_SCHEMA")
    if not schema:
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    try:
        with engine.begin() as connection:
            # Fetch existing userIDs
            user_ids = [u["userID"] for u in users if "userID" in u]
            existing = get_existing_users(connection, schema, user_ids)

            # Split into new and existing users
            new_users = [u for u in users if u["userID"] not in existing]
            existing_users = [u for u in users if u["userID"] in existing]

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
    Return list of userIDs that already exist in staging.PUser.
    """
    if not user_ids:
        return []

    placeholders = ", ".join([f":id_{i}" for i in range(len(user_ids))])
    query = f"SELECT userID FROM {schema}.PUSER WHERE userID IN ({placeholders})"
    params = {f"id_{i}": val for i, val in enumerate(user_ids)}

    result = connection.execute(query, params)
    return [row[0] for row in result.fetchall()]


def insert_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Insert new users into staging.PUser.
    Auto-generates userUuid (UUID v4).
    """
    sql = f"""
        INSERT INTO {schema}.PUSER (
            userID, firstName, lastName, displayName, email, phoneNumber, country,
            zip, userUuid, userName, status, userType
        )
        VALUES (
            :userID, :firstName, :lastName, :displayName, :email, :phoneNumber, :country,
            :zip, :userUuid, :userName, :status, :userType
        )
    """

    batch = []
    for u in users:
        batch.append({
            "userID": u.get("userID"),
            "firstName": u.get("firstName"),
            "lastName": u.get("lastName"),
            "displayName": u.get("displayName"),
            "email": u.get("email"),
            "phoneNumber": u.get("phoneNumber"),
            "country": u.get("country"),
            "zip": u.get("zip"),
            "userUuid": str(uuid.uuid4()),   # auto-generate UUID
            "userName": u.get("userName"),
            "status": u.get("status"),
            "userType": u.get("userType")
        })

    connection.execute(sql, batch)
    logger.info(f"Inserted {len(users)} new users")


def update_users_bulk(connection, schema: str, users: List[Dict[str, Any]]):
    """
    Update existing users in staging.PUser.
    """
    sql = f"""
        UPDATE {schema}.PUSER
        SET firstName = :firstName,
            lastName = :lastName,
            displayName = :displayName,
            email = :email,
            phoneNumber = :phoneNumber,
            country = :country,
            zip = :zip,
            userName = :userName,
            status = :status,
            userType = :userType
        WHERE userID = :userID
    """

    batch = []
    for u in users:
        batch.append({
            "userID": u.get("userID"),
            "firstName": u.get("firstName"),
            "lastName": u.get("lastName"),
            "displayName": u.get("displayName"),
            "email": u.get("email"),
            "phoneNumber": u.get("phoneNumber"),
            "country": u.get("country"),
            "zip": u.get("zip"),
            "userName": u.get("userName"),
            "status": u.get("status"),
            "userType": u.get("userType")
        })

    connection.execute(sql, batch)
    logger.info(f"Updated {len(users)} existing users")
