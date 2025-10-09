import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from db_connection import get_hana_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
USER_ID_KEY = "User ID"


def insert_or_update_users_bulk(users: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert or update users in bulk in the HANA database.

    Args:
        users: List of user dictionaries containing S/P data

    Returns:
        Dictionary with counts of inserted and updated records

    Raises:
        Exception: If database operation fails
    """
    if not os.environ.get("HANA_SCHEMA"):
        raise ValueError("Environment variable HANA_SCHEMA is not set.")

    engine = get_hana_client()

    try:
        # Start a transaction
        with engine.begin() as connection:
            # Get existing users based on UserIDs
            existing_users = get_existing_users(
                connection, [user[USER_ID_KEY] for user in users]
            )

            # Separate new and existing users
            new_users = [
                user for user in users if user[USER_ID_KEY] not in existing_users
            ]
            existing_users_to_update = [
                user for user in users if user[USER_ID_KEY] in existing_users
            ]

            # Insert new users
            if new_users:
                insert_users_bulk(connection, new_users)

            # Update existing users
            if existing_users_to_update:
                update_users_bulk(connection, existing_users_to_update)

            return {
                "inserted": len(new_users),
                "updated": len(existing_users_to_update),
            }

    except Exception as error:
        logger.error("Error during bulk insert/update operation: %s", error)
        raise error


def get_existing_users(connection: Any, userids: List[str]) -> List[str]:
    """
    Get existing users based on IDs.

    Args:
        connection: Database connection object
        userids: List of IDs to check

    Returns:
        List of existing IDs
    """
    if not userids:
        return []

    # Using parameterized queries to prevent SQL injection
    schema = os.environ.get("HANA_SCHEMA")
    placeholders = ", ".join([f":id_{i}" for i in range(len(userids))])

    query = f"""
        SELECT userid 
        FROM {schema}.SP_USERS 
        WHERE userid IN ({placeholders})
    """

    # Create parameters dictionary
    params = {f"id_{i}": id for i, id in enumerate(userids)}

    result = connection.execute(query, params)
    return [row[0] for row in result.fetchall()]


def insert_users_bulk(connection: Any, users: List[Dict[str, Any]]) -> None:
    """
    Insert new users in bulk using batch operations.

    Args:
        connection: Database connection object
        users: List of user dictionaries to insert
    """
    if not users:
        return

    schema = os.environ.get("HANA_SCHEMA")

    insert_sql = f"""
        INSERT INTO {schema}.SP_USERS (
            userid, userStatus, companyName, email, phoneNumber, customerNumber, country,
            city, postalCode, street, firstName, lastName, language,
            expiryDate, department, additionalNote
        ) VALUES (
            :userid, :userStatus, :companyName, :email, :phoneNumber, :customerNumber, :country,
            :city, :postalCode, :street, :firstName, :lastName, :language,
            :expiryDate, :department, :additionalNote
        )
    """

    batch_data = []
    for user in users:
        batch_data.append(
            {
                "userid": user[USER_ID_KEY],
                "userStatus": user["Status"],
                "companyName": user["Company Name"],
                "email": user["Email"],
                "phoneNumber": user["Phone Number"],
                "customerNumber": user["Customer Number"],
                "country": user["Country"],
                "city": user["City"],
                "postalCode": user["Postal Code"],
                "street": user["Street"],
                "firstName": user["First Name"],
                "lastName": user["Last Name"],
                "language": user["Language"],
                "expiryDate": format_date(user["Expiry Date"]),
                "department": user["Department"],
                "additionalNote": user["Additional Note"],
            }
        )

    connection.execute(insert_sql, batch_data)
    logger.info("Inserted %d new users", len(users))


def update_users_bulk(connection: Any, users: List[Dict[str, Any]]) -> None:
    """
    Update existing users in bulk using batch operations.

    Args:
        connection: Database connection object
        users: List of user dictionaries to update
    """
    if not users:
        return

    schema = os.environ.get("HANA_SCHEMA")

    update_sql = f"""
        UPDATE {schema}.SP_USERS
        SET userStatus = :userStatus,
            companyName = :companyName,
            email = :email,
            phoneNumber = :phoneNumber,
            customerNumber = :customerNumber,
            country = :country,
            city = :city,
            postalCode = :postalCode,
            street = :street,
            firstName = :firstName,
            lastName = :lastName,
            language = :language,
            expiryDate = :expiryDate,
            department = :department,
            additionalNote = :additionalNote
        WHERE userid = :userid
    """

    batch_data = []
    for user in users:
        batch_data.append(
            {
                "userid": user[USER_ID_KEY],
                "userStatus": user["Status"],
                "companyName": user["Company Name"],
                "email": user["Email"],
                "phoneNumber": user["Phone Number"],
                "customerNumber": user["Customer Number"],
                "country": user["Country"],
                "city": user["City"],
                "postalCode": user["Postal Code"],
                "street": user["Street"],
                "firstName": user["First Name"],
                "lastName": user["Last Name"],
                "language": user["Language"],
                "expiryDate": format_date(user["Expiry Date"]),
                "department": user["Department"],
                "additionalNote": user["Additional Note"],
            }
        )

    connection.execute(update_sql, batch_data)
    logger.info("Updated %d existing users", len(users))


def format_date(date_str: Optional[str]) -> Optional[str]:
    """
    Format date string for database insertion.

    Args:
        date_str: Date string to format

    Returns:
        Formatted date string or None if input is None/invalid
    """
    if not date_str:
        return None

    try:
        return datetime.fromisoformat(date_str).date().isoformat()
    except (ValueError, TypeError):
        return None


def get_custom_field_value(user: Dict[str, Any], field_name: str) -> str:
    """
    Extract custom field values from user object.

    Args:
        user: User dictionary
        field_name: Name of the field to extract

    Returns:
        Field value or empty string if not found
    """
    if field_name in user:
        return user.get(field_name, "") or ""

    if isinstance(user.get("Custom Fields"), list):
        for field in user["Custom Fields"]:
            if field.get("name") == field_name:
                return field.get("value", "") or ""

    return ""


def get_cost_center_code(user: Dict[str, Any]) -> str:
    """
    Get cost center code with fallback.

    Args:
        user: User dictionary

    Returns:
        Cost center code or empty string
    """
    return user.get("Cost Center Code") or user.get("Cost Object Code") or ""


def get_cost_center_name(user: Dict[str, Any]) -> str:
    """
    Get cost center name with fallback.

    Args:
        user: User dictionary

    Returns:
        Cost center name or empty string
    """
    return user.get("Cost Object Name") or user.get("Cost Center Name") or ""
