from dataclasses import dataclass
import uuid
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from app.database import get_db
from typing import List, Optional
from werkzeug.exceptions import BadRequest, NotFound


@dataclass
class UseCase:
    uuid: uuid.UUID
    name: str
    description: str
    point_of_contact: str
    status: str
    jira_ticket: str
    point_of_contact_email: str

    @classmethod
    def load_all_use_cases(cls) -> List["UseCase"]:
        conn = get_db()
        query = conn.execute(
            text(
                """  
                SELECT uuid, name, description, point_of_contact, status, jira_ticket, point_of_contact_email  
                FROM project_management.use_case  
                """
            )
        )
        use_cases_data = query.fetchall()

        use_cases = [UseCase(*use_case_data) for use_case_data in use_cases_data]
        return use_cases

    @classmethod
    def load_use_case_by_uuid(cls, use_case_uuid: uuid.UUID) -> Optional["UseCase"]:
        conn = get_db()
        query = conn.execute(
            text(
                """  
                SELECT uuid, name, description, point_of_contact, status, jira_ticket, point_of_contact_email  
                FROM project_management.use_case  
                WHERE uuid = :uuid  
                """
            ),
            {"uuid": use_case_uuid},
        )
        use_case_data = query.fetchone()

        if use_case_data:
            return UseCase(*use_case_data)
        else:
            return None

    @classmethod
    def create_use_case(cls, use_case: "UseCase") -> None:
        conn = get_db()
        query = conn.execute(
            text(
                """  
                INSERT INTO project_management.use_case  
                    (uuid, name, description, point_of_contact, status, jira_ticket, point_of_contact_email)  
                VALUES  
                    (:uuid, :name, :description, :point_of_contact, :status, :jira_ticket, :point_of_contact_email)  
                """
            ),
            {
                "uuid": use_case.uuid,
                "name": use_case.name,
                "description": use_case.description,
                "point_of_contact": use_case.point_of_contact,
                "status": use_case.status,
                "jira_ticket": use_case.jira_ticket,
                "point_of_contact_email": use_case.point_of_contact_email,
            },
        )
        conn.commit()


def value_set_use_case_link_set_up(use_case_data, vs_uuid):
    # Insert the value_set and use_case associations into the value_sets.value_set_use_case_link table
    conn = get_db()
    if use_case_data is None:
        use_case_data = []

    for use_case_dict in use_case_data:
        use_case_uuid = use_case_dict.get("use_case_uuid")
        is_primary = use_case_dict.get("is_primary", False)
        conn.execute(
            text(
                """      
                INSERT INTO value_sets.value_set_use_case_link      
                (value_set_uuid, use_case_uuid, is_primary)      
                VALUES      
                (:value_set_uuid, :use_case_uuid, :is_primary)      
                """
            ),
            {
                "value_set_uuid": vs_uuid,
                "use_case_uuid": use_case_uuid,
                "is_primary": is_primary,
            },
        )
    conn.execute(text("commit"))


def load_use_case_by_value_set_uuid(
    value_set_uuid: uuid.UUID,
) -> Optional[List[UseCase]]:
    """
    This function is used to fetch use case data associated with a specific value set based on its universally unique identifier (UUID).

    Args:
    value_set_uuid (uuid.UUID): The UUID of the value set for which use cases are to be fetched.

    Returns:
    Optional[List[UseCase]]: Returns a list of UseCase objects containing the details of each use case linked to the provided value set UUID.
    If no use cases are found for the provided UUID, returns None.

    Raises:
    SQLAlchemyError: An error occurred while executing the SQL query.
    """
    conn = get_db()
    query = conn.execute(
        text(
            """    
            SELECT uc.uuid, uc.name, uc.description, uc.point_of_contact, uc.status, uc.jira_ticket, uc.point_of_contact_email
            FROM project_management.use_case uc   
            INNER JOIN value_sets.value_set_use_case_link link ON uc.uuid = link.use_case_uuid    
            WHERE link.value_set_uuid = :value_set_uuid    
            """
        ),
        {"value_set_uuid": value_set_uuid},
    )
    use_case_data_list = query.fetchall()

    if use_case_data_list:
        return [UseCase(*use_case_data) for use_case_data in use_case_data_list]
    else:
        return None


def remove_is_primary_status(
    use_case_uuid: uuid.UUID, value_set_uuid: uuid.UUID
) -> None:
    conn = get_db()
    query = conn.execute(
        text(
            """  
            UPDATE value_sets.value_set_use_case_link  
            SET is_primary = false  
            WHERE use_case_uuid = :use_case_uuid AND value_set_uuid = :value_set_uuid  
            """
        ),
        {"use_case_uuid": use_case_uuid, "value_set_uuid": value_set_uuid},
    )
    conn.commit()


def remove_use_case_from_value_set(
    use_case_uuid: uuid.UUID, value_set_uuid: uuid.UUID
) -> None:
    conn = get_db()
    result = conn.execute(
        text(
            """  
            select is_primary from value_sets.value_set_use_case_link  
            where use_case_uuid = :use_case_uuid  
            and value_set_uuid = :value_set_uuid  
            """
        ),
        {"use_case_uuid": use_case_uuid, "value_set_uuid": value_set_uuid},
    )
    row = result.fetchone()
    if row and row["is_primary"]:
        raise BadRequest(
            "This use case case is not eligable for deletion because it is the primary use case."
        )
    else:
        query = conn.execute(
            text(
                """    
                DELETE FROM value_sets.value_set_use_case_link    
                WHERE use_case_uuid = :use_case_uuid AND value_set_uuid = :value_set_uuid    
                """
            ),
            {"use_case_uuid": use_case_uuid, "value_set_uuid": value_set_uuid},
        )
        conn.commit()


def delete_all_use_cases_for_value_set(value_set_uuid: uuid.UUID) -> None:
    conn = get_db()

    # Delete all rows associated with the given value_set_uuid from the value_set_use_case_link table
    conn.execute(
        text(
            """  
            DELETE FROM value_sets.value_set_use_case_link  
            WHERE value_set_uuid = :value_set_uuid  
            """
        ),
        {"value_set_uuid": value_set_uuid},
    )

    conn.execute(text("commit"))
