from dataclasses import dataclass
import uuid
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from app.database import get_db
from typing import List, Optional


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
    def load_all_use_cases(cls) -> List[UseCase]:
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
    def load_use_case_by_uuid(cls, use_case_uuid: uuid.UUID) -> Optional[UseCase]:
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
    def create_use_case(cls, use_case: UseCase) -> None:
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


def use_case_set_up_on_value_set_creation(use_case_uuids, vs_uuid):
    # Insert the value_set and use_case associations into the value_sets.value_set_use_case_link table
    conn = get_db()
    if use_case_uuids is None:
        use_case_uuids = []

    for use_case_uuid in use_case_uuids:
        conn.execute(
            text(
                """    
                INSERT INTO value_sets.value_set_use_case_link    
                (value_set_uuid, use_case_uuid)    
                VALUES    
                (:value_set_uuid, :use_case_uuid)    
                """
            ),
            {"value_set_uuid": vs_uuid, "use_case_uuid": use_case_uuid},
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
