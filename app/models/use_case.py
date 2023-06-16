from dataclasses import dataclass
import uuid
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from app.database import get_db
from typing import List, Optional, Tuple
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

        use_cases = [
            UseCase(
                uuid=use_case_item.uuid,
                name=use_case_item.name,
                description=use_case_item.description,
                point_of_contact=use_case_item.point_of_contact,
                status=use_case_item.status,
                jira_ticket=use_case_item.jira_ticket,
                point_of_contact_email=use_case_item.point_of_contact_email,
            )
            for use_case_item in use_cases_data
        ]
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
    def save(
        cls,
        use_case: "UseCase",
        value_set_uuid: Optional[uuid.UUID] = None,
        is_primary: Optional[bool] = False,
    ) -> None:
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

        if value_set_uuid and is_primary is not None:
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
                    "value_set_uuid": value_set_uuid,
                    "use_case_uuid": use_case.uuid,
                    "is_primary": is_primary,
                },
            )
            conn.commit()


def load_use_case_by_value_set_uuid(
    value_set_uuid: uuid.UUID,
) -> Tuple[UseCase, List[UseCase]]:
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
            and link.is_primary is true
            """
        ),
        {"value_set_uuid": value_set_uuid},
    )
    primary_use_case_data = query.first()
    if primary_use_case_data is not None:
        primary_use_case = UseCase(
            uuid=primary_use_case_data.uuid,
            name=primary_use_case_data.name,
            description=primary_use_case_data.description,
            point_of_contact=primary_use_case_data.point_of_contact,
            status=primary_use_case_data.status,
            jira_ticket=primary_use_case_data.jira_ticket,
            point_of_contact_email=primary_use_case_data.point_of_contact_email,
        )
    else:
        primary_use_case = None

    query = conn.execute(
        text(
            """    
            SELECT uc.uuid, uc.name, uc.description, uc.point_of_contact, uc.status, uc.jira_ticket, uc.point_of_contact_email
            FROM project_management.use_case uc   
            INNER JOIN value_sets.value_set_use_case_link link ON uc.uuid = link.use_case_uuid    
            WHERE link.value_set_uuid = :value_set_uuid 
            and link.is_primary is false
            """
        ),
        {"value_set_uuid": value_set_uuid},
    )
    secondary_use_cases_data = query.fetchall()
    secondary_use_cases = [
        UseCase(
            uuid=item.uuid,
            name=item.name,
            description=item.description,
            point_of_contact=item.point_of_contact,
            status=item.status,
            jira_ticket=item.jira_ticket,
            point_of_contact_email=item.point_of_contact_email,
        )
        for item in secondary_use_cases_data
    ]

    return primary_use_case, secondary_use_cases


def delete_all_use_cases_for_value_set(value_set_uuid: uuid.UUID) -> None:
    # Delete all rows associated with the given value_set_uuid from the value_set_use_case_link table
    conn = get_db()
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
