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


def load_all_use_cases() -> List[UseCase]:
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


def load_use_case_by_uuid(use_case_uuid: uuid.UUID) -> Optional[UseCase]:
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


def load_use_case_by_value_set_uuid(value_set_uuid: uuid.UUID) -> Optional[UseCase]:
    conn = get_db()
    query = conn.execute(
        text(
            """  
            SELECT u.uuid, u.name, u.description, u.point_of_contact, u.status, u.jira_ticket, u.point_of_contact_email  
            FROM project_management.use_case u  
            INNER JOIN value_sets.value_set v ON u.uuid = v.use_case_uuid  
            WHERE v.uuid = :value_set_uuid  
            """
        ),
        {"value_set_uuid": value_set_uuid},
    )
    use_case_data = query.fetchone()

    if use_case_data:
        return UseCase(*use_case_data)
    else:
        return None


def create_use_case(use_case: UseCase) -> None:
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
