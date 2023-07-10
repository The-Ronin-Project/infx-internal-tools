from dataclasses import dataclass
import uuid
from sqlalchemy import text
from app.database import get_db
from typing import List, Optional, Tuple


@dataclass
class UseCase:
    """
    A class used to represent the UseCase model.

    ...

    Attributes
    ----------
    uuid : uuid.UUID
        The unique identifier for the use case
    name : str
        The name of the use case
    description : str
        The description of the use case
    point_of_contact : str
        The point of contact for the use case
    status : str
        The status of the use case
    jira_ticket : str
        The Jira ticket associated with the use case
    point_of_contact_email : str
        The email of the point of contact for the use case
    """

    uuid: uuid.UUID
    name: str
    description: str
    point_of_contact: str
    status: str
    jira_ticket: str
    point_of_contact_email: str

    @classmethod
    def load_all_use_cases(cls) -> List["UseCase"]:
        """
        Class method to load all use case data from the database.

        This method retrieves all rows from the `project_management.use_case` table, creates
        UseCase instances from each row, and returns a list of these instances.

        Returns
        -------
        List[UseCase]
            A list of UseCase instances representing all use cases in the database.
        """

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
        """
        Class method to load a specific use case by its UUID from the database.

        This method retrieves a row identified by the provided UUID from the `project_management.use_case` table,
        creates a UseCase instance from the row if it exists, and returns it.

        Parameters
        ----------
        use_case_uuid : uuid.UUID
            The unique identifier of the use case to load.

        Returns
        -------
        Optional[UseCase]
            A UseCase instance representing the loaded use case if it exists, otherwise None.
        """
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
            return UseCase(
                uuid=use_case_data.uuid,
                name=use_case_data.name,
                description=use_case_data.description,
                point_of_contact=use_case_data.point_of_contact,
                status=use_case_data.status,
                jira_ticket=use_case_data.jira_ticket,
                point_of_contact_email=use_case_data.point_of_contact_email,
            )
        else:
            return None

    @classmethod
    def save(
        cls,
        use_case: "UseCase",
    ):
        """
        Class method to save a UseCase instance to the database.

        This method inserts a new row to the `project_management.use_case` table with the values from the provided
        UseCase instance.

        Parameters
        ----------
        use_case : UseCase
            The UseCase instance to save.

        Returns
        -------
        None
        """
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

    @classmethod
    def save_value_set_link(
        cls,
        use_case: "UseCase",
        value_set_uuid: uuid.UUID,
        is_primary: Optional[bool] = False,
    ) -> None:
        """
        Class method to create a link between a use case and a value set in the database.

        This method inserts a new row to the `value_sets.value_set_use_case_link` table with the provided values.

        Parameters
        ----------
        use_case : UseCase
            The UseCase instance to link.
        value_set_uuid : uuid.UUID
            The UUID of the value set to link.
        is_primary : bool, optional
            Whether the value set is primary for the use case, by default False.

        Returns
        -------
        None
        """
        conn = get_db()
        query = conn.execute(
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
                "use_case_uuid": use_case["uuid"],
                "is_primary": is_primary,
            },
        )


def load_use_case_by_value_set_uuid(
    value_set_uuid: uuid.UUID,
) -> Tuple[UseCase, List[UseCase]]:
    """
    Retrieve the primary and secondary use cases associated with a specific value set from the database.

    This function connects to the database, executes two SQL SELECT queries to get the primary and secondary use cases
    associated with a particular value set, based on the value set UUID. It then creates a new UseCase instance for each use case.
    It finally returns a dictionary with the primary use case instance and a list of secondary use case instances.

    Parameters
    ----------
    value_set_uuid : uuid.UUID
        The unique identifier for the value set.

    Returns
    -------
    dict
        A dictionary containing the primary use case instance under the "Primary Use Case" key,
        and a list of UseCase instances representing all secondary use cases under the "Secondary Use Case(s)" key.
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

    return {
        "primary_use_case": primary_use_case,
        "secondary_use_cases": secondary_use_cases,
    }


def delete_all_use_cases_for_value_set(value_set_uuid: uuid.UUID) -> None:
    """
    Deletes all the use cases associated with a specific value set from the database.

    This function connects to the database and executes a SQL DELETE query to remove all use cases
    associated with the specified value set, based on the value set UUID. The changes are then committed
    to the database. This function does not return anything.

    Parameters
    ----------
    value_set_uuid : uuid.UUID
        The unique identifier for the value set whose associated use cases are to be deleted.
    """
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
