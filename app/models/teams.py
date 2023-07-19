from dataclasses import dataclass
import uuid
from sqlalchemy import text
from app.database import get_db
from app.models.use_case import UseCase


@dataclass
class Teams:
    """
    A class used to represent the Teams model.

    ...

    Attributes
    ----------
    name : str
        The name of the team
    slack_channel : str
        The Slack channel associated with the team
    team_uuid : uuid.UUID
        The unique identifier for the team

    Methods
    -------
    load_all_teams():
        Class method to load all team data from the database and return a list of Team instances.
    save():
        Instance method to save the Team instance to the database.
    """

    name: str
    slack_channel: str
    team_uuid: uuid.UUID

    @classmethod
    def load_all_teams(cls):
        """
        Load all teams from the database and return a list of Team instances.

        This method connects to the database, executes a SQL query to get all teams, and then
        creates a new Team instance for each team. It finally returns a list of all these instances.

        Returns
        -------
        list
            A list of Team instances representing all teams in the database.
        """
        conn = get_db()
        query = conn.execute(
            text(
                """  
                SELECT name, slack_channel, team_uuid  
                FROM project_management.teams 
                """
            )
        )
        teams_data = query.fetchall()

        teams = [
            Teams(
                name=team_item.name,
                slack_channel=team_item.slack_channel,
                team_uuid=team_item.team_uuid,
            )
            for team_item in teams_data
        ]
        return teams

    def save(self):
        """
        Save the Team instance into the database.

        This method connects to the database and executes an SQL INSERT query to store the team data.
        It uses the team's name, Slack channel, and UUID as values for the query parameters.

        Returns
        -------
        None
        """
        conn = get_db()
        query = conn.execute(
            text(
                """
                Insert into project_management.teams
                (name, slack_channel, uuid)
                values
                (:name, :slack_channel, :team_uuid)
                """
            ),
            {
                "name": self.name,
                "slack_channel": self.slack_channel,
                "uuid": self.team_uuid,
            },
        )


def get_teams_by_use_case(use_case_uuid: uuid.UUID):
    """
    Retrieve teams associated with a specific use case from the database.

    This function connects to the database, executes a SQL SELECT query to get all teams
    associated with a particular use case and then creates a new Team instance for each team.
    It finally returns a list of these instances.

    Parameters
    ----------
    use_case_uuid : uuid.UUID
        The unique identifier for the use case.

    Returns
    -------
    list
        A list of Team instances representing all teams associated with the given use case.
    """
    conn = get_db()
    query = conn.execute(
        text(
            """
            SELECT t.*
            FROM project_management.teams t
            JOIN project_management.use_case_teams_link link 
            ON t.team_uuid = link.team_uuid
            WHERE link.use_case_uuid = :use_case_uuid
            """
        ),
        {"use_case_uuid": use_case_uuid},
    )
    team_data = query.fetchall()
    teams_list = [
        Teams(
            name=team_item.name,
            slack_channel=team_item.slack_channel,
            team_uuid=team_item.team_uuid,
        )
        for team_item in team_data
    ]

    return teams_list


def get_use_case_by_team(team_uuid: uuid.UUID):
    """
    Retrieve use cases associated with a specific team from the database.

    This function connects to the database and executes a SQL SELECT query to get all use cases
    associated with a particular team. It then loads each use case by its UUID and finally returns
    a list of these use case instances.

    Parameters
    ----------
    team_uuid : uuid.UUID
        The unique identifier for the team.

    Returns
    -------
    list
        A list of UseCase instances representing all use cases associated with the given team.
    """
    conn = get_db()
    query = conn.execute(
        text(
            """
            SELECT link.use_case_uuid
            FROM project_management.use_case_teams_link link
            WHERE link.team_uuid = :team_uuid
            """
        ),
        {"team_uuid": team_uuid},
    )
    use_case_uuid_results = query.fetchall()
    use_case_list = []
    for (use_case_uuid,) in use_case_uuid_results:
        use_case_list.append(UseCase.load_use_case_by_uuid(use_case_uuid))
    return use_case_list


def set_up_use_case_teams_link(use_case_uuid: uuid.UUID, team_uuid: uuid.UUID):
    """
    Set up a link between a use case and a team in the database.

    This function connects to the database and executes a SQL INSERT query to create a link
    between a use case and a team.

    Parameters
    ----------
    use_case_uuid : uuid.UUID
        The unique identifier for the use case.
    team_uuid : uuid.UUID
        The unique identifier for the team.

    Returns
    -------
    None
    """
    conn = get_db()
    query = conn.execute(
        text(
            """
            INSERT INTO project_management.use_case_teams_link
            (use_case_uuid, team_uuid)
            VALUES 
            (:use_case_uuid, :team_uuid)
            """
        ),
        {"use_case_uuid": use_case_uuid, "team_uuid": team_uuid},
    )


def delete_all_teams_for_a_use_case(use_case_uuid: uuid.UUID):
    """
    Delete all links between teams and a specific use case in the database.

    This function connects to the database and executes a SQL DELETE query to remove all links
    between teams and a specific use case from the project_management.use_case_teams_link table.
    After the execution of the DELETE query, it commits the transaction to ensure the changes
    are saved in the database.

    Parameters
    ----------
    use_case_uuid : uuid.UUID
        The unique identifier for the use case.

    Returns
    -------
    None
    """
    # Delete all rows associated with the given use case from project_management.use_case_teams_link table
    conn = get_db()
    conn.execute(
        text(
            """
            DELETE FROM project_management.use_case_teams_link
            WHERE use_case_uuid = :use_case_uuid
            """
        ),
        {"use_case_uuid": use_case_uuid},
    )

    conn.execute(text("commit"))
