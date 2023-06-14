from dataclasses import dataclass
import uuid
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from app.database import get_db
from typing import List, Optional, Tuple
from werkzeug.exceptions import BadRequest, NotFound


@dataclass
class Teams:
    name: str
    slack_channel: str
    team_uuid: uuid.UUID

    def __post_init__(self):
        self.conn = get_db()
        self.team_uuid = uuid.uuid4()

    @classmethod
    def load_all_teams(cls):
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
        self.conn.execute(
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


def get_team_names_by_use_case(use_case_uuid):
    """
    Get all team names associated with a specific use case from the database.

    @param use_case_uuid: UUID of the use case
    :type use_case_uuid: str
    @return: List of team names associated with the use case
    :rtype: list
    """
    conn = get_db()
    query = conn.execute(
        text(
            """
            SELECT t.name 
            FROM project_management.teams t
            JOIN project_management.use_case_teams_link link 
            ON t.team_uuid = link.team_uuid
            WHERE link.use_case_uuid = :use_case_uuid
            """
        ),
        {"use_case_uuid": use_case_uuid},
    )
    teams_list = [row[0] for row in query.fetchall()]

    return teams_list


def get_use_case_names_by_team(team_uuid):
    """
    Get all use case names associated with a specific team from the database.

    @param team_uuid: UUID of the team
    :type team_uuid: str
    @return: List of use case names associated with the team
    :rtype: list
    """
    conn = get_db()
    query = conn.execute(
        text(
            """
            SELECT uc.name
            FROM project_management.use_case uc
            JOIN project_management.use_case_teams_link link
            ON uc.uuid = link.use_case_uuid
            WHERE link.team_uuid = :team_uuid
            """
        ),
        {"team_uuid": team_uuid},
    )
    use_case_list = [row[0] for row in query.fetchall()]

    return use_case_list


def delete_all_teams_for_a_use_case(use_case_uuid):
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
