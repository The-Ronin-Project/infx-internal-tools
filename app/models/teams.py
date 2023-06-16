from dataclasses import dataclass
import uuid
from sqlalchemy import text
from app.database import get_db
from app.models.use_case import UseCase


@dataclass
class Teams:
    name: str
    slack_channel: str
    team_uuid: uuid.UUID

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


def get_teams_by_use_case(use_case_uuid):
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


def get_use_case_by_team(team_uuid):
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
            SELECT link.use_case_uuid
            FROM project_management.use_case_teams_link link
            WHERE link.team_uuid = :team_uuid
            """
        ),
        {"team_uuid": team_uuid},
    )
    use_case_uuid_results = [query.fetchall()]
    use_case_list = []
    for use_case_uuid in use_case_uuid_results:
        use_case_list.append(UseCase.load_use_case_by_uuid(use_case_uuid))
    return use_case_list


def set_up_use_case_teams_link(use_case_uuid, team_uuid):
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
