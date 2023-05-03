import os
from jira import JIRA
import openai
import re
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dataclasses import dataclass
from typing import List
from decouple import config

from app.database import get_db
import app.coding_companion.github
import app.coding_companion.openai

# Set up environment variables and API keys
JIRA_URL = config('JIRA_URL')
JIRA_USERNAME = config('JIRA_USERNAME')
JIRA_PASSWORD = config('JIRA_PASSWORD')
OPENAI_API_KEY = config('OPENAI_API_KEY')

# Authenticate with the JIRA API
jira = JIRA(JIRA_URL, basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))
jira_datetime_format = '%Y-%m-%dT%H:%M:%S.%f%z'

# Authenticate with the OpenAI API
# openai.api_type = "azure"
# openai.api_base = "https://oai-ds-test2.openai.azure.com/"
# openai.api_version = "2023-03-15-preview"
# openai.api_key = config("OPENAI_API_KEY")


@dataclass
class Ticket:
    ticket_id: str
    title: str
    description: str
    acceptance_criteria: str
    scope: str
    last_updated: datetime


@dataclass
class Conversation:
    conversation_id: str
    ticket_id: str
    start_time: datetime
    end_time: datetime


@dataclass
class Message:
    message_id: str
    conversation_id: str
    sender: str
    content: str
    timestamp: datetime


def get_coco_enabled_tickets() -> List[Ticket]:
    # Get all the epics associated with the 'INFX' board
    jql = f'issuetype = Epic AND project = "INFX"'
    all_epics = jira.search_issues(jql)

    # Filter epics with the 'coco-enabled' label
    coco_enabled_epics = [epic for epic in all_epics if 'coco-enabled' in epic.fields.labels]

    # Get all the tickets associated with the filtered epics and with a status of "To Do" or "In Progress"
    tickets = []
    for epic in coco_enabled_epics:
        jql = f'"Epic Link" = {epic.key} AND status in ("To Do", "In Progress")'
        issues = jira.search_issues(jql)
        for issue in issues:
            ticket = Ticket(
                ticket_id=issue.key,
                title=issue.fields.summary,
                description=issue.fields.description,
                acceptance_criteria=None,  # todo: identify appropriate field from issue.fields
                scope=None,  # todo: identify appropriate field from issue.fields
                last_updated=datetime.strptime(issue.fields.updated, jira_datetime_format)
            )
            tickets.append(ticket)

    return tickets


def get_ticket_by_id(ticket_id: str) -> Ticket:
    issue = jira.issue(ticket_id)
    ticket = Ticket(
        ticket_id=issue.key,
        title=issue.fields.summary,
        description=issue.fields.description,
        acceptance_criteria=None,  # todo: identify appropriate field from issue.fields
        scope=None,  # todo: identify appropriate field from issue.fields
        last_updated=datetime.strptime(issue.fields.updated, jira_datetime_format)
    )
    return ticket


def add_ticket_comment(ticket: Ticket, response: str) -> None:
    jira.add_comment(ticket.ticket_id, response)


def save_chat_thread(ticket: Ticket, conversation: Conversation, messages: List[Message]) -> None:
    with get_db() as session:
        session.execute(text("""  
            INSERT INTO coding_companion.tickets (ticket_id, title, description, acceptance_criteria, scope, last_updated)  
            VALUES (:ticket_id, :title, :description, :acceptance_criteria, :scope, :last_updated)  
            ON CONFLICT (ticket_id) DO UPDATE SET  
                title = EXCLUDED.title,  
                description = EXCLUDED.description,  
                acceptance_criteria = EXCLUDED.acceptance_criteria,  
                scope = EXCLUDED.scope,  
                last_updated = EXCLUDED.last_updated;  
        """), ticket.__dict__)

        session.execute(text("""  
            INSERT INTO coding_companion.conversations (conversation_id, ticket_id, start_time, end_time)  
            VALUES (:conversation_id, :ticket_id, :start_time, :end_time);  
        """), conversation.__dict__)

        for message in messages:
            session.execute(text("""  
                INSERT INTO coding_companion.messages (message_id, conversation_id, sender, content, timestamp)  
                VALUES (:message_id, :conversation_id, :sender, :content, :timestamp);  
            """), message.__dict__)

        session.commit()


# def main():
#     tickets = get_coco_enabled_tickets()
#
#     for ticket in tickets:
#         print(ticket.ticket_id, ticket.title)
#         print("Description:", ticket.description)
#         response = generate_response(ticket, code_context=file_content)
#         print("Response", response)
#         # add_code_comment(ticket, code)
#
#         # TODO: Retrieve conversation and messages from the JIRA ticket comments and save them to the database
#         # conversation, messages = ...
#         # save_chat_thread(ticket, conversation, messages)


if __name__ == "__main__":
    # main()

    # ticket = get_ticket_by_id('INFX-2387')
    ticket = get_ticket_by_id('INFX-2390')
    print(ticket)
    response = app.coding_companion.openai.generate_response(ticket)
    print("Response")
    print(response)

    add_ticket_comment(ticket, response)

    # schema = app.coding_companion.openai.read_schema('concept_maps')
    # print(schema)