import re
import os
import requests
import uuid
import json

from sqlalchemy import text
from markdownify import markdownify as md
from bs4 import BeautifulSoup as Soup
from app.database import get_db
from dataclasses import dataclass
from enum import Enum, unique
from decouple import config


@unique
class ResourceType(str, Enum):
    external_link: str = 'external link'
    markdown: str = 'markdown'


@dataclass
class ExternalResource:
    """ related to external resources """
    uuid: uuid
    resource_type: ResourceType
    title: str
    body: str
    version: str
    language: str
    url: str

    @staticmethod
    def locate_ex_resource(language, resource_id):
        """ given the url, locate resource and request data """
        conn = get_db()
        url = config('EXTERNAL_RESOURCE_URL')
        response = requests.get(f"{url}{language}/{resource_id}")
        xml_data = response.text
        xml_soup = Soup(xml_data, 'html.parser')
        version = xml_soup.find('meta', {'name': 'revisedDate'})['content'],
        check_if_resource_exists(version, resource_id)
        md_text_body = md(xml_data, heading_style='ATX')
        resource_type = ResourceType.markdown if 'elsevier' in url else ResourceType.external_link
        body = os.linesep.join([empty_lines for empty_lines in md_text_body.splitlines() if empty_lines])
        _uuid = uuid.uuid1()
        conn.execute(text(
            """
            INSERT INTO patient_education.external_resource_version
            (uuid, version, type, url, title, body, language) 
            VALUES (:uuid, :version, :type, :url, :title, :body, :language, :endpoint);
            """
        ), {
            'url': f"{url}{language}/{resource_id}",
            'language': xml_soup.find('meta', {'name': 'language'})['content'],
            'uuid': _uuid,
            'type': json.dumps(resource_type),
            'version': version,
            'title': xml_soup.title.get_text(),
            'body': body,
            'endpoint': resource_id
        })

        get_inserted_ex_resource = conn.execute(text(
            """
            SELECT * FROM patient_education.external_resource_version WHERE
            uuid=:uuid
            """
        ), {
            'uuid': _uuid
        }).fetchall()
        if not get_inserted_ex_resource:
            return f"Could not return linked data"

        return [dict(row) for row in get_inserted_ex_resource]

    def check_if_resource_exists(self, version, resource_id):
        """ check if resource is already in db - based on id and revised date """
        pass
