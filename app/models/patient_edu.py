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
    def locate_external_resource(language, resource_id):
        """ given the url, locate resource and perform checks before loading into db """
        url = config('EXTERNAL_RESOURCE_URL')
        resource_url = f"{url}{language}/{resource_id}"
        response = requests.get(resource_url)
        xml_data = response.text
        xml_soup = Soup(xml_data, 'html.parser')
        version = xml_soup.find('meta', {'name': 'revisedDate'})['content'],
        does_exist = ExternalResource.check_if_resource_exists(version, resource_id)

        if not does_exist:
            title = xml_soup.title.get_text()
            resource_language = xml_soup.find('meta', {'name': 'language'})['content']
            md_text_body = md(xml_data, heading_style='ATX')
            resource_type = ResourceType.markdown if 'elsevier' in url else ResourceType.external_link
            body = os.linesep.join([empty_lines for empty_lines in md_text_body.splitlines() if empty_lines])
            _uuid = uuid.uuid1()
            external_resource = {
                'url': resource_url,
                'language': resource_language,
                'uuid': _uuid,
                'type': resource_type,
                'version': version,
                'title': title,
                'body': body,
                'external_resource_id': resource_id
            }
            return ExternalResource.load_resource(external_resource)

        return False

    @staticmethod
    def check_if_resource_exists(version, resource_id):
        """ check if resource is already exits in db - based on resource_id and revised date/version """
        conn = get_db()
        resource_exists = conn.execute(text(
            """
            SELECT * FROM patient_education.external_resource_version WHERE
            version=:version AND external_resource_id=:external_resource_id
            """
        ), {
            'version': version,
            'external_resource_id': resource_id
        }).fetchall()

        return True if resource_exists else False

    @staticmethod
    def load_resource(external_resource):
        """ insert external resource into db, return inserted data to user """
        conn = get_db()
        conn.execute(text(
            """
            INSERT INTO patient_education.external_resource_version
            (uuid, version, type, url, title, body, language, external_resource_id) 
            VALUES (:uuid, :version, :type, :url, :title, :body, :language, :external_resource_id);
            """
        ), {
            'uuid': external_resource.get('uuid'),
            'version': external_resource.get('version'),
            'type': external_resource.get('type'),
            'url': external_resource.get('url'),
            'title': external_resource.get('title'),
            'body': external_resource.get('body'),
            'language': external_resource.get('language'),
            'external_resource_id': external_resource.get('external_resource_id')
        })

        get_inserted_ex_resource = conn.execute(text(
            """
            SELECT * FROM patient_education.external_resource_version WHERE
            uuid=:uuid
            """
        ), {
            'uuid': external_resource.get('uuid')
        }).fetchall()
        if not get_inserted_ex_resource:
            return f"Could not return linked data"

        return [dict(row) for row in get_inserted_ex_resource]


@dataclass
class Resource:
    """ related to local or tenant resources """
    pass
