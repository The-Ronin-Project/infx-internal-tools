import collections
import os
import requests
import uuid

from sqlalchemy import text
from markdownify import markdownify as md
from bs4 import BeautifulSoup as Soup
from app.database import get_db
from dataclasses import dataclass
from typing import Optional
from enum import Enum, unique
from decouple import config
from app.helpers.db_helper import db_cursor

# TODO: create helper function for linking local resource to tenant resource?
# TODO: the link should happen during the download


@unique
class ResourceType(str, Enum):
    external_link: str = 'external link'
    markdown: str = 'markdown'


@unique
class Status(str, Enum):
    draft: str = 'Draft'
    under_review: str = 'Under Review'
    active: str = 'Active'


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

    def __post_init__(self):
        self.conn = get_db()

    @staticmethod
    def locate_external_resource(language, ex_resource_id):
        """ given the url, locate resource and perform checks before loading into db """
        url = config('EXTERNAL_RESOURCE_URL')
        resource_url = f"{url}{language}/{ex_resource_id}"
        response = requests.get(resource_url)
        xhtml_data = response.text
        xhtml_soup = Soup(xhtml_data, 'html.parser')
        version = xhtml_soup.find('meta', {'name': 'revisedDate'})['content'],
        does_exist = ExternalResource.check_if_resource_exists(version, ex_resource_id)

        if not does_exist:
            title = xhtml_soup.title.get_text()
            resource_language = xhtml_soup.find('meta', {'name': 'language'})['content']
            md_text_body = md(xhtml_data, heading_style='ATX')
            resource_type = ResourceType.markdown if 'elsevier' in url else ResourceType.external_link
            body = os.linesep.join([empty_lines for empty_lines in md_text_body.splitlines() if empty_lines])
            _uuid = uuid.uuid1()
            external_resource = collections.namedtuple(
                'external_resource', [
                    'url',
                    'language',
                    'uuid',
                    'type',
                    'version',
                    'title',
                    'body',
                    'external_resource_id'
                ]
            )
            exr = external_resource(resource_url, language, _uuid, resource_type, version, title, body, ex_resource_id)
            return ExternalResource.load_resource(exr)

        return False

    @staticmethod
    @db_cursor
    def check_if_resource_exists(cursor, version, resource_id):
        """ check if resource is already exits in db - based on resource_id and revised date/version """
        resource_exists = cursor.execute(text(
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
    @db_cursor
    def load_resource(cursor, external_resource):
        """ insert external resource into db, return inserted data to user """
        cursor.execute(text(
            """
            INSERT INTO patient_education.external_resource_version
            (uuid, version, type, url, title, body, language, external_resource_id) 
            VALUES (:uuid, :version, :type, :url, :title, :body, :language, :external_resource_id);
            """
        ), {
            'uuid': external_resource.uuid,
            'version': external_resource.version,
            'type': external_resource.type,
            'url': external_resource.url,
            'title': external_resource.title,
            'body': external_resource.body,
            'language': external_resource.language,
            'external_resource_id': external_resource.external_resource_id
        })

        check_for_resource = ExternalResource.check_if_exists(external_resource.uuid)
        return check_for_resource

    @staticmethod
    @db_cursor
    def check_if_exists(cursor, external_uuid):
        get_inserted_ex_resource = cursor.execute(text(
            """
            SELECT * FROM patient_education.external_resource_version WHERE uuid=:uuid
            """
        ), {
            'uuid': external_uuid
        }).fetchall()
        if not get_inserted_ex_resource:
            return f"Could not return linked data"

        return [dict(row) for row in get_inserted_ex_resource]


@dataclass
class Resource:
    """ related to local or tenant resources """
    language: str
    title: str
    body: str
    resource_uuid: Optional = None
    status: Optional[str] = 'Draft'

    def __post_init__(self):
        self.conn = get_db()
        if not self.resource_uuid:
            self.resource_uuid = uuid.uuid1()
            self.create_local_or_tenant_resource()
        if self.resource_uuid:
            self.update_local_or_tenant_resource()

    def create_local_or_tenant_resource(self):
        """ create/insert new resource into db, return inserted data to user """
        self.conn.execute(text(
            """
            INSERT INTO patient_education.resource_version
            (uuid, title, body, language, status) 
            VALUES (:uuid, :title, :body, :language, :status);
            """
        ), {
            'uuid': self.resource_uuid,
            'title': self.title,
            'body': self.body,
            'language': self.language,
            'status': self.status
        })
        return Resource

    def update_local_or_tenant_resource(self):
        # TODO does exist check? status check
        self.conn.execute(text(
            """
            UPDATE patient_education.resource_version
            SET title=:title, body=:body, language=:language, status=:status
            WHERE uuid=:uuid
            """
        ), {
            'uuid': self.resource_uuid,
            'title': self.title,
            'body': self.body,
            'language': self.language,
            'status': self.status
        })
        return Resource

    @staticmethod
    @db_cursor
    def delete(cursor, resource_id):
        cursor.execute(text(
            """
            DELETE FROM patient_education.resource_version WHERE uuid=:uuid
            """
        ), {
            'uuid': resource_id
        })
        return resource_id

    def link_local_and_external_resources(self):
        pass

    def delete_linked_ex_resource(self):
        pass
