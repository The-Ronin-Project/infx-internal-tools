import collections
import os
import requests
import uuid

from sqlalchemy import text, Table, MetaData
from markdownify import markdownify as md
from bs4 import BeautifulSoup as Soup
from app.database import get_db
from dataclasses import dataclass
from typing import Optional
from enum import Enum, unique
from decouple import config
from app.helpers.db_helper import db_cursor

# TODO: create helper function for linking local resource to tenant resource?
#  the link should happen during the download


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
        """ given the url, locate resource and perform checks and format before loading into db """
        url = config('EXTERNAL_RESOURCE_URL')
        resource_url = f"{url}{language}/{ex_resource_id}"
        response = requests.get(resource_url)
        xhtml_data = response.text
        xhtml_soup = Soup(xhtml_data, 'html.parser')
        version = xhtml_soup.find('meta', {'name': 'revisedDate'})['content'],
        table_query = {'name': 'external_resource_version', 'schema': 'patient_education'}
        data = {
            'version': version,
            'external_resource_id': ex_resource_id
        }
        resource_exist = ExternalResource.dynamic_select(table_query, data)

        if not resource_exist:
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
            exr = external_resource(
                resource_url, resource_language, _uuid, resource_type,
                version, title, body, ex_resource_id
            )
            return ExternalResource.load_resource(exr)

        return resource_exist

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

        query_table = {'name': 'external_resource_version', 'schema': 'patient_education'}
        data = {
            'uuid': str(external_resource.uuid),
        }
        check_for_resource = ExternalResource.dynamic_select(query_table, data)
        return check_for_resource if check_for_resource else False

    @staticmethod
    @db_cursor
    def link_external_and_internal_resources(cursor, ex_resource_id, resource_id, tenant_id):
        uuid_resource_id = uuid.UUID(resource_id)
        cursor.execute(text(
            """
            INSERT INTO patient_education.additional_resource_link
            (resource_version_uuid, external_resource_version_uuid, tenant_id)
            VALUES (:resource_version_uuid, :external_resource_version_uuid, :tenant_id)
            """
        ), {
            'resource_version_uuid': uuid_resource_id,
            'external_resource_version_uuid': ex_resource_id,
            'tenant_id': tenant_id if tenant_id else None
        })
        query_table = {'name': 'additional_resource_link', 'schema': 'patient_education'}
        data = {
            'resource_version_uuid': str(uuid_resource_id),
            'external_resource_version_uuid': str(ex_resource_id)
        }
        check_link = ExternalResource.dynamic_select(query_table, data)
        return True if check_link else False

    @staticmethod
    @db_cursor
    def dynamic_select(cursor, query_table, data):
        table_name = query_table.get('name')
        schema = query_table.get('schema')
        metadata = MetaData()
        table = Table(table_name, metadata, schema=schema, autoload=True, autoload_with=cursor)
        query = table.select()
        for k, v in data.items():
            query = query.where(getattr(table.columns, k) == v)

        return [dict(row) for row in cursor.execute(query).all()]


@dataclass
class Resource:
    """ related to local or tenant resources """
    language: str
    title: str
    body: str
    version: Optional[int] = None
    status: Optional[Status] = None
    resource_uuid: Optional = None

    def __post_init__(self):
        self.conn = get_db()
        if not self.resource_uuid:
            self.resource_uuid = uuid.uuid1()
            self.status = Status.draft.value
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
        # TODO should there be a does exist check? or a status check? if status is active
        #  create new? -- call create new?
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
    def get_all_resources_with_linked(cursor):
        all_resources = cursor.execute(text(
            """
            SELECT rv.uuid as internal_uuid, rv.title, rv.language as internal_language, rv.status, 
            erv.uuid as external_uuid, erv.title as external_title, erv.version as external_version, 
            erv.language as external_language, external_resource_id
            FROM patient_education.external_resource_version erv 
            JOIN patient_education.additional_resource_link arl 
            ON erv.uuid = arl.external_resource_version_uuid 
            FULL OUTER JOIN patient_education.resource_version rv 
            ON arl.resource_version_uuid = rv.uuid
            """
        ))
        return all_resources

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

    @staticmethod
    @db_cursor
    def delete_linked_ex_resource(cursor, ex_resource_id):
        cursor.execute(text(
            """
            DELETE FROM patient_education.additional_resource_link 
            WHERE external_resource_version_uuid=:uuid
            """
        ), {
            'external_resource_version_uuid': ex_resource_id
        })
        return ex_resource_id
