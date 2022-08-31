import collections
import os
import requests
import uuid
import readtime as rt

from sqlalchemy import text, Table, MetaData
from markdownify import markdownify as md
from bs4 import BeautifulSoup as Soup
from dataclasses import dataclass
from typing import Optional
from enum import Enum, unique
from decouple import config
from app.helpers.db_helper import db_cursor, \
    dynamic_select_stmt, dynamic_update_stmt, dynamic_delete_stmt


@unique
class ResourceType(str, Enum):
    external_link: str = 'external link'
    markdown: str = 'markdown'


@unique
class Status(str, Enum):
    draft: str = 'draft'
    under_review: str = 'under review'
    active: str = 'active'
    retired: str = 'retired'


@dataclass
class ExternalResource:
    """ in the event that only elsevier resources are used/linked """
    external_id: str
    patient_term: str
    language: str
    body: Optional[str] = None
    external_url: Optional[str] = None
    tenant_id: Optional[str] = None
    external_version: Optional[int] = None
    version_uuid: Optional[uuid] = None
    resource_type: Optional[ResourceType] = None
    language_code: Optional[str] = None
    status: Optional[Status] = None
    data_source: Optional[str] = None

    def __post_init__(self):
        self.version_uuid = uuid.uuid1()
        if not self.status:
            self.status = Status.draft.value

        self.extract_and_modify_resource()

    @staticmethod
    @db_cursor
    def retrieve_language_code(cursor, code):
        """
        check if code is in table, if it is return true, else return the base language_code
        example: if code were en-XX only return en **** this will always search the public.languages
        tables - therefore always hardcoded.
        """
        metadata = MetaData()
        table = Table('languages', metadata, schema='public', autoload=True, autoload_with=cursor)
        query = table.select()
        for k, v in code.items():
            query = query.where(getattr(table.columns, k) == v)

        result = [dict(row) for row in cursor.execute(query).all()]
        return True if result else False

    def extract_and_modify_resource(self):
        url = config('EXTERNAL_RESOURCE_URL')
        self.external_url = f"{url}{self.language}/{self.external_id}"
        response = requests.get(self.external_url)
        xhtml_data = response.text
        xhtml_soup = Soup(xhtml_data, 'html.parser')
        self.external_version = xhtml_soup.find('meta', {'name': 'revisedDate'})['content']
        language_code = {'language_code': xhtml_soup.html['lang']}
        title = xhtml_soup.title.get_text()

        try:
            ExternalResource.retrieve_language_code(language_code)
        except Exception as error:
            return f"Cannot import external resource. Language code not found: {error}"

        table_query = {'name': 'elsevier_only_test_table', 'schema': 'patient_education'}
        select_data = {
            'external_version': self.external_version,
            'external_id': self.external_id,
            'external_title': title
        }
        resource_exist = dynamic_select_stmt(table_query, select_data)

        if not resource_exist:
            md_text_body = md(xhtml_data, heading_style='ATX')
            self.resource_type = ResourceType.markdown if 'elsevier' in url else ResourceType.external_link
            self.body = os.linesep.join([empty_lines for empty_lines in md_text_body.splitlines() if empty_lines])
            self.language_code = xhtml_soup.html['lang']
            resource_uuid = uuid.uuid1()
            if self.tenant_id:
                self.data_source = self.tenant_id
            else:
                self.data_source = 'Elsevier'
            # disclaimer_and_copywrite = x
            readtime = rt.of_markdown(self.body)
            external_resource = collections.namedtuple(
                'external_resource', [
                    'version_uuid',
                    'version',
                    'content_type',
                    'url',
                    'title',
                    'body',
                    'read_time',
                    'language_code',
                    'status',
                    'external_id',
                    'data_source',
                    'tenant_id'
                    'resource_uuid'
                ]
            )
            exr = external_resource(
                self.version_uuid, self.external_version, self.resource_type, url, title, self.body,
                readtime, self.language_code, self.status, self.external_id,  self.data_source,
                self.tenant_id, resource_uuid
            )
            resource = ExternalResource.save_external_resource(exr)
            return resource
        return resource_exist

    @staticmethod
    @db_cursor
    def save_external_resource(cursor, external_resource):
        """ insert external resource into db, return inserted data to user """
        cursor.execute(text(
            """
            INSERT INTO patient_education_external.resource
            (uuid) VALUES (:uuid)
            """
            ), {
            'uuid': external_resource.resource_uuid
        })

        cursor.execute(text(
            """
            INSERT INTO patient_education_external.resource_version
            (version_uuid, version, content_type, url, title, body, read_time, language_code, status, external_id, data_source, tenant_id, resource_uuid) 
            VALUES (:version_uuid, :version, :content_type, :url, :title, :body, :read_time, :language_code, :status, :external_id, :data_source, :tenant_id, :resource_uuid);
            """
        ), {
            'version_uuid': external_resource.version_uuid,
            'version': external_resource.external_version,
            'content_type': external_resource.content_type,
            'url': external_resource.url,
            'title': external_resource.title,
            'body': external_resource.body,
            'read_time': external_resource.read_time,
            'language_code': external_resource.language_code,
            'status': external_resource.status,
            'external_id': external_resource.external_id,
            'data_source': external_resource.data_source,
            'tenant_id': external_resource.tenant_id,
            'resource_uuid': external_resource.resource_uuid
        })

        query_table = {'name': 'resource_version', 'schema': 'patient_education_external'}
        select_data = {
            'version_uuid': str(external_resource.version_uuid),
        }
        check_for_resource = dynamic_select_stmt(query_table, select_data)
        return check_for_resource if check_for_resource else False

    @staticmethod
    @db_cursor
    def get_all_external_resources(cursor):
        all_external_resources = cursor.execute(text(
            """
            SELECT * FROM patient_education_external.resource_version
            """
        )).fetchall()

        return [dict(row) for row in all_external_resources]

    @staticmethod
    def update_status(status, _uuid):
        table_query = {'name': 'resource_version', 'schema': 'patient_education_external'}
        version_to_update = {'version_uuid': str(_uuid)}
        data = {'status': status}
        update = dynamic_update_stmt(table_query, version_to_update, data)
        return update if update else False

    @staticmethod
    def unlink_resource(_uuid):
        """
        delete resource, cannot be in PUBLISHED status
        ReTool handle status on this - only give option to delete if not active status
        checking status here as well
        """
        table_query = {'name': 'resource_version', 'schema': 'patient_education_external'}
        data = {'version_uuid': _uuid}
        get_status = dynamic_select_stmt(table_query, data)
        if get_status.status != 'active':
            dynamic_delete_stmt(table_query, data)
            return {"message": f"{_uuid} has been removed"}
        return {"message": f"{_uuid} cannot be removed, status is {get_status.status}"}
