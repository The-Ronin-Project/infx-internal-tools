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
from app.helpers.db_helper import db_cursor, \
    dynamic_select_stmt, dynamic_insert_stmt, dynamic_update_stmt, dynamic_delete_stmt


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
        version = xhtml_soup.find('meta', {'name': 'revisedDate'})['content']
        language_code = {'language_code': xhtml_soup.html['lang']}
        try:
            ExternalResource.retrieve_language_code(language_code)
        except Exception as error:
            return f"Cannot import external resource. Language code not found: {error}"

        table_query = {'name': 'external_resource_version', 'schema': 'patient_education'}
        select_data = {
            'version': version,
            'external_id': ex_resource_id
        }
        resource_exist = dynamic_select_stmt(table_query, select_data)

        if not resource_exist:
            title = xhtml_soup.title.get_text()
            md_text_body = md(xhtml_data, heading_style='ATX')
            resource_type = ResourceType.markdown if 'elsevier' in url else ResourceType.external_link
            body = os.linesep.join([empty_lines for empty_lines in md_text_body.splitlines() if empty_lines])
            _uuid = uuid.uuid1()
            external_resource = collections.namedtuple(
                'external_resource', [
                    'url',
                    'language_code',
                    'external_uuid',
                    'type',
                    'version',
                    'title',
                    'body',
                    'external_resource_id'
                ]
            )
            exr = external_resource(
                resource_url, language_code.get('language_code'), _uuid, resource_type,
                version, title, body, ex_resource_id
            )
            return ExternalResource.load_resource(exr)

        return resource_exist

    @staticmethod
    @db_cursor
    def load_resource(cursor, external_resource):
        """ insert external resource into db, return inserted data to user """
        # query_table = {'name': 'external_resource_version', 'schema': 'patient_education'}
        cursor.execute(text(
            """
            INSERT INTO patient_education.external_resource_version
            (uuid, version, type, url, title, body, language_code, external_resource_id) 
            VALUES (:uuid, :version, :type, :url, :title, :body, :language_code, :external_resource_id);
            """
        ), {
            'external_uuid': external_resource.uuid,
            'version': external_resource.version,
            'type': external_resource.type,
            'url': external_resource.url,
            'title': external_resource.title,
            'body': external_resource.body,
            'language_code': external_resource.language_code,
            'external_id': external_resource.external_resource_id
        })

        query_table = {'name': 'external_resource_version', 'schema': 'patient_education'}
        select_data = {
            'external_uuid': str(external_resource.uuid),
        }
        check_for_resource = dynamic_select_stmt(query_table, select_data)
        return check_for_resource if check_for_resource else False

    @staticmethod
    def link_external_and_internal_resources(ex_resource_id, resource_id, tenant_id):
        uuid_resource_id = uuid.UUID(resource_id)
        query_table = {'name': 'additional_resource_link', 'schema': 'patient_education'}
        insert_data = {
            'resource_version_uuid': uuid_resource_id,
            'external_resource_version_uuid': ex_resource_id,
            'tenant_id': tenant_id if tenant_id else None}
        dynamic_insert_stmt(query_table, insert_data)

        select_data = {
            'resource_version_uuid': str(uuid_resource_id),
            'external_resource_version_uuid': str(ex_resource_id)
        }
        check_link = dynamic_select_stmt(query_table, select_data)
        return True if check_link else False

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

    @staticmethod
    def delete_linked_ex_resource(ex_resource_id):
        """ delete link between internal and external resource """
        table_query = {'name': 'additional_resource_link', 'schema': 'patient_education'}
        delete_data = {'external_resource_version_uuid': str(ex_resource_id)}
        dynamic_delete_stmt(table_query, delete_data)
        return ex_resource_id


@dataclass
class Resource:
    """ related to local or tenant resources """
    language_code: str
    title: str
    body: str
    status: Optional[Status] = None
    version_uuid: Optional = None
    resource_uuid: Optional = None
    version: Optional[int] = 1

    def __post_init__(self):
        self.conn = get_db()
        if not self.version_uuid:
            self.version_uuid = uuid.uuid1()
            self.resource_uuid = uuid.uuid1()
            self.status = Status.draft.value
            self.create_local_or_tenant_resource()
        if self.version_uuid:
            self.update_local_or_tenant_resource()

    def create_local_or_tenant_resource(self):
        """ create/insert new resource into db, return inserted data to user """
        # TODO add versioning (1, 2, 3)... and resource_uuid
        self.conn.execute(text(
            """
            INSERT INTO patient_education.resource (uuid) VALUES (:uuid);
        
            INSERT INTO patient_education.resource_version
            (version_uuid, title, body, language_code, status, version, resource_uuid) 
            VALUES (:version_uuid, :title, :body, :language_code, :status, :version, :resource_uuid);
            """
        ), {
            'version_uuid': self.version_uuid,
            'title': self.title,
            'body': self.body,
            'language_code': self.language_code,
            'status': self.status,
            'version': self.version,  # initial creation returns version 1
            'resource_uuid': self.resource_uuid,
            'uuid': self.resource_uuid
        })
        return Resource

    def update_local_or_tenant_resource(self):
        """ Update resource based on user changes """
        self.conn.execute(text(
            """
            UPDATE patient_education.resource_version
            SET title=:title, body=:body, language_code=:language_code, status=:status, version=:version
            WHERE version_uuid=:version_uuid
            """
        ), {
            'version_uuid': self.version_uuid,
            'title': self.title,
            'body': self.body,
            'language_code': self.language_code,
            'status': self.status,
            'version': self.version
        })
        return Resource

    @staticmethod
    @db_cursor
    def get_all_resources_with_linked(cursor):
        """ return all resources with or without linked external resource """
        all_resources = cursor.execute(text(
            """
            SELECT rv.version_uuid as internal_uuid, rv.title, language_name, l.language_code, rv.status, 
            erv.external_uuid, erv.title as external_title, erv.version as external_version, 
            erv.language_code as external_language, external_id, erv.url
            FROM patient_education.external_resource_version erv 
            JOIN patient_education.additional_resource_link arl 
            ON erv.external_uuid = arl.external_resource_version_uuid 
            FULL OUTER JOIN patient_education.resource_version rv 
            ON arl.resource_version_uuid = rv.version_uuid
            JOIN public.languages l
            ON rv.language_code = l.language_code
            ORDER BY rv.version DESC
            """
        )).fetchall()
        return [dict(row) for row in all_resources]

    @staticmethod
    def status_update(version_uuid, status):
        """ strictly for updated status, generated by retool button click """
        table_query = {'name': 'resource_version', 'schema': 'patient_education'}
        version_to_update = {'version_uuid': str(version_uuid)}
        data = {'status': status}
        update = dynamic_update_stmt(table_query, version_to_update, data)

        return update

    @staticmethod
    def new_version(resource_uuid):
        """
        create new version of existing resource (get most recent version and bump it +1),
        if status is active retool will only let you create new, status is checked here too
        """
        version_uuid = uuid.uuid1()
        table_query = {'name': 'resource_version', 'schema': 'patient_education'}
        copy_data = {'resource_uuid': resource_uuid}
        current_version = dynamic_select_stmt(table_query, copy_data, 'version')

        if current_version.status == Status.active.value:
            insert_data = {
                'version_uuid': str(version_uuid),
                'title': current_version.title,
                'body': current_version.body,
                'language_code': current_version.language_code,
                'status': Status.draft.value,  # draft value for new version
                'version': current_version.version + 1,
                'resource_uuid': resource_uuid
            }
            dynamic_insert_stmt(table_query, insert_data)

            select_data = {'version_uuid': str(version_uuid)}
            new_version = dynamic_select_stmt(table_query, select_data)
            return new_version
        return {'message': 'Cannot create new version. Version must be active for a new version to be created.'}

    @staticmethod
    def delete(version_uuid):
        """
        delete resource, cannot be in PUBLISHED status
        retool handle status on this - only give option to delete if not active status?
        """
        table_query = {'name': 'resource_version', 'schema': 'patient_education'}
        delete_data = {'version_uuid': str(version_uuid)}
        dynamic_delete_stmt(table_query, delete_data)
        return {"message": f"{version_uuid} has been removed"}
