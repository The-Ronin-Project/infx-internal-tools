import asyncio
import datetime
import json
import logging
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from json import JSONDecodeError
from typing import Dict, List, Optional
import traceback
import sys

import httpx
import pytest
from cachetools.func import ttl_cache
from decouple import config
from deprecated.classic import deprecated
from httpx import ReadTimeout
from sqlalchemy import text, Table, Column, MetaData, Text, bindparam
from sqlalchemy.dialects.postgresql import UUID as UUID_column_type
from werkzeug.exceptions import BadRequest

import app
import app.concept_maps.models
import app.models.data_ingestion_registry
import app.value_sets.models
from app.helpers.message_helper import message_exception_summary
from app.models.codes import Code
from app.models.models import Organization
from app.terminologies.models import Terminology
from app.database import get_db

DATABASE_HOST = config("DATABASE_HOST", default="")
DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL", default=""
)
CLIENT_ID = config("DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_ID", default="")
CLIENT_SECRET = config("DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_SECRET", default="")
AUTH_AUDIENCE = config("DATA_NORMALIZATION_ERROR_SERVICE_AUDIENCE", default="")
AUTH_URL = config("DATA_NORMALIZATION_ERROR_SERVICE_AUTH_URL", default="")
PAGE_SIZE = 1000

LOGGER = logging.getLogger()

def get_token(url: str, client_id: str, client_secret: str, audience: str) -> str:
    """
    Fetches a token from Auth0.
    """
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": "client_credentials",
    }
    response = httpx.post(url, json=payload)
    try:
        token = response.json()["access_token"]
    except (KeyError, ValueError) as e:
        LOGGER.error("Failed to get token")
        LOGGER.debug(response.content)
        raise e
    return token


def convert_string_to_datetime_or_none(input_string):
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")


# Function to use the token to access the API
def make_get_request(token, client: httpx.Client, base_url, api_url, params={}):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


async def make_get_request_async(token, client: httpx.AsyncClient, base_url, api_url, params={}):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


async def make_post_request_async(token, client: httpx.AsyncClient, base_url, api_url, params={}):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.post(base_url + api_url, headers=headers, params=params)
    return response.json()


class ResourceType(Enum):
    OBSERVATION = "Observation"
    CONDITION = "Condition"
    # MEDICATION = "Medication"
    LOCATION = "Location"
    PATIENT = "Patient"
    PRACTITIONER = "Practitioner"
    PRACTITIONER_ROLE = "PractitionerRole"  # For both use and system
    APPOINTMENT = "Appointment"
    DOCUMENT_REFERENCE = "DocumentReference"
    # TELECOM_USE = "Practitioner.telecom.use"  # Only in for testing until we have a real data type live


@dataclass
class ErrorServiceResource:
    """
    We'll fill this out later...
    """

    id: uuid.UUID
    organization: Organization
    resource_type: ResourceType
    resource: str
    status: str
    severity: str
    create_dt_tm: datetime.datetime
    update_dt_tm: Optional[datetime.datetime]
    reprocess_dt_tm: Optional[datetime.datetime]
    reprocessed_by: Optional[str]
    token: Optional[str]

    def __post_init__(self):
        self.issues = []
        if self.token is None:
            self.token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)

    @classmethod
    def deserialize(cls, resource_data, token=None):
        organization = Organization(resource_data.get("organization_id"))

        resource_type = None
        for resource_type_option in ResourceType:
            if resource_type_option.value == resource_data.get("resource_type"):
                resource_type = resource_type_option
                break

        return cls(
            id=uuid.UUID(resource_data.get("id")),
            organization=organization,
            resource_type=resource_type,
            resource=resource_data.get("resource"),
            status=resource_data.get("status"),
            severity=resource_data.get("severity"),
            create_dt_tm=convert_string_to_datetime_or_none(
                resource_data.get("create_dt_tm")
            ),
            update_dt_tm=convert_string_to_datetime_or_none(
                resource_data.get("update_dt_tm")
            ),
            reprocess_dt_tm=convert_string_to_datetime_or_none(
                resource_data.get("reprocess_dt_tm")
            ),
            reprocessed_by=resource_data.get("reprocessed_by"),
            token=token,
        )

    async def load_issues(self, client):
        # Call the endpoint
        try:
            get_issues_for_resource = await make_get_request_async(
                token=self.token,
                client=client,
                base_url=f"{DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}",
                api_url=f"/resources/{self.id}/issues",
                params={},
            )
        # If an error occurs on one resource, skip it
        except httpx.ConnectError:
            LOGGER.warning(
                f"httpx.ConnectError, skipping load: Error Service Resource ID: {self.id} - {self.resource_type.value} for organization {self.organization.id}"
            )
            return None
        except:  # uncaught exceptions can be so costly here, that a 'bare except' is acceptable, despite PEP 8: E722
            LOGGER.warning(
                f"Unknown Error, skipping load: Error Service Resource ID: {self.id} - {self.resource_type.value} for organization {self.organization.id}"
            )
            return None

        # Instantiate ErrorServiceIssue
        for issue_data in get_issues_for_resource:
            issue = ErrorServiceIssue.deserialize(issue_data)
            self.issues.append(issue)

    def filter_issues_by_type(self, issue_type="NOV_CONMAP_LOOKUP"):
        filtered_issues = []
        for issue in self.issues:
            if issue.type == issue_type:
                filtered_issues.append(issue)
        return filtered_issues

    @property
    def issue_ids(self) -> List[uuid.UUID]:
        return [issue.id for issue in self.issues]


@dataclass
class ErrorServiceIssue:
    """
    To be filled
    """

    id: uuid.UUID
    severity: str
    type: str
    description: str
    status: str
    create_dt_tm: datetime.datetime
    location: str
    update_dt_tm: Optional[datetime.datetime]
    metadata: Optional[str]

    @classmethod
    def deserialize(cls, issue_data):
        return cls(
            id=uuid.UUID(issue_data.get("id")),
            severity=issue_data.get("severity"),
            type=issue_data.get("type"),
            description=issue_data.get("description"),
            status=issue_data.get("status"),
            create_dt_tm=convert_string_to_datetime_or_none(
                issue_data.get("create_dt_tm")
            ),
            location=issue_data.get("location"),
            update_dt_tm=convert_string_to_datetime_or_none(
                issue_data.get("update_dt_tm")
            ),
            metadata=issue_data.get("metadata"),
        )


metadata = MetaData()

# Define the table using the Table syntax
temp_error_data = Table(
    'temp_error_data', metadata,
    Column('code', Text),
    Column('display', Text),
    Column('terminology_version_uuid', UUID_column_type),
    Column('depends_on_system', Text),
    Column('depends_on_property', Text),
    Column('depends_on_display', Text),
    Column('depends_on_value', Text),
    Column('issue_uuid', UUID_column_type),
    Column('resource_uuid', UUID_column_type),
    Column('status', Text)
)


def extract_telecom_data(resource_type, location, raw_resource):
    """
    Helper function for processing fields like Patient.telecom.system and Patient.telecom.use in various FHIR resources.
    @param resource_type: Patient, Practitioner, PractitionerRole, or Location
    @param location: Key provided by the issue report identifying the data location of the issue, for example
    Patient.telecom[0].system is an error on the system value in the first entry in the Patient.telecom list.
    @param raw_resource: raw data received with the issue report.
    @return: A code object and display text.
    """
    # Define the regular expression patterns for 'system' and 'use'
    regex_patterns = {
        'system': re.compile(fr"^{resource_type}\.telecom\[(\d+)\]\.system$"),
        'use': re.compile(fr"^{resource_type}\.telecom\[(\d+)\]\.use$")
    }

    # Initialize variables for processed code and display
    processed_code = None
    processed_display = None

    # Loop through the keys 'system' and 'use' to find matches
    for key in ['system', 'use']:
        match = regex_patterns[key].search(location)
        if match:
            list_index = int(match.group(1))
            raw_code = raw_resource['telecom'][list_index][key]
            processed_code = raw_code
            processed_display = raw_code

    return processed_code, processed_display


def load_concepts_from_errors(
        commit_changes=True,
        page_size: int = None,
        requested_organization_id: str = None,
        requested_resource_type: str = None,
):
    """
    Extracts specific concepts from a list of errors and saves them to a custom terminology.

    This function processes errors to identify and extract relevant concepts. It then organizes
    these concepts by the originating organization and type of resource they pertain to. The
    results are saved back to the appropriate custom terminology.

    A daily run inputs no values and gets all organization_ids and resource_types using the default HTTP GET page_size.

    Here is a sample test output log limited by input values for page_size, organization_id, and resource_type:
    ```
    Begin import from error service
    50 errors in page
    Checking registry for concept map entry for data element: Appointment.status and organization: apposnd
    Loading 3 new codes to terminology apposnd_appointmentstatus version 2
    50 errors in page
    Loading 4 new codes to terminology apposnd_appointmentstatus version 2
    50 errors in page
    Loading 4 new codes to terminology apposnd_appointmentstatus version 2
    500 errors in page
    Loading 3 new codes to terminology apposnd_appointmentstatus version 2
    41 errors in page
    Loading 2 new codes to terminology apposnd_appointmentstatus version 2
    Loading data from error service to custom terminologies complete

    Process finished with exit code 0
    ```

    Parameters:
        commit_changes (bool, optional): Whether or not to commit after each batch
        page_size (int, optional): HTTP GET page size, if empty, use the default PAGE_SIZE
        requested_organization_id (str): If non-empty, get errors only for the organization_id listed; if empty, get all
        requested_resource_type (str): Get errors only for the ResourceType enum string listed; if empty, get all

    Procedure:
        1. Fetch resources that have encountered errors.
        2. Extract relevant codes from the resource.
        3. Deduplicate the codes to avoid redundancy.
        4. Load the unique codes into their respective terminologies.
    """
    # Step 0: Initialize and setup
    if page_size is None:
        page_size = PAGE_SIZE
    if requested_resource_type is not None and (requested_resource_type not in [r.value for r in ResourceType]):
        LOGGER.warning(f"Support for the {requested_resource_type} resource type has not been implemented")
        return

    # Logging variables
    organization_id = requested_organization_id
    input_fhir_resource = requested_resource_type
    error_service_resource_id = None
    error_service_issue_id = None
    time_start = datetime.datetime.now()
    previous_time = datetime.datetime.now()
    zero = datetime.timedelta(0)
    all_loop_average = zero
    all_loop_total = zero
    all_loop_count = 0
    all_skip_count = 0
    step_1_total = zero
    step_2_total = zero
    step_1_average = zero
    step_2_average = zero

    # Start logging
    LOGGER.warning(
        f"Begin import from error service at local time {time_start}\n" +
        f"  Load from: {DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}\n" +
        f"  Load to:   {DATABASE_HOST}\n\n" +
        f"Main loop:\n"
    )

    # Local caches
    # todo: study why ttl_cache, or other caching strategies, did not stop repeat loads from happening
    data_normalization_registry = dict()
    source_value_set = dict()
    terminology_for_value_set = dict()
    terminology_version = dict()

    # Resources we have already processed in this environment
    environment = get_environment_from_service_url(DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL)
    error_service_resource_ids = get_all_unresolved_validation(environment)

    try:
        # Step 1: Fetch resources that have encountered errors.
        token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
        with httpx.Client(timeout=60.0) as client:

            # Collecting all resources with errors through paginated API calls.
            rest_api_params = dict(
                {
                    "order": "ASC",
                    "limit": page_size,
                    "issue_type": "NOV_CONMAP_LOOKUP",
                    "status": "REPORTED",  # exclude these status values: IGNORED - ADDRESSING - REPROCESSED - CORRECTED
                }
            )
            if input_fhir_resource is not None:
                rest_api_params.update({"resource_type": input_fhir_resource})
            if organization_id is not None:
                rest_api_params.update({"organization_id": organization_id})

            # Continuously fetch resources until all pages have been retrieved.
            all_resources_fetched = False
            while all_resources_fetched is False:
                # Retrieve the first page of resources
                response = make_get_request(
                    token=token,
                    client=client,
                    base_url=DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL,
                    api_url="/resources",
                    params=rest_api_params,
                )
                resources_with_errors = response

                length_of_response = len(response)

                if length_of_response < page_size:
                    all_resources_fetched = True

                else:
                    last_uuid = response[-1].get("id")
                    rest_api_params["after"] = last_uuid

                current_time = datetime.datetime.now()
                since_last_time = current_time - previous_time
                previous_time = current_time
                loop_total = since_last_time
                step_1 = since_last_time
                step_1_total += since_last_time

                # Convert API response data to ErrorServiceResource objects.
                error_resources = []
                for resource_data in resources_with_errors:
                    organization = Organization(resource_data.get("organization_id"))
                    organization_id = organization.id

                    fhir_resource_type = None
                    for resource_type_option in ResourceType:
                        if resource_type_option.value == resource_data.get("resource_type"):
                            fhir_resource_type = resource_type_option
                            input_fhir_resource = resource_type_option.value
                            break

                    if fhir_resource_type is None:
                        LOGGER.warning(
                            f"Support for the {fhir_resource_type} resource type has not been implemented"
                        )
                        all_skip_count += 1
                        continue

                    # See if we processed this error_service_resource_id in the past; if so, it is being addressed. If
                    # the way we addressed the case for error_service_resource_id did not successfully fix it, the case
                    # will reoccur in Interops, and will arrive at this function with a new error_service_resource_id
                    error_service_resource_id = resource_data.get("id")
                    if error_service_resource_id in error_service_resource_ids:
                        all_skip_count += 1
                        continue

                    error_resource = ErrorServiceResource(
                        id=uuid.UUID(error_service_resource_id),
                        organization=organization,
                        resource_type=fhir_resource_type,
                        resource=resource_data.get("resource"),
                        status=resource_data.get("status"),
                        severity=resource_data.get("severity"),
                        create_dt_tm=convert_string_to_datetime_or_none(
                            resource_data.get("create_dt_tm")
                        ),
                        update_dt_tm=convert_string_to_datetime_or_none(
                            resource_data.get("update_dt_tm")
                        ),
                        reprocess_dt_tm=convert_string_to_datetime_or_none(
                            resource_data.get("reprocess_dt_tm")
                        ),
                        reprocessed_by=resource_data.get("reprocessed_by"),
                        token=token,
                    )
                    error_resources.append(error_resource)

                # Load issues for all error service resources
                async def load_all_issues():
                    async with httpx.AsyncClient() as async_client:
                        await asyncio.gather(*(error_resource.load_issues(client=async_client) for error_resource in error_resources))
                asyncio.run(load_all_issues())

                # Step 2: Extract relevant codes from the resource.
                # For specific resource types, we may need to read the issue to
                # determine where in the resource the failure occurred.
                current_time = datetime.datetime.now()
                since_last_time = current_time - previous_time
                previous_time = current_time
                loop_total += since_last_time
                step_2 = since_last_time
                step_2_total += since_last_time

                # Initialize a dictionary to hold codes that need deduplication, grouped by terminology.
                # Key: terminology_version_uuid, Value: list containing the codes to load to that terminology
                new_codes_to_deduplicate_by_terminology = {}

                # Create a list to put the data linking codes back to the issues/errors they came from
                error_code_link_data = []

                # Walk through all the error service resources
                for error_service_resource in error_resources:
                    # Get the FHIR resource information from the error resource
                    resource_type = error_service_resource.resource_type
                    raw_resource = json.loads(error_service_resource.resource)

                    # The data element where validation is failed is stored in the 'location' on the issue
                    # We need to filter the issues to just the NOV_CONMAP_LOOKUP issues and get the location
                    nov_conmap_issues = error_service_resource.filter_issues_by_type(
                        "NOV_CONMAP_LOOKUP"
                    )

                    # In each error service resource, walk through each NOV_CONMAP_LOOKUP issue.
                    for issue in nov_conmap_issues:
                        # save for logging
                        error_service_issue_id = str(issue.id)

                        # data_element is the location without any brackets or indices
                        # ex. if location is 'Patient.telecom[1].system' then data_element is 'Patient.telecom.system'
                        location = issue.location
                        element = re.sub(r'\[\d+\]', '', location)
                        match = re.search(r'\[(\d+)\]', location)
                        # Extract any index that may be present in the location string
                        index = None
                        if match is not None:
                            index = int(match.group(1))

                        # Based on resource_type, identify the actual coding which needs to make it into the concept map.
                        # There is something unique about the handling for every resource_type we support.
                        # Condition
                        if resource_type == ResourceType.CONDITION:
                            # Condition.code is a CodeableConcept
                            raw_code = raw_resource["code"]
                            processed_code = raw_code
                            processed_display = raw_code.get("text")

                        # Observation
                        elif resource_type == ResourceType.OBSERVATION:
                            # Observation.value is a CodeableConcept
                            if element == "Observation.value":
                                if "valueCodeableConcept" not in raw_resource:
                                    continue
                                raw_code = raw_resource["valueCodeableConcept"]
                                processed_code = raw_code
                                processed_display = raw_code.get("text")

                            # Observation.code is a CodeableConcept
                            elif element == "Observation.code":
                                if "code" not in raw_resource:
                                    continue
                                raw_code = raw_resource["code"]
                                processed_code = raw_code
                                processed_display = raw_code.get("text")

                            # Observation.component.code is a CodeableConcept - the location will come in with an index
                            elif element == "Observation.component.code":
                                if index is None:
                                    continue
                                if (
                                    "component" not in raw_resource or
                                    index not in raw_resource["component"] or
                                    "code" not in raw_resource["component"][index]
                                ):
                                    continue
                                raw_code = raw_resource["component"][index]["code"]
                                processed_code = raw_code
                                processed_display = raw_code.get("text")

                            # Observation.component.value is a CodeableConcept - the location will come in with an index
                            elif element == "Observation.component.value":
                                if index is None:
                                    continue
                                if (
                                    "component" not in raw_resource or
                                    index not in raw_resource["component"] or
                                    "valueCodeableConcept" not in raw_resource["component"][index]
                                ):
                                    continue
                                raw_code = raw_resource["component"][index]["valueCodeableConcept"]
                                processed_code = raw_code
                                processed_display = raw_code.get("text")
                            else:
                                LOGGER.warning(f"Unrecognized location for Observation error: {location}")

                        # DocumentReference
                        elif resource_type == ResourceType.DOCUMENT_REFERENCE:
                            # DocumentReference.type is a CodeableConcept
                            if element == "DocumentReference.type":
                                raw_code = raw_resource["type"]
                                processed_code = raw_code
                                processed_display = raw_code.get("text")

                        # Appointment
                        elif resource_type == ResourceType.APPOINTMENT:
                            # Appointment.status is a raw code - used for code and display
                            if element == "Appointment.status":
                                raw_code = raw_resource["status"]
                                processed_code = raw_code
                                processed_display = raw_code

                        # Use the extract_telecom_data helper function to extract the codes from each resource type
                        # Extract codes for issues from all resource types for ContactPoint.system and ContactPoint.use

                        # Patient
                        elif resource_type == ResourceType.PATIENT:
                            processed_code, processed_display = extract_telecom_data('Patient', location, raw_resource)

                        # Practitioner
                        elif resource_type == ResourceType.PRACTITIONER:
                            processed_code, processed_display = extract_telecom_data('Practitioner', location, raw_resource)

                        # PractitionerRole
                        elif resource_type == ResourceType.PRACTITIONER_ROLE:
                            processed_code, processed_display = extract_telecom_data('PractitionerRole', location, raw_resource)

                        # Location
                        elif resource_type == ResourceType.LOCATION:
                            processed_code, processed_display = extract_telecom_data('Location', location, raw_resource)

                        # Case not yet implemented
                        else:
                            LOGGER.warning(
                                f"Support for the {fhir_resource_type} resource type at location {element} has not been implemented"
                            )
                            all_skip_count += 1
                            continue

                        # Lookup the concept map version used to normalize this type of resource
                        # So that we can then identify the correct terminology to load the new coding to

                        # Note that some normalization registry data_element strings need adjustment.
                        if element == "Observation.value" or element == "Observation.component.value":
                            data_element = f"{element}CodeableConcept"
                        else:
                            data_element = element

                        # Get the concept_map_version_for_normalization
                        registry_key = f"{organization.id} {data_element}"
                        if registry_key not in data_normalization_registry.keys():
                            concept_map_version_for_normalization = (
                                lookup_concept_map_version_for_data_element(
                                    data_element=data_element,
                                    organization=error_service_resource.organization,
                                )
                            )
                            data_normalization_registry.update({
                                registry_key: concept_map_version_for_normalization
                            })

                        # Is the concept_map_version_for_normalization valid?
                        concept_map_version_for_normalization = data_normalization_registry[registry_key]
                        if concept_map_version_for_normalization is None:
                            # We already messaged that the concept map for this resource and issue is missing
                            continue

                        # Inside the concept map version, we'll extract the source value set
                        source_value_set_uuid = (
                            concept_map_version_for_normalization.concept_map.source_value_set_uuid
                        )
                        if source_value_set_uuid not in source_value_set.keys():
                            try:
                                most_recent_active_source_value_set_version = (
                                    app.value_sets.models.ValueSet.load_most_recent_active_version_with_cache(
                                        source_value_set_uuid
                                    )
                                )
                                most_recent_active_source_value_set_version.expand(no_repeat=True)
                                source_value_set.update({
                                    source_value_set_uuid: most_recent_active_source_value_set_version
                                })
                            except BadRequest:
                                source_value_set.update({
                                    source_value_set_uuid: None
                                })
                                LOGGER.warning(
                                    f"No active published version of ValueSet with UUID: {source_value_set_uuid}"
                                )
                                all_skip_count += 1
                                continue

                        # Is the most_recent_active_source_value_set_version valid?
                        most_recent_active_source_value_set_version = source_value_set[source_value_set_uuid]
                        if most_recent_active_source_value_set_version is None:
                            # We messaged the first time we encountered the issue, do not repeat message
                            continue

                        # Identify the terminology inside the source value set
                        if source_value_set_uuid not in terminology_version.keys():
                            terminologies_in_source_value_set = (
                                most_recent_active_source_value_set_version.lookup_terminologies_in_value_set_version()
                            )

                            if len(terminologies_in_source_value_set) > 1:
                                raise Exception(
                                    "There should only be a single source terminology for a concept map used in data normalization"
                                )
                            if len(terminologies_in_source_value_set) == 0:
                                raise Exception(
                                    f"No terminologies in source value set {most_recent_active_source_value_set_version.uuid}"
                                )

                            current_terminology_version = terminologies_in_source_value_set[0]
                            terminology_for_value_set.update({
                                source_value_set_uuid: current_terminology_version
                            })
                            terminology_version.update({
                                str(current_terminology_version.uuid): current_terminology_version
                            })
                        current_terminology_version = terminology_for_value_set[source_value_set_uuid]

                        #  The custom terminology may have already passed its effective end date, so we might need to create a new version
                        terminology_to_load_to = (
                            current_terminology_version.version_to_load_new_content_to()
                        )

                        new_code_uuid = uuid.uuid4()
                        if processed_display is None:
                            processed_display = ''
                        new_code = Code(
                            code=processed_code,
                            display=processed_display,
                            system=None,
                            version=None,
                            terminology_version_uuid=terminology_to_load_to.uuid,
                            custom_terminology_code_uuid=new_code_uuid,
                        )

                        # Save the data linking this code back to its original error
                        # After de-duplication, we will look them up and insert them to the table
                        for issue_id in error_service_resource.issue_ids:
                            error_code_link_data.append(
                                {
                                    "issue_uuid": issue_id,
                                    "resource_uuid": error_service_resource.id,
                                    "environment": environment,
                                    "status": "pending",
                                    "code": json.dumps(processed_code) if type(processed_code) == dict else processed_code,
                                    "display": processed_display,
                                    "terminology_version_uuid": terminology_to_load_to.uuid,
                                    # Note: no currently supported resource requires the dependsOn data
                                    # but its part of the unique constraint to look up a row, so we include it
                                    "depends_on_property": "",
                                    "depends_on_system": "",
                                    "depends_on_value": "",
                                    "depends_on_display": ""
                                }
                            )

                        # Assemble additionalData from the raw resource.
                        # This is where we'll extract unit, value, referenceRange, and value[x] if available
                        resource_json = json.loads(error_service_resource.resource)
                        new_code.add_examples_to_additional_data(
                            unit=resource_json.get("unit"),
                            value=resource_json.get("value"),
                            reference_range=resource_json.get("referenceRange"),
                            value_quantity=resource_json.get("valueQuantity"),
                            value_boolean=resource_json.get("valueBoolean"),
                            value_string=resource_json.get("valueString"),
                            value_date_time=resource_json.get("valueDateTime"),
                            value_codeable_concept=resource_json.get("valueCodeableConcept"),
                        )

                        if terminology_to_load_to.uuid in new_codes_to_deduplicate_by_terminology:
                            new_codes_to_deduplicate_by_terminology[
                                terminology_to_load_to.uuid
                            ].append(new_code)
                        else:
                            new_codes_to_deduplicate_by_terminology[terminology_to_load_to.uuid] = [
                                new_code
                            ]

                # Step 3: Deduplicate the codes to avoid redundant data.
                deduped_codes_by_terminology = {}

                # Loop through codes, identify duplicates, and merge them.
                for (
                    terminology_uuid,
                    new_codes_to_deduplicate,
                ) in new_codes_to_deduplicate_by_terminology.items():
                    # Store duplicates in a dictionary with lists
                    dedup_dict = {}
                    for code in new_codes_to_deduplicate:
                        if code in dedup_dict:
                            dedup_dict[code].append(code)
                        else:
                            dedup_dict[code] = [code]

                    # Merge duplicates
                    deduped_codes = []
                    for duplicates in dedup_dict.values():
                        if len(duplicates) > 1:
                            merged_code = duplicates[0]
                            for i in range(1, len(duplicates)):
                                current_duplicate = duplicates[i]
                                # Merge in duplicates
                                if current_duplicate.additional_data is not None:
                                    merged_code.add_examples_to_additional_data(
                                        unit=current_duplicate.additional_data.get(
                                            "example_unit"
                                        ),
                                        value=current_duplicate.additional_data.get(
                                            "example_value"
                                        ),
                                        reference_range=current_duplicate.additional_data.get(
                                            "example_reference_range"
                                        ),
                                        value_quantity=current_duplicate.additional_data.get(
                                            "example_value_quantity"
                                        ),
                                        value_boolean=current_duplicate.additional_data.get(
                                            "example_value_boolean"
                                        ),
                                        value_string=current_duplicate.additional_data.get(
                                            "example_value_string"
                                        ),
                                        value_date_time=current_duplicate.additional_data.get(
                                            "example_value_date_time"
                                        ),
                                        value_codeable_concept=current_duplicate.additional_data.get(
                                            "example_codeable_concept"
                                        ),
                                    )
                            deduped_codes.append(merged_code)
                        else:
                            deduped_codes.append(duplicates[0])

                    deduped_codes_by_terminology[terminology_uuid] = deduped_codes

                # Step 4: Load the deduplicated codes into their respective terminologies.
                for terminology_version_uuid, code_list in deduped_codes_by_terminology.items():
                    if terminology_version_uuid not in terminology_version.keys():
                        terminology = Terminology.load_from_cache(terminology_version_uuid)
                        terminology_version.update({
                            terminology_version_uuid: terminology
                        })
                    terminology = terminology_version[terminology_version_uuid]

                    if len(code_list) > 0:
                        LOGGER.warning(
                            f"Loading {len(code_list)} new codes to terminology " +
                            f"{terminology.terminology} version {terminology.version}"
                        )
                        terminology.load_new_codes_to_terminology(
                            code_list, on_conflict_do_nothing=True
                        )

                # Step 5: Save the IDs of the original errors and link back to the codes
                conn = get_db()
                try:
                    if error_code_link_data:
                        # Create a temporary table and insert the data to it
                        conn.execute(
                            text(
                                """
                                CREATE TEMP TABLE temp_error_data (
                                    code text,
                                    display text,
                                    terminology_version_uuid UUID,
                                    depends_on_system text,
                                    depends_on_property text,
                                    depends_on_display text,
                                    depends_on_value text,
                                    issue_uuid UUID,
                                    resource_uuid UUID,
                                    environment text,
                                    status text
                                )
                                """
                            )
                        )

                        # Optimized bulk insert
                        conn.execute(temp_error_data.insert(), error_code_link_data)

                        # Insert from the temporary table, allowing the database to batch lookups
                        conn.execute(
                            text(
                                """
                                INSERT INTO custom_terminologies.error_service_issue
                                    (custom_terminology_code_uuid, issue_uuid, environment, status, resource_uuid)
                                SELECT c.uuid, t.issue_uuid, t.environment, t.status, t.resource_uuid
                                FROM temp_error_data t
                                JOIN custom_terminologies.code c 
                                ON c.code = t.code 
                                    AND c.display = t.display 
                                    AND c.terminology_version_uuid = t.terminology_version_uuid
                                    AND c.depends_on_system = t.depends_on_system
                                    AND c.depends_on_property = t.depends_on_property
                                    AND c.depends_on_display = t.depends_on_display
                                    AND c.depends_on_value = t.depends_on_value
                            ON CONFLICT do nothing
                            """
                        )
                    )

                    # Delete the temporary table
                    conn.execute(
                        text(
                            """
                            DROP TABLE IF EXISTS temp_error_data
                            """
                        )
                    )

                    if commit_changes:
                        conn.commit()
                except:  # uncaught exceptions can be so costly here, that a 'bare except' is acceptable, despite PEP 8: E722
                    conn.rollback()

                current_time = datetime.datetime.now()
                since_last_time = current_time - previous_time
                previous_time = current_time
                loop_total += since_last_time
                all_loop_total += loop_total
                all_loop_count += 1
                LOGGER.warning(
                    f"  {len(resources_with_errors)} errors received and loaded in {step_1}\n" +
                    f"  {len(new_codes_to_deduplicate_by_terminology)} new codes found and deduplicated in {step_2}\n" +
                    f"  at local time {current_time}, loop duration {loop_total}"
                )

    except ReadTimeout as rt:
        LOGGER.warning(
            f"""\nTask halted by ReadTimeout: {rt.message if hasattr(rt, "message") else ""}\n""" +
            f"  on: {rt.request.method} {rt.request.url}\n"+
            f"  at: {datetime.datetime.now()}\n\n\n"
        )
    except Exception as e:  # uncaught exceptions can be so costly, that a 'bare except' is fine, despite PEP 8: E722
        LOGGER.warning(
            f"\nTask halted by exception at Data Normalization Error Service " +
            f"Resource ID: {error_service_resource_id} Issue ID: {error_service_issue_id} " +
            f"while processing {input_fhir_resource} for organization: {organization_id}\n" +
            f"Exception may reflect a general, temporary problem, such as another service being unavailable.\n\n" +
            message_exception_summary(e)
        )
        # A full stack trace is necessary to pinpoint issues triggered by frequently used constructs like terminologies
        traceback.print_exception(*sys.exc_info())
    finally:
        time_end = datetime.datetime.now()
        time_elapsed = time_end - time_start
        if all_loop_count > 0:
            all_loop_average = all_loop_total / all_loop_count
            step_1_average = step_1_total / all_loop_count
            step_2_average = step_2_total / all_loop_count
        LOGGER.warning(
            f"\nDONE at local time {time_end}, duration {time_elapsed}\n" +
            f"  Load from: {DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}\n" +
            f"  Load to:   {DATABASE_HOST}\n\n" +
            f"Main loop skipped {all_skip_count} times (repeated report or FHIR resource not supported).\n" +
            f"Main loop ran {all_loop_count} times:\n" +
            f"  Average loop duration: {all_loop_average}\n" +
            f"  Average time to receive and load errors from service: {step_1_average}\n" +
            f"  Average time to extract, deduplicate, and load codes into terminologies: {step_2_average}\n\n\n"
        )


@ttl_cache()
def lookup_concept_map_version_for_data_element(
    data_element: str, organization: Organization
) -> "app.concept_maps.models.ConceptMapVersion":  # Full path avoids circular import
    """
    Returns the specific ConceptMapVersion currently in use for normalizing data with the specified resource_type and organization
    :param data_element:
    :param organization:
    :return:
    """
    LOGGER.warning(
        f"Checking registry for concept map entry for data element: {data_element} and organization: {organization.id}")
    # Load the data normalization registry
    registry = app.models.data_ingestion_registry.DataNormalizationRegistry()  # Full path avoids circular import
    registry.load_entries()

    # Filter based on resource type
    filtered_registry = [
        registry_entry
        for registry_entry in registry.entries
        if registry_entry.data_element == data_element
    ]

    # First, we will check to see if there's an organization-specific entry to use
    # If not, we will fall back to checking for a tenant-agnostic entry
    concept_map_version = None

    organization_specific = [
        registry_entry
        for registry_entry in filtered_registry
        if registry_entry.tenant_id == organization.id and registry_entry.registry_entry_type == "concept_map"
    ]
    if len(organization_specific) > 0:
        concept_map_version = organization_specific[
            0
        ].concept_map.most_recent_active_version

    # if concept_map_version is no longer none then we can skip the tenant_agnostic portion
    if concept_map_version is None:
        # Falling back to check for tenant agnostic entry
        tenant_agnostic = [
            registry_entry
            for registry_entry in filtered_registry
            if registry_entry.tenant_id is None and registry_entry.registry_entry_type == "concept_map"
        ]
        if len(tenant_agnostic) > 0:
            concept_map_version = tenant_agnostic[
                0
            ].concept_map.most_recent_active_version

    # If nothing is found, raise an appropriate error
    if concept_map_version is None:
        LOGGER.warning(f"No appropriate registry entry found for organization: {organization.id} and data element: {data_element}")
        return None

    return concept_map_version


def get_environment_from_service_url(url: str):
    """
    Extract the environment value from a service's URL string, such as: https://interop-validation.stage.projectronin.io
    @return "dev" or "stage" or "prod" or "" if nothing found
    """
    if ".dev." in url:
        return "dev"
    elif ".stage." in url:
        return "stage"
    elif ".prod." in url:
        return "prod"
    else:
        return ""


def get_outstanding_errors(
    registry: "app.models.data_ingestion_registry.DataNormalizationRegistry" = None,  # Full path avoids circular import
) -> List[Dict]:
    """
    This will generate a report on concept maps which have data
    waiting to be loaded into them in custom terminologies.

    It will provide a list of dictionaries, each specifying:
    - the concept map impacted, including title and uuid
    - the number of codes outstanding (waiting to be pulled into a new version of the concept map)
    - the date the oldest outstanding code was loaded to the custom terminology
    """
    conn = get_db()
    results = conn.execute(
        text(
            """
            -- Use a CTE to rank the concept map versions in descending order of their versions for each concept map
            WITH RankedCMVersions AS (
                SELECT 
                    concept_map_uuid, 
                    uuid AS concept_map_version_uuid,
                    version,
                    created_date,
                    source_value_set_version_uuid,
                    -- Rank the versions in descending order for each concept map
                    ROW_NUMBER() OVER(PARTITION BY concept_map_uuid ORDER BY version DESC) as rn
                FROM 
                    concept_maps.concept_map_version
            ),
            
            -- Use a CTE to get the latest version for each concept map
            LatestConceptMapVersions as (
                SELECT 
                    concept_map.title,
                    concept_map_uuid, 
                    concept_map_version_uuid,
                    RankedCMVersions.created_date as version_created_date,
                    source_value_set_version_uuid
                FROM 
                    RankedCMVersions
                JOIN
                    concept_maps.concept_map
                    on concept_map.uuid=concept_map_uuid
                WHERE 
                    rn = 1 -- Only pick the latest version
            ),
            
            -- Use a CTE to fetch unique concept maps that are being used for data normalization
            ConceptMapsForDataNormalization as (
                SELECT distinct concept_map_uuid 
                FROM data_ingestion.registry
            ),
            
            -- Use a CTE to get unique value set versions and their associated terminology versions
            TerminologiesInValueSetVersions as (
                SELECT distinct value_set_version, terminology_version 
                FROM value_sets.value_set_rule
            ),
            
            -- Use a CTE to rank the terminology versions in descending order for each terminology
            TerminologyRankedVersions AS (
                SELECT 
                    terminology,
                    uuid AS latest_version_uuid,
                    -- Rank the versions in descending order for each terminology
                    ROW_NUMBER() OVER(PARTITION BY terminology ORDER BY version DESC) as rn
                FROM 
                    public.terminology_versions
            ),
            
            -- Use a CTE to link the previous version UUID of a terminology to its latest version UUID
            TerminologyVersionLatestLookup as (
                SELECT 
                    tv.uuid AS previous_uuid,
                    lv.latest_version_uuid
                FROM 
                    public.terminology_versions tv
                JOIN 
                    TerminologyRankedVersions lv ON tv.terminology = lv.terminology
                WHERE 
                    lv.rn = 1
            )
            
            -- Main query to fetch the details for concept maps being used for data normalization 
            -- and the count of new codes from the latest terminology version post the creation of the concept map version
            SELECT 
                lcmv.title concept_map_title,
                lcmv.concept_map_uuid,
                lcmv.concept_map_version_uuid as latest_concept_map_version_uuid,
                term_latest_lookup.latest_version_uuid as latest_terminology_version,
                COUNT(ctc.uuid) AS code_count,
                -- This will fetch the oldest code creation date that's newer than the concept map version creation date
                MIN(ctc.created_date) AS oldest_new_code_date 
            FROM 
                LatestConceptMapVersions lcmv
            JOIN 
                TerminologiesInValueSetVersions tivs ON tivs.value_set_version = lcmv.source_value_set_version_uuid
            LEFT JOIN
                TerminologyVersionLatestLookup term_latest_lookup ON term_latest_lookup.previous_uuid = tivs.terminology_version 
            LEFT JOIN 
                custom_terminologies.code ctc ON ctc.terminology_version_uuid = term_latest_lookup.latest_version_uuid 
                                            AND ctc.created_date > lcmv.version_created_date
            WHERE 
                lcmv.concept_map_uuid in (SELECT concept_map_uuid FROM ConceptMapsForDataNormalization)
            GROUP BY 
                lcmv.title,
                lcmv.concept_map_uuid, 
                lcmv.concept_map_version_uuid, 
                lcmv.version_created_date, 
                lcmv.source_value_set_version_uuid,
                term_latest_lookup.latest_version_uuid
            HAVING COUNT(ctc.uuid) > 0;
            """
        )
    )
    list_result = []
    for row in results:
        if row.code_count > 0:
            list_result.append(
                {
                    "concept_map_title": row.concept_map_title,
                    "concept_map_uuid": row.concept_map_uuid,
                    "latest_concept_map_version_uuid": row.latest_concept_map_version_uuid,
                    "outstanding_code_count": row.code_count,
                    "oldest_new_code_date": row.oldest_new_code_date,
                }
          )
    return list_result


def get_all_unresolved_validation(environment: str):
    """
    Identify all open Data Validation Service resources with unresolved issues
    @param environment - "dev", "stage", or "prod"
    @return resource_uuid_list - lists of str identifying uuids for validation resources with unresolved issues
    """
    conn = get_db()
    issues_resources = conn.execute(
        text(
            """
            SELECT DISTINCT e.resource_uuid FROM 
            custom_terminologies.error_service_issue as e
            WHERE e.status <> 'resolved' and e.environment = :environment
            """
        ),
        {
            "environment": environment,
        }
    )
    resource_uuid_list = []
    for row in issues_resources:
        resource_uuid = row.resource_uuid
        if resource_uuid is not None:
            resource_uuid_list.append(str(resource_uuid))
    return resource_uuid_list


def set_issues_resolved(issue_uuid_list):
    """
    Assign an Informatics custom_terminologies.error_service_issue the final status of 'resolved'.
    @param issue_uuid_list: List of issue_uuid values from custom_terminologies.error_service_issue
    """
    conn = get_db()
    query = """
            UPDATE custom_terminologies.error_service_issue 
            SET status = 'resolved'
            WHERE issue_uuid IN :issue_uuid_list
            """
    converted_query = text(query).bindparams(bindparam(key="issue_uuid_list", expanding=True))
    conn.execute(converted_query, {"issue_uuid_list": issue_uuid_list})


def reprocess_resources(resource_uuid_list):
    """
    Request the Data Validation Error Service to reprocess resources that previously reported errors.
    @param resource_uuid_list: List of UUIDs of resources to reprocess in the Data Validation Error Service.
    """
    async def reprocess_all():
        reprocess_token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
        async with httpx.AsyncClient(timeout=60.0) as async_client:
            await asyncio.gather(*(reprocess_resource(
                resource_uuid,
                reprocess_token,
                async_client
            ) for resource_uuid in resource_uuid_list))

    asyncio.run(reprocess_all())


async def reprocess_resource(resource_uuid, token, client):
    """
    @param resource_uuid: UUID of the resource to reprocess in the Data Validation Error Service.
    @param token: Authorization token for the Data Validation Error Service.
    @param client: Client for the Data Validation Error Service.
    """
    try:
        await make_post_request_async(
            token=token,
            client=client,
            base_url=f"{DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}",
            api_url=f"/resources/{resource_uuid}/reprocess",
            params={},
        )
    except httpx.ConnectError:
        # If an error occurs on one resource, skip it
        LOGGER.warning(
            f"httpx.ConnectError, skipping load: Error Service Resource ID: {resource_uuid}"
        )
        return None


if __name__ == "__main__":
    # todo: clean out this method altogether, when a temporary, manual error load task is not needed.
    from app.database import get_db

    # Moved logging setup to here so it does not run in main program and cause duplicate logs

    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded.
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    # Per our usual practice, open a DatabaseHandler, that database calls within load_concepts_from_errors will re-use
    conn = get_db()

    # COMMENT the line below, for merge and normal use; uncomment when running the temporary error load task
    # load_concepts_from_errors(commit_changes=True)

    # UNCOMMENT the line below, for merges and normal use; comment out when running the temporary error load task
    load_concepts_from_errors(commit_changes=False)

    # load_concepts_from_errors ran rollback() and commit() where and as needed; now ask the DatabaseHandler to close()
    conn.close()


@deprecated(
    "This one-time repair function has been used already. "
    + "Posting it in a PR as an example of a pattern we probably need for the purge task."
)
@pytest.mark.skip(
    reason="External calls: Reads from the database, writes to the database. This is a utility, not a test." +
           "Do not write out to the database or write out to OCI data store, from tests. "
)
def test_norm_registry_null_environments():
    """
    Not a test. Really a tool for developers to perform a one-time correction in the error_service_issue table.
    Repeat test_norm_registry_null_environments() in each environment until there are no results to the first query.
    This function offers a useful pattern for using GET /resources/:resource_uuid to see whether a particular resource
    still exists on the Data Ingestion Error Validation Service. The pattern is that if a JSONDecodeError is raised
    by this GET it is because it returned an empty string. This particular GET does that when the resource is not found.
    """
    from app.database import get_db
    conn = get_db()
    query = """
        select distinct resource_uuid from 
        custom_terminologies.error_service_issue
        where environment is null
    """
    results = conn.execute(
        text(query)
    )
    print("\nResource UUIDs:")
    environment = get_environment_from_service_url(DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL)
    environment_uuid_list = []
    for row in results:
        resource_uuid = row.resource_uuid
        try:
            token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
            url = f"/resources/{resource_uuid}"
            with httpx.Client(timeout=60.0) as client:
                # try to GET this resource_uuid from the environment
                response = make_get_request(
                    token=token,
                    client=client,
                    base_url=DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL,
                    api_url=url
                )
                if response is not None:
                    # the GET response shows this resource_uuid was found in this environment
                    environment_uuid_list.append(str(resource_uuid))
        except JSONDecodeError:
            # the GET response was an empty string that JSON could not decode because the resource_uuid was not found
            continue
        except Exception as e:
            # the GET request experienced an uncaught exception that we need to study and/or prevent
            raise e
    # insert the environment name into the environment column for each row whose resource_uuid is in the uuid_list
    query = f"""
        update custom_terminologies.error_service_issue set environment = '{environment}'
        where resource_uuid in ('{"','".join(environment_uuid_list)}')
    """
    try:
        conn.execute(
            text(query)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise e
