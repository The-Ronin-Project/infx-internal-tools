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
from cachetools.func import ttl_cache
from decouple import config
from deprecated.classic import deprecated
from httpx import ReadTimeout
from httpcore import PoolTimeout as HttpcorePoolTimeout
from httpx import PoolTimeout as HttpxPoolTimeout
from sqlalchemy import text, Table, Column, MetaData, Text, bindparam
from sqlalchemy.dialects.postgresql import UUID as UUID_column_type
from werkzeug.exceptions import BadRequest

import app
import app.concept_maps.models
import app.models.data_ingestion_registry
import app.value_sets.models
from app.errors import BadDataError
from app.helpers.message_helper import (
    message_exception_summary,
    message_exception_classname,
)
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
PAGE_SIZE = 300

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


async def make_get_request_async(
    token, client: httpx.AsyncClient, base_url, api_url, params={}
):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


async def make_post_request_async(
    token, client: httpx.AsyncClient, base_url, api_url, params={}
):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.post(base_url + api_url, headers=headers, params=params)
    return response.json()


class ResourceType(Enum):
    OBSERVATION = "Observation"
    CONDITION = "Condition"
    MEDICATION = "Medication"
    LOCATION = "Location"
    PATIENT = "Patient"
    PRACTITIONER = "Practitioner"
    PRACTITIONER_ROLE = "PractitionerRole"  # For both use and system
    APPOINTMENT = "Appointment"
    DOCUMENT_REFERENCE = "DocumentReference"
    CARE_PLAN = "CarePlan"
    PROCEDURE = "Procedure" # Only code for now
    # TELECOM_USE = "Practitioner.telecom.use"  # Only in for testing until we have a real data type live


class IssueType(Enum):
    """
    Data Ingestion Validation Error Service Issue Types confirmed by Content and Interops to be of interest.

    The Interops Data Ingestion Validation Error Service defines Issue Types that may be detected while validating
    incoming FHIR resources against specific FHIR profiles such as R4, USCore, or Ronin Common Data Model profiles.
    Issue Types are unique strings. They are abbreviated, but reasonably meaningful, such as NOV_CONMAP_LOOKUP for
    "No value (required value missing) - Concept Map - Data Normalization Lookup". Even when a FHIR resource triggers
    multiple validation Issues, only 1 Error resource is created for that FHIR resource. Multiple Issues are attached
    to the 1 Error. Each Issue has an Issue Type, and each is likely to have a different Issue Type from the others.
    """
    NOV_CONMAP_LOOKUP = "NOV_CONMAP_LOOKUP"


class IncludeTenant(Enum):
    """
    Tenant IDs confirmed by Content and Interops to be of interest. Subset of "Organization Ids" list on Confluence.
    Link if needed: https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1737556005/Organization+Ids
    """
    MD_ANDERSON = "mdaoc"
    MD_ANDERSON_TEST = "5jzj62vp"
    PROVIDENCE_ST_JOHNS_PROD = "v7r1eczk"
    PROVIDENCE_ST_JOHNS_TEST = "1xrekpx5"
    EPIC_API_SANDBOX = "apposnd"
    CERNER_API_SANDBOX = "ejh3j95h"
    CERNER_APP_VALIDATION = "ggwadc8y"
    RONIN_EPIC_MOCK_EHR = "ronin"
    RONIN_CERNER_MOCK_EHR = "ronincer"


class ExcludeTenant(Enum):
    """
    Tenant IDs confirmed by Content as intentionally not having concept maps created. Also listed on "Organization Ids".
    """
    CERNER_SALES_DOMAIN = "tv6fx8pm"
    LEGACY_RONIN_DEMO = "demo"
    LEGACY_RONIN_DEV = "peeng"


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
        # If a timeout occurs on one resource, skip it
        except (
            httpx.ConnectError
            or HttpxPoolTimeout
            or HttpcorePoolTimeout
            or BadDataError
            or asyncio.exceptions.CancelledError
            or TimeoutError
            or ReadTimeout
        ) as e:
            LOGGER.warning(
                f"{message_exception_summary(e)}, skipping load of issue data for Error Service Resource ID: {self.id} - {self.resource_type.value} for organization {self.organization.id}"
            )
            return None
        except Exception as e:
            intro = f"{message_exception_classname(e)}, skipping load of issue data for Error Service Resource ID: {self.id} - {self.resource_type.value} for organization {self.organization.id}"
            LOGGER.warning(intro)
            if "Timeout" in intro:
                return None
            else:
                raise e

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
    "temp_error_data",
    metadata,
    Column("code", Text),
    Column("display", Text),
    Column("terminology_version_uuid", UUID_column_type),
    Column("depends_on_system", Text),
    Column("depends_on_property", Text),
    Column("depends_on_display", Text),
    Column("depends_on_value", Text),
    Column("issue_uuid", UUID_column_type),
    Column("resource_uuid", UUID_column_type),
    Column("status", Text),
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
        "system": re.compile(rf"^{resource_type}\.telecom\[(\d+)\]\.system$"),
        "use": re.compile(rf"^{resource_type}\.telecom\[(\d+)\]\.use$"),
    }

    # Initialize variables for processed code and display
    processed_code = None
    processed_display = None

    # Loop through the keys 'system' and 'use' to find matches
    for key in ["system", "use"]:
        match = regex_patterns[key].search(location)
        if match:
            list_index = int(match.group(1))
            raw_code = raw_resource["telecom"][list_index][key]
            processed_code = raw_code
            processed_display = raw_code

    return processed_code, processed_display


def load_concepts_from_errors(
    commit_changes=True,
    page_size: int = None,
    requested_organization_id: str = None,
    requested_resource_type: str = None,
    requested_issue_type: str = None,
):
    """
    Extracts specific concepts from a list of errors and saves them to a custom terminology.

    This function processes errors to identify and extract relevant concepts. It then organizes
    these concepts by the originating organization and type of resource they pertain to. The
    results are saved back to the appropriate custom terminology. The Mapping Request Service sends Mapping Requests
    into Informatics tooling, such as the Retool Mapping Request Dashboard where outstanding Mapping Requests
    can be viewed, triaged, and loaded into ConceptMaps. Upon publication of a new ConceptMap version, the
    Mapping Request Service reaches out to the Interops Service again, requesting reprocessing of the data that
    originated that Mapping Request. Because of the new ConceptMap, this data now successfully enters the system.
    ```
    Parameters:
        commit_changes (bool, optional): Whether to commit after each batch
        page_size (int, optional): HTTP GET page size, if empty, use the default PAGE_SIZE
        requested_organization_id (str): If non-empty, get errors only for the organization_id listed; if empty, get all
        requested_resource_type (str): Get errors only for the ResourceType enum string listed; if empty, get all
        requested_issue_type (str): Get errors only for the IssueType enum string listed; if empty, get all

    Procedure:
        1. Fetch resources that have encountered errors.
        2. Extract relevant codes from the resource.
        3. Deduplicate the codes to avoid redundancy.
        4. Load the unique codes into their respective terminologies.
    """
    # Step 0: Initialize and setup

    # API call paging
    if page_size is None:
        page_size = PAGE_SIZE

    # resource type
    unsupported_resource_types = []
    supported_resource_types = [r.value for r in ResourceType]
    if requested_resource_type is not None and requested_resource_type not in supported_resource_types:
        LOGGER.warning(
            f"Support for the {requested_resource_type} resource type has not been implemented"
        )
        return
    if requested_resource_type is None:
        requested_resource_types = supported_resource_types
    else:
        requested_resource_types = [requested_resource_type]

    # issue type
    supported_issue_types = [u.value for u in IssueType]
    if requested_issue_type is not None and requested_issue_type not in supported_issue_types:
        LOGGER.warning(
            f"Support for the {requested_issue_type} issue type has not been implemented"
        )
        return
    if requested_issue_type is None:
        requested_issue_types = supported_issue_types
    else:
        requested_issue_types = [requested_issue_type]

    # organization IDs
    unsupported_organization_ids = [x.value for x in ExcludeTenant]
    supported_organization_ids = [i.value for i in IncludeTenant]
    if requested_organization_id in unsupported_organization_ids or (
         requested_organization_id not in supported_organization_ids
    ):
        if requested_organization_id is not None:
            LOGGER.warning(
                f"The Content team does not provide concept maps for the tenant ID: {requested_organization_id}"
            )
            return
    if requested_organization_id is None:
        organization_ids = supported_organization_ids
    else:
        organization_ids = [requested_organization_id]

    # Logging variables
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
    total_count_loaded_codes = 0

    # Start logging
    LOGGER.warning(
        f"Begin loading data from the Interops Data Ingestion Validation Error Service at local time {time_start}\n"
        + f"  Load from: {DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}\n"
        + f"  Load to:   {DATABASE_HOST}\n\n"
        + f"  Settings: \n"
        + f"    commit_changes={commit_changes}\n"
        + f"    page_size={page_size}\n"
        + f"    requested_organization_id={requested_organization_id}\n"
        + f"    requested_resource_type={requested_resource_type}\n"
        + f"    requested_issue_type={requested_issue_type}\n\n"
        + f"Main loop:\n"
    )

    # Local caches
    terminology_version = dict()

    # todo: (feature, not bug) When we support multiple issue types, cache the Error resource objects we load for use,
    # because the GET call returns each discovered Error resource object in full, with all of that resource's issues of
    # all issue types, even if we filtered the GET call by issue type. We get the entire object back each time!
    # That is, each Error resource we get back from a GET call is guaranteed to contain at least one issue that matches
    # the issue type we requested, but it could contain multiple other issue types as well. When we support multiple
    # issue types we will likely see the same Error resource returned by our calls multiple times. We should check this
    # by Error resource id and only load Error resource data as an object once, cache it, and re-use it per issue type.
    #
    # By contrast, below we assume only 1 issue type ever, so if we saw an Error before, we already processed: skip it.
    #
    # Resources we have already processed in this environment
    environment = get_environment_from_service_url(
        DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL
    )
    error_service_resource_ids = get_all_unresolved_validation(environment)

    # Timeouts for API calls to the error service
    timeout_config = httpx.Timeout(timeout=600.0, pool=600.0, read=600.0, connect=600.0)

    # Main loop
    try:
        for input_resource_type in requested_resource_types:
            for organization_id in organization_ids:
                for input_issue_type in requested_issue_types:
                    try:
                        # Step 1: Fetch resources that have encountered errors.
                        token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
                        with (httpx.Client(timeout=timeout_config) as client):

                            # Collecting all resources with errors through paginated API calls.
                            rest_api_params = dict(
                                {
                                    "order": "ASC",
                                    "limit": page_size,
                                    "issue_type": input_issue_type,
                                    "status": "REPORTED",  # exclude: IGNORED - ADDRESSING - REPROCESSED - CORRECTED
                                    "resource_type": input_resource_type,
                                    "organization_id": organization_id,
                                }
                            )

                            # Continuously fetch resources until all pages have been retrieved.
                            all_resources_fetched = False
                            while all_resources_fetched is False:
                                # Retrieve the first page of resources
                                try:
                                    response = make_get_request(
                                        token=token,
                                        client=client,
                                        base_url=DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL,
                                        api_url="/resources",
                                        params=rest_api_params,
                                    )
                                except (httpx.ConnectError or HttpxPoolTimeout or HttpcorePoolTimeout or BadDataError or asyncio.exceptions.CancelledError or TimeoutError or ReadTimeout) as e:
                                    LOGGER.warning(
                                        f"{message_exception_summary(e)}, skipping load of {page_size} resources of type {input_resource_type} for {input_issue_type} issue for organization {organization_id}"
                                    )
                                    continue
                                except Exception as e:
                                    intro = f"{message_exception_classname(e)}, skipping load of {page_size} resources of type {input_resource_type} for issue {input_issue_type} for organization {organization_id}"
                                    if "Timeout" not in intro:
                                        info = "".join(traceback.format_exception(*sys.exc_info()))
                                        if "Expecting value: line 1 column 1 (char 0)" in intro:
                                            message = f"{intro}\n{info}\nThis error means a required value was empty."
                                        else:
                                            message = f"{intro}\n{info}"
                                    else:
                                        message = intro
                                    LOGGER.warning(message)
                                    continue

                                # Here is the first page of resources
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

                                    # We know the organization_id and input_resource_type from the loop
                                    organization = Organization(organization_id)
                                    fhir_resource_type = None
                                    for resource_type_option in ResourceType:
                                        if resource_type_option.value == input_resource_type:
                                            fhir_resource_type = resource_type_option
                                            input_fhir_resource = input_resource_type
                                            break

                                    # todo: (feature, not bug) see todo note where error_service_resource_ids is defined
                                    # Did we see this error_service_resource_id before? if so, it is being handled: skip
                                    error_service_resource_id = resource_data.get("id")
                                    if error_service_resource_id in error_service_resource_ids:
                                        all_skip_count += 1
                                        continue

                                    # Create an ErrorServiceResource object with the values we have received.
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
                                        await asyncio.gather(
                                            *(
                                                error_resource.load_issues(client=async_client)
                                                for error_resource in error_resources
                                            )
                                        )

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
                                # Key: terminology_version_uuid, Value: list containing the codes to load to terminology
                                new_codes_to_deduplicate_by_terminology = {}

                                # Create a list to put the data linking codes back to the issues/errors they came from
                                error_code_link_data = []

                                # Walk through all the error service resources
                                for error_service_resource in error_resources:
                                    (
                                        terminology_version,
                                        new_codes_to_deduplicate_by_terminology,
                                        error_code_link_data,
                                        skip_count
                                    ) = extract_error_resource_data(
                                        error_service_resource,
                                        input_fhir_resource,
                                        organization_id,
                                        input_issue_type,
                                        terminology_version,
                                        new_codes_to_deduplicate_by_terminology,
                                        error_code_link_data,
                                    )
                                    all_skip_count += skip_count

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
                                for (
                                    terminology_version_uuid,
                                    code_list,
                                ) in deduped_codes_by_terminology.items():
                                    if terminology_version_uuid not in terminology_version.keys():
                                        terminology = Terminology.load_from_cache(
                                            terminology_version_uuid
                                        )
                                        terminology_version.update(
                                            {terminology_version_uuid: terminology}
                                        )
                                    terminology = terminology_version[terminology_version_uuid]

                                    if len(code_list) > 0:
                                        LOGGER.warning(
                                            f"Attempting to load {len(code_list)} new codes to terminology "
                                            + f"{terminology.terminology} version {terminology.version}"
                                        )
                                        inserted_count = terminology.load_new_codes_to_terminology(
                                            code_list, on_conflict_do_nothing=True
                                        )
                                        total_count_loaded_codes += inserted_count
                                        LOGGER.warning(
                                            f"Actually inserted {inserted_count} codes to "
                                            + f"{terminology.terminology} version {terminology.version} "
                                            + "after deduplication"
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
                                except Exception as e:
                                    LOGGER.warning(
                                        f"{message_exception_summary(e)} adding data to custom_terminologies.error_service_issue"
                                    )
                                    conn.rollback()
                                    raise e

                                current_time = datetime.datetime.now()
                                since_last_time = current_time - previous_time
                                previous_time = current_time
                                loop_total += since_last_time
                                all_loop_total += loop_total
                                all_loop_count += 1
                                LOGGER.warning(
                                    f"  {len(resources_with_errors)} responses received and loaded in {step_1}\n"
                                    + f"  {total_count_loaded_codes} new codes found and deduplicated in {step_2}\n"
                                    + f"  loop {all_loop_count} done at time {current_time.time()}, duration {loop_total}"
                                )

                    # at the innermost loop level, we have all the variables defined for a useful message
                    except Exception as e:
                        LOGGER.warning(
                            f"\n{message_exception_summary(e)} at Interops Data Ingestion Validation Error Service "
                            + f"Resource ID: {error_service_resource_id} Issue ID: {error_service_issue_id} "
                            + f"while processing {input_fhir_resource} issue {input_issue_type} "
                            + f"""for organization: {organization_id}"""
                        )
                        raise e

    # Complete the main loop
    except Exception as e:
        info = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(
            f"""\nTask halted by {message_exception_classname(e)}\n{info}"""
        )

    # Complete the log
    finally:
        time_end = datetime.datetime.now()
        time_elapsed = time_end - time_start
        if all_loop_count > 0:
            all_loop_average = all_loop_total / all_loop_count
            step_1_average = step_1_total / all_loop_count
            step_2_average = step_2_total / all_loop_count
        if len(unsupported_resource_types) > 0:
            intro = "Encountered these unsupported FHIR resource types: "
            list_of_unsupported = (
                f"""{intro}\n  {",".join(unsupported_resource_types)}"""
            )
        else:
            list_of_unsupported = ""
        LOGGER.warning(
            f"\nDONE at local time {time_end}, time since start {time_elapsed}\n"
            + f"  Load from: {DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}\n"
            + f"  Load to:   {DATABASE_HOST}\n\n"
            + f"Main loop skipped {all_skip_count} times (repeated report or FHIR resource not supported).\n"
            + f"Main loop ran {all_loop_count} times at page size {page_size}:\n"
            + f"  Average loop duration: {all_loop_average}\n"
            + f"  Average time to receive and load errors from service: {step_1_average}\n"
            + f"  Average time to extract, deduplicate, and load codes into terminologies: {step_2_average}\n"
            + f"{list_of_unsupported}\n\n"
        )


def extract_error_resource_data(
    error_service_resource: ErrorServiceResource,
    fhir_resource_type: str,
    input_issue_type: str,
    organization_id: str,
    terminology_version: dict,
    new_codes_to_deduplicate_by_terminology: dict,
    error_code_link_data: list,
) -> (dict, dict, list, int):
    """
    Extract the FHIR resource validation issue data from a single ErrorServiceResource item from Interops
    @param error_service_resource: object from the main loop, one of many ErrorServiceResource item from Interops
    @param fhir_resource_type: control value from the main loop through all error resources
    @param input_issue_type: control value from the main loop through all error resources
    @param organization_id: control value from the main loop through all error resources
    @param terminology_version: discovered Terminology objects for deduplication
    @param new_codes_to_deduplicate_by_terminology: holds codes that need deduplication
            Key: terminology_version_uuid, Value: list containing the codes to load to terminology
    @param error_code_link_data: data linking codes back to the issues/errors they came from
    @return: (terminology_version, new_codes_to_deduplicate_by_terminology, error_code_link_data, all_skip_count)
        terminology_version (dict): updated dict of Terminology objects discovered in processing
            Key: terminology_version_uuid, Value: Terminology object for that uuid
        new_codes_to_deduplicate_by_terminology (dict): holds codes that need deduplication
            Key: terminology_version_uuid, Value: list containing the codes to load to terminology
        error_code_link_data (list): data linking codes back to the issues/errors they came from
        all_skip_count: count of resources skipped for transitory (i.e. network connection) reasons
    """
    # Log setup
    all_skip_count = 0
    tenant_load_json_format_error_reported = []

    # Local caches
    data_normalization_registry = dict()
    source_value_set = dict()
    terminology_for_value_set = dict()

    # Get the FHIR resource information from the error resource
    resource_type = error_service_resource.resource_type
    try:
        raw_resource = json.loads(error_service_resource.resource)
    except Exception as e:
        intro = f"{message_exception_classname(e)} loading {fhir_resource_type} data for issue {input_issue_type} for {organization_id}: 1 report for 1+ cases"
        if intro not in tenant_load_json_format_error_reported:
            error_code = (
                "NormalizationErrorService.load_concepts_from_errors"
            )
            info = "".join(traceback.format_exception(*sys.exc_info()))
            LOGGER.warning(f"{intro}\n{info}\n")
            tenant_load_json_format_error_reported.append(intro)
            raise BadDataError(
                code=error_code,
                description=intro,
                errors=message_exception_summary(e),
            )

    # Our GET returned only the Error resources that had at least one issue of the
    # issue tye we requested. Now extract only the issues of that type, to make a list.
    requested_issues = error_service_resource.filter_issues_by_type(
        input_issue_type
    )
    for issue in requested_issues:
        # save for logging
        error_service_issue_id = str(issue.id)

        # data_element is the location without any brackets or indices
        # i.e. if location is Patient.telecom[1].system,
        # then data_element is Patient.telecom.system
        location = issue.location
        element = re.sub(r"\[\d+\]", "", location)
        match = re.search(r"\[(\d+)\]", location)
        # Extract any index that may be present in the location string
        index = None
        if match is not None:
            index = int(match.group(1))

        # Based on resource_type, identify coding that needs to get into the concept map
        # There's something unique about the handling for every resource_type we support
        # Condition
        if resource_type == ResourceType.CONDITION:
            # Condition.code is a CodeableConcept
            raw_code = raw_resource["code"]
            processed_code = raw_code
            processed_display = raw_code.get("text")

        # Medication
        elif resource_type == ResourceType.MEDICATION:
            # Medication.code is a CodeableConcept
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

            # Observation.component.code is a CodeableConcept - location has an index
            elif element == "Observation.component.code":
                if index is None:
                    continue
                if (
                        "component" not in raw_resource
                        or index not in raw_resource["component"]
                        or "code" not in raw_resource["component"][index]
                ):
                    continue
                raw_code = raw_resource["component"][index]["code"]
                processed_code = raw_code
                processed_display = raw_code.get("text")

            # Observation.component.value is a CodeableConcept - location will have an index
            elif element == "Observation.component.value":
                if index is None:
                    continue
                if (
                        "component" not in raw_resource
                        or index not in raw_resource["component"]
                        or "valueCodeableConcept"
                        not in raw_resource["component"][index]
                ):
                    continue
                raw_code = raw_resource["component"][index][
                    "valueCodeableConcept"
                ]
                processed_code = raw_code
                processed_display = raw_code.get("text")
            else:
                LOGGER.warning(
                    f"Unrecognized location for Observation error: {location}"
                )

        # Procedure
        elif resource_type == ResourceType.PROCEDURE:
            # Procedure.code is a CodeableConcept
            if element == "Procedure.code":
                if "code" not in raw_resource:
                    continue
                raw_code = raw_resource["code"]
                processed_code = raw_code
                processed_display = raw_code.get("text")

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

        # Use extract_telecom_data function to extract the codes from each resource type
        # Extract codes for issues from all resource types for ContactPoint.system, .use

        # Patient
        elif resource_type == ResourceType.PATIENT:
            processed_code, processed_display = extract_telecom_data(
                "Patient", location, raw_resource
            )

        # Practitioner
        elif resource_type == ResourceType.PRACTITIONER:
            processed_code, processed_display = extract_telecom_data(
                "Practitioner", location, raw_resource
            )

        # PractitionerRole
        elif resource_type == ResourceType.PRACTITIONER_ROLE:
            processed_code, processed_display = extract_telecom_data(
                "PractitionerRole", location, raw_resource
            )

        # Location
        elif resource_type == ResourceType.LOCATION:
            processed_code, processed_display = extract_telecom_data(
                "Location", location, raw_resource
            )

        # Case not yet implemented OR raw_resource is None (raw_resource data load failed)
        else:
            LOGGER.warning(
                f"Support for the {fhir_resource_type} resource type at location {element} has not been implemented"
            )
            all_skip_count += 1
            continue

        # Lookup the concept map version used to normalize this type of resource
        # So that we can then identify the correct terminology to load the new coding to

        # Note that some normalization registry data_element strings need adjustment.
        if (
                element == "Observation.value"
                or element == "Observation.component.value"
        ):
            data_element = f"{element}CodeableConcept"
        else:
            data_element = element

        # Get the concept_map_version_for_normalization
        registry_key = f"{organization_id} {data_element}"
        if registry_key not in data_normalization_registry.keys():
            concept_map_version_for_normalization = (
                lookup_concept_map_version_for_data_element(
                    data_element=data_element,
                    organization=error_service_resource.organization,
                )
            )
            data_normalization_registry.update(
                {registry_key: concept_map_version_for_normalization}
            )

        # Is the concept_map_version_for_normalization valid?
        concept_map_version_for_normalization = (
            data_normalization_registry[registry_key]
        )
        if concept_map_version_for_normalization is None:
            # per Content team, desired action is continue (stop loop, process next error)
            continue

        # Inside the concept map version, we'll extract the source value set
        source_value_set_uuid = (
            concept_map_version_for_normalization.concept_map.source_value_set_uuid
        )
        if source_value_set_uuid not in source_value_set.keys():
            try:
                most_recent_active_source_value_set_version = app.value_sets.models.ValueSet.load_most_recent_active_version_with_cache(
                    source_value_set_uuid
                )
                most_recent_active_source_value_set_version.expand(
                    no_repeat=True
                )
                source_value_set.update(
                    {
                        source_value_set_uuid: most_recent_active_source_value_set_version
                    }
                )
            except BadRequest:
                source_value_set.update({source_value_set_uuid: None})
                LOGGER.warning(
                    f"No active published version of ValueSet with UUID: {source_value_set_uuid}"
                )
                all_skip_count += 1
                continue
            except Exception as e:
                LOGGER.warning(
                    f"{message_exception_summary(e)} loading ValueSet with UUID: {source_value_set_uuid}"
                )
                all_skip_count += 1
                raise e

        # Is the most_recent_active_source_value_set_version valid?
        most_recent_active_source_value_set_version = source_value_set[
            source_value_set_uuid
        ]
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

            current_terminology_version = (
                terminologies_in_source_value_set[0]
            )
            terminology_for_value_set.update(
                {source_value_set_uuid: current_terminology_version}
            )
            terminology_version.update(
                {
                    str(
                        current_terminology_version.uuid
                    ): current_terminology_version
                }
            )
        current_terminology_version = terminology_for_value_set[
            source_value_set_uuid
        ]

        # The custom terminology may have already passed its end date, check it
        terminology_to_load_to = (
            current_terminology_version.version_to_load_new_content_to()
        )

        new_code_uuid = uuid.uuid4()
        if processed_display is None:
            processed_display = ""
        new_code = Code(
            code=processed_code,
            display=processed_display,
            system=None,
            version=None,
            terminology_version_uuid=terminology_to_load_to.uuid,
            custom_terminology_code_uuid=new_code_uuid,
        )

        # Save the data linking this code back to its original error
        # After deduplication, we will look them up and insert them to the table
        environment = get_environment_from_service_url(
            DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL
        )
        for issue_id in error_service_resource.issue_ids:
            error_code_link_data.append(
                {
                    "issue_uuid": issue_id,
                    "resource_uuid": error_service_resource.id,
                    "environment": environment,
                    "status": "pending",
                    "code": json.dumps(processed_code)
                    if type(processed_code) == dict
                    else processed_code,
                    "display": processed_display,
                    "terminology_version_uuid": terminology_to_load_to.uuid,
                    # todo: no currently supported resource requires the dependsOn data
                    # but it is part of the unique constraint to look up a row, so use it
                    "depends_on_property": "",
                    "depends_on_system": "",
                    "depends_on_value": "",
                    "depends_on_display": "",
                }
            )

        # Assemble additionalData from the raw resource.
        # This is where we'll extract unit, value, referenceRange, and value[x] if available
        new_code.add_examples_to_additional_data(
            unit=raw_resource.get("unit"),
            value=raw_resource.get("value"),
            reference_range=raw_resource.get("referenceRange"),
            value_quantity=raw_resource.get("valueQuantity"),
            value_boolean=raw_resource.get("valueBoolean"),
            value_string=raw_resource.get("valueString"),
            value_date_time=raw_resource.get("valueDateTime"),
            value_codeable_concept=raw_resource.get(
                "valueCodeableConcept"
            ),
        )

        if (
                terminology_to_load_to.uuid
                in new_codes_to_deduplicate_by_terminology
        ):
            new_codes_to_deduplicate_by_terminology[
                terminology_to_load_to.uuid
            ].append(new_code)
        else:
            new_codes_to_deduplicate_by_terminology[
                terminology_to_load_to.uuid
            ] = [new_code]

    return terminology_version, new_codes_to_deduplicate_by_terminology, error_code_link_data, all_skip_count

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
        f"Checking registry for concept map entry for data element: {data_element} and organization: {organization.id}"
    )
    # Load the data normalization registry
    registry = (
        app.models.data_ingestion_registry.DataNormalizationRegistry()
    )  # Full path avoids circular import
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
        if registry_entry.tenant_id == organization.id
        and registry_entry.registry_entry_type == "concept_map"
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
            if registry_entry.tenant_id is None
            and registry_entry.registry_entry_type == "concept_map"
        ]
        if len(tenant_agnostic) > 0:
            concept_map_version = tenant_agnostic[
                0
            ].concept_map.most_recent_active_version

    # If nothing is found (result is None) then per Content team, desired action here is to return None (ignore)
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
        },
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
    converted_query = text(query).bindparams(
        bindparam(key="issue_uuid_list", expanding=True)
    )
    conn.execute(converted_query, {"issue_uuid_list": issue_uuid_list})


def reprocess_resources(resource_uuid_list):
    """
    Request the Data Validation Error Service to reprocess resources that previously reported errors.
    @param resource_uuid_list: List of UUIDs of resources to reprocess in the Data Validation Error Service.
    """

    async def reprocess_all():
        reprocess_token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
        # todo: any performance improvements we discover for GET, apply here for POST
        async with httpx.AsyncClient(timeout=60.0) as async_client:
            await asyncio.gather(
                *(
                    reprocess_resource(resource_uuid, reprocess_token, async_client)
                    for resource_uuid in resource_uuid_list
                )
            )

    asyncio.run(reprocess_all())


def reprocess_errors_for_published_concept_maps():
    """
    One time process to purge errors from daily load.
    """
    conn = get_db()
    resource_uuid_query = conn.execute(
        text(
            """
            SELECT distinct esi.resource_uuid FROM   
            concept_maps.concept_relationship cr  
        JOIN   
            concept_maps.source_concept sc ON cr.source_concept_uuid = sc.uuid  
        JOIN   
            concept_maps.concept_map_version cmv ON sc.concept_map_version_uuid = cmv.uuid  
        JOIN   
            custom_terminologies.error_service_issue esi ON sc.custom_terminology_uuid = esi.custom_terminology_code_uuid  
        WHERE   
            cmv.status = 'active'
            AND esi.status = 'pending'  
            AND EXISTS (  
                SELECT 1  
                FROM concept_maps.concept_relationship cr_exist  
                WHERE cr_exist.source_concept_uuid = sc.uuid  
            )  
    """
        )
    )
    resource_uuid_list = []
    for row in resource_uuid_query:
        resource_uuid = row.resource_uuid
        if resource_uuid is not None:
            resource_uuid_list.append(str(resource_uuid))

    def chunked(iterable, chunk_size):
        """Yield successive chunk_size chunks from iterable."""
        for i in range(0, len(iterable), chunk_size):
            yield iterable[i : i + chunk_size]

    chunk_size = 25  # Set the chunk size
    for chunk in chunked(resource_uuid_list, chunk_size):
        reprocess_resources(chunk)


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
    except (
        httpx.ConnectError
        or HttpxPoolTimeout
        or HttpcorePoolTimeout
        or BadDataError
        or asyncio.exceptions.CancelledError
        or TimeoutError
        or ReadTimeout
    ) as e:
        # If an error occurs on one resource, skip it
        LOGGER.warning(
            f"{message_exception_summary(e)}, skipping reprocess resource: Error Service Resource ID: {resource_uuid}"
        )
        return None
    except Exception as e:
        intro = f"{message_exception_classname(e)}, skipping reprocess resource: Error Service Resource ID: {resource_uuid}"
        stacktrace = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(f"{intro}\n{stacktrace}")
        return None


if __name__ == "__main__":
    # todo: clean out this method altogether, when a temporary, manual error load task is not needed.
    from app.database import get_db

    # Moved logging setup to here so it does not run in main program and cause duplicate logs

    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    # Per our usual practice, open a DatabaseHandler, that database calls within load_concepts_from_errors will re-use
    conn = get_db()

    # Instructions for use:
    # INPUT page_size and/or requested_organization_id and/or requested_resource_type and/or requested_issue_type.
    #     requested_organization_id: Confluence page called "Organization Ids" under "Living Architecture" lists them
    #     requested_resource_type: must be a type load_concepts_from_errors() already supports (see ResourceType enum)
    #     requested_issue_type: must be a type load_concepts_from_errors() already supports (see IssueType enum)
    # COMMENT the line below, for merge and normal use; uncomment when running the temporary error load task
    # load_concepts_from_errors(commit_changes=True)

    # UNCOMMENT the line below, for merges and normal use; comment out when running the temporary error load task
    load_concepts_from_errors(commit_changes=False)

    # load_concepts_from_errors ran rollback() and commit() where and as needed; now ask the DatabaseHandler to close()
    conn.close()
