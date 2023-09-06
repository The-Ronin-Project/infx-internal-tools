import datetime
import json
from uuid import UUID
from typing import Dict, Tuple, List, Optional
from enum import Enum
from dataclasses import dataclass
from functools import lru_cache
import warnings
import logging

from decouple import config
import requests

from app.models.models import Organization
from app.models.codes import Code
from app.terminologies.models import Terminology
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.concept_maps.models import ConceptMapVersion, ConceptMap
from app.value_sets.models import ValueSet, ValueSetVersion

DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL", default=""
)
CLIENT_ID = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_ID", default=""
)
CLIENT_SECRET = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_SECRET", default=""
)
AUTH_AUDIENCE = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_AUDIENCE", default=""
)
AUTH_URL = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_AUTH_URL", default=""
)

LOGGER = logging.getLogger()

LOGGER.setLevel("INFO")


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
    headers = {"content-type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    try:
        token = response.json()["access_token"]
    except (KeyError, ValueError) as e:
        LOGGER.error("Failed to get token")
        LOGGER.debug(response.content)
        raise e
    return token


def get_client(token) -> requests.Session:
    """
    Configure the client for the error normalization service.
    """
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    session = requests.Session()
    session.headers.update(headers)
    return session


def convert_string_to_datetime_or_none(input_string):
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")


# Function to use the token to access the API
def make_get_request(token, client, base_url, api_url, params={}):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json"
    }
    response = client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


class ResourceType(Enum):
    OBSERVATION = "Observation"
    CONDITION = "Condition"
    MEDICATION = "Medication"
    LOCATION = "Location"
    PATIENT = "Patient"
    APPOINTMENT = "Appointment"
    DOCUMENT_REFERENCE = "DocumentReference"
    TELECOM_USE = "Practitioner.telecom.use"  # Only in for testing until we have a real data type live


@dataclass
class ErrorServiceResource:
    """
    We'll fill this out later...
    """

    id: UUID
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
            id=UUID(resource_data.get("id")),
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

    def load_issues(self, client):
        # Call the endpoint
        get_issues_for_resource = make_get_request(
            token=self.token,
            client=client,
            base_url=f"{DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}",
            api_url=f"/resources/{self.id}/issues",
            params={},
        )

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


@dataclass
class ErrorServiceIssue:
    """
    To be filled
    """

    id: UUID
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
            id=UUID(issue_data.get("id")),
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


def load_concepts_from_errors():
    """
        Extracts specific concepts from a list of errors and saves them to a custom terminology.

        This function processes errors to identify and extract relevant concepts. It then organizes
        these concepts by the originating organization and type of resource they pertain to. The
        results are saved back to the appropriate custom terminology.

        Parameters:
            terminology_whitelist (list, optional): A list of URIs (fhir_uri) specifying the terminologies
                to which data can be loaded. If not provided, uses a default whitelist.

        Procedure:
            1. Fetch resources that have encountered errors.
            2. Process these resources to identify the source of the error.
            3. Extract relevant codes from the resource.
            4. Deduplicate the codes to avoid redundancy.
            5. Load the unique codes into their respective terminologies.
        """

    # Step 1: Fetch resources that have encountered errors.
    token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
    client = get_client(token)

    # Collecting all resources with errors through paginated API calls.
    # resources_with_errors = [] # Moving into loop for batch test
    PAGE_SIZE = 2000
    rest_api_params = {
        "order": "ASC",
        "limit": PAGE_SIZE,
        "issue_type": "NOV_CONMAP_LOOKUP",
        "resource_type": "DocumentReference"  # todo: remove hard-coded limit, eventually
    }

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
        # resources_with_errors.extend(response)
        resources_with_errors = response

        length_of_response = len(response)

        if length_of_response < PAGE_SIZE:
            all_resources_fetched = True

        else:
            last_uuid = response[-1].get("id")
            rest_api_params["after"] = last_uuid
        LOGGER.info(f"{len(resources_with_errors)} errors in page")

        # Convert API response data to ErrorServiceResource objects.
        resources = []
        for resource_data in resources_with_errors:
            organization = Organization(resource_data.get("organization_id"))

            resource_type = None
            for resource_type_option in ResourceType:
                if resource_type_option.value == resource_data.get("resource_type"):
                    resource_type = resource_type_option
                    break

            if resource_type not in (
                ResourceType.CONDITION,
                ResourceType.OBSERVATION,
                ResourceType.DOCUMENT_REFERENCE
            ):
                warnings.warn(
                    f"Support for the {resource_type} resource type has not been implemented"
                )
                continue

            resource = ErrorServiceResource(
                id=UUID(resource_data.get("id")),
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
            resource.load_issues(client=client)
            resources.append(resource)

        # Step 2: For specific resource types, we may need to read the issue to
        # determine where in the resource the failure occurred.
        # However, this is initially implemented for Condition and Observation only.

        # Step 3: Extract relevant codes from the resource.

        # Initialize a dictionary to hold codes that need deduplication, grouped by terminology.
        # Key: terminology_version_uuid, Value: list containing the codes to load to that terminology
        new_codes_to_deduplicate_by_terminology = {}

        for error_service_resource in resources:

            # For a given resource type, identify the actual coding which needs to make it into the concept map
            raw_resource = json.loads(error_service_resource.resource)

            if error_service_resource.resource_type in [
                ResourceType.CONDITION,
                ResourceType.OBSERVATION,
            ]:
                raw_coding = raw_resource["code"]
            elif error_service_resource.resource_type in [
                ResourceType.DOCUMENT_REFERENCE
            ]:
                raw_coding = raw_resource["type"]
            else:
                raise NotImplementedError(f"Support for extracting codeable concept not implemented for {error_service_resource.resource_type}")

            raw_coding_as_value_codeable_concept = {
                "valueCodeableConcept": raw_coding
            }

            # Lookup the concept map version used to normalize this type of resource
            # So that we can then identify the correct terminology to load the new coding to

            # The data element where validation is failed is stored in the 'location' on the issue
            # We need to filter the issues to just the NOV_CONMAP_LOOKUP issues and get the location
            nov_conmap_issues = error_service_resource.filter_issues_by_type('NOV_CONMAP_LOOKUP')
            locations = [issue.location for issue in nov_conmap_issues]
            locations = list(set(locations))
            if len(locations) > 1:
                warnings.warn("Resource has more than one issue of type NOV_CONMAP_LOOKUP; cannot extract just one location")
                continue # Skip the rest of this one and move on
            if not locations:
                warnings.warn("Resource has no locations")
                continue
            data_element = locations[0]

            concept_map_version_for_normalization = (
                lookup_concept_map_version_for_data_element(
                    data_element=data_element,
                    organization=error_service_resource.organization,
                )
            )

            # Inside the concept map version, we'll extract the source value set
            source_value_set_uuid = (
                concept_map_version_for_normalization.concept_map.source_value_set_uuid
            )
            most_recent_active_source_value_set_version = (
                ValueSet.load_most_recent_active_version_with_cache(source_value_set_uuid)
            )
            most_recent_active_source_value_set_version.expand(no_repeat=True)

            # Identify the terminology inside the source value set
            terminologies_in_source_value_set = (
                most_recent_active_source_value_set_version.lookup_terminologies_in_value_set_version()
            )

            if len(terminologies_in_source_value_set) > 1:
                raise Exception(
                    "There should only be a single source terminology for a concept map used in data normalization"
                )

            current_terminology_version = terminologies_in_source_value_set[0]

            # The custom terminology may have already passed its effective end date, so we might need to create a new version
            terminology_to_load_to = (
                current_terminology_version.version_to_load_new_content_to()
            )

            new_code = Code(
                code=raw_coding_as_value_codeable_concept,
                display=raw_coding.get("text"),
                system=None,
                version=None,
                terminology_version_uuid=terminology_to_load_to.uuid,
            )

            # Assemble additionalData from the raw resource.
            # This is where we'll extract unit, value, valueQuantity, and referenceRange if available
            resource_json = json.loads(error_service_resource.resource)
            unit = resource_json.get('unit')
            value = resource_json.get('value')
            value_quantity = resource_json.get('valueQuantity')
            reference_range = resource_json.get('referenceRange')

            if unit or value or value_quantity or reference_range:
                new_code.add_examples_to_additional_data(
                    unit=unit,
                    value=value,
                    value_quantity=value_quantity,
                    reference_range=reference_range
                )

            if terminology_to_load_to.uuid in new_codes_to_deduplicate_by_terminology:
                new_codes_to_deduplicate_by_terminology[terminology_to_load_to.uuid].append(
                    new_code
                )
            else:
                new_codes_to_deduplicate_by_terminology[terminology_to_load_to.uuid] = [new_code]

        # Step 4: Deduplicate the codes to avoid redundant data.
        deduped_codes_by_terminology = {}

        # Loop through codes, identify duplicates, and merge them.
        for terminology_uuid, new_codes_to_deduplicate in new_codes_to_deduplicate_by_terminology.items():

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
                        if merged_code.additional_data:
                            merged_code.add_examples_to_additional_data(
                                unit=current_duplicate.additional_data.get('example_unit'),
                                value=current_duplicate.additional_data.get('example_value'),
                                value_quantity=current_duplicate.additional_data.get('example_value_quantity'),
                                reference_range=current_duplicate.additional_data.get('example_reference_range')
                            )
                    deduped_codes.append(merged_code)
                else:
                    deduped_codes.append(duplicates[0])

            deduped_codes_by_terminology[terminology_uuid] = deduped_codes

        # Step 5: Load the deduplicated codes into their respective terminologies.
        for terminology_version_uuid, code_list in deduped_codes_by_terminology.items():
            terminology = Terminology.load(terminology_version_uuid)

            LOGGER.info(f"Loading {len(code_list)} new codes to terminology {terminology.terminology} version {terminology.version}")
            terminology.load_new_codes_to_terminology(code_list, on_conflict_do_nothing=True) #todo: reconsider on conflict do nothing

    LOGGER.info("Loading data from error service to custom terminologies complete")


@lru_cache
def lookup_concept_map_version_for_data_element(
    data_element: str, organization: Organization
) -> "ConceptMapVersion":
    """
    Returns the specific ConceptMapVersion currently in use for normalizing data with the specified resource_type and organization
    :param data_element:
    :param organization:
    :return:
    """
    # Load the data normalization registry
    registry = DataNormalizationRegistry()
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
        ]
        if len(tenant_agnostic) > 0:
            concept_map_version = tenant_agnostic[
                0
            ].concept_map.most_recent_active_version

    # If nothing is found, raise an appropriate error
    if concept_map_version is None:
        raise Exception("No appropriate registry entry found")

    return concept_map_version


def get_outstanding_errors(
    registry: DataNormalizationRegistry = None,
) -> List[Dict]:
    if registry is None:
        registry = DataNormalizationRegistry()
        registry.load_entries()

    outstanding_errors = []
    for registry_item in registry.entries:
        if registry_item.registry_entry_type == "concept_map":
            # Load the most recent concept map version (not just the most recent active)
            concept_map = registry_item.concept_map
            concept_map_version = concept_map.get_most_recent_version(active_only=False)

            # Identify the source terminology
            if concept_map_version.source_value_set_version_uuid is not None:
                value_set_version = ValueSetVersion.load(
                    concept_map_version.source_value_set_version_uuid
                )

                terminologies_in_value_set_version = (
                    value_set_version.lookup_terminologies_in_value_set_version()
                )
                if terminologies_in_value_set_version:
                    source_terminology = terminologies_in_value_set_version[0]
                    source_terminology = source_terminology.load_latest_version()

                    # Grab the first item from the list

                    # Write a SQL query to identify all codes in custom_terminology.code
                    # with a created_date for the code that is more recent than
                    # the created_date of the underlying source terminology
                    recent_codes = source_terminology.get_recent_codes(
                        concept_map_version.created_date
                    )

                    if len(recent_codes) != 0:
                        outstanding_errors.append(
                            {
                                "concept_map_uuid": registry_item.concept_map.uuid,
                                "concept_map_title": registry_item.concept_map.title,
                                "number_of_outstanding_codes": len(recent_codes),
                            }
                        )
    return outstanding_errors


if __name__ == "__main__":
    from app.database import get_db
    conn = get_db()

    load_concepts_from_errors()

    conn.commit()
    conn.close()


