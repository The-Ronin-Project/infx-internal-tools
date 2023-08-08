import datetime
import json
from uuid import UUID
from typing import Dict, Tuple, List, Optional
from enum import Enum
from dataclasses import dataclass
from functools import lru_cache
import warnings

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
DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_ID = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_ID", default=""
)
DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_SECRET = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_SECRET", default=""
)
DATA_NORMALIZATION_ERROR_SERVICE_AUDIENCE = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_AUDIENCE", default=""
)
DATA_NORMALIZATION_ERROR_SERVICE_AUTH_URL = config(
    "DATA_NORMALIZATION_ERROR_SERVICE_AUTH_URL", default=""
)


def convert_string_to_datetime_or_none(input_string):
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")


# Function to use the token to access the API
def make_get_request(token, base_url, api_url, params={}):
    headers = {"Authorization": f"Bearer {token}", "content-type": "application/json"}
    response = requests.get(base_url + api_url, headers=headers, params=params)
    return response.json()


class ResourceType(Enum):
    OBSERVATION = "Observation"
    CONDITION = "Condition"
    MEDICATION = "Medication"
    LOCATION = "Location"
    PATIENT = "Patient"
    APPOINTMENT = "Appointment"
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
            self.token = get_token()

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

    def load_issues(self):
        # Call the endpoint
        get_issues_for_resource = make_get_request(
            token=self.token,
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


def get_token():
    url = DATA_NORMALIZATION_ERROR_SERVICE_AUTH_URL
    payload = {
        "client_id": DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_ID,
        "client_secret": DATA_NORMALIZATION_ERROR_SERVICE_CLIENT_SECRET,
        "audience": DATA_NORMALIZATION_ERROR_SERVICE_AUDIENCE,
        "grant_type": "client_credentials",
    }
    headers = {"content-type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    token = json.loads(response.text)
    return token["access_token"]


def load_concepts_from_errors() -> Dict[Tuple[Organization, ResourceType], List[Code]]:
    """
    Loads and processes a list of errors to extract specific concepts from them.
    Save these new concepts back to the correct custom terminology.

    This function parses each error and identifies the relevant concepts. These concepts are
    then grouped by the originating organization and the type of resource they belong to. The
    results are returned as a dictionary, where each key is a tuple of an organization and a
    resource type, and each value is a list of concepts associated with that key.

    Returns:
        Dict[Tuple[Organization, ResourceType], List[Code]]: A dictionary mapping tuples of
        organization and resource type to lists of concepts extracted from the errors.
    """
    # {{validation_url}}/resources?order=ASC&limit=25&issue_type=NOV_CONMAP_LOOKUP
    # 1. query to get all resources that have failed
    token = get_token()

    # Loop through all available errors and retrieve a complete set
    resources_with_errors = []
    PAGE_SIZE = 250
    rest_api_params = {
        "order": "ASC",
        "limit": PAGE_SIZE,
        "issue_type": "NOV_CONMAP_LOOKUP",
    }
    while True:
        response = make_get_request(
            token=token,
            base_url=DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL,
            api_url="/resources",
            params=rest_api_params,
        )
        resources_with_errors.extend(response)

        length_of_response = len(response)
        # print(length_of_response)

        if length_of_response < PAGE_SIZE:
            break

        else:
            last_uuid = response[-1].get("id")
            rest_api_params["after"] = last_uuid

    resources = []
    for resource_data in resources_with_errors:
        organization = Organization(resource_data.get("organization_id"))

        resource_type = None
        for resource_type_option in ResourceType:
            if resource_type_option.value == resource_data.get("resource_type"):
                resource_type = resource_type_option
                break

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
        resource.load_issues()
        resources.append(resource)

    # For some resource types (ex. Location, Appointment), we need to read the issue to know where in the
    # resource the failure occured. However, as we are initially implementing for Condition, where the failure
    # will be in coding.code, we can skip this step

    # 3. Lookup where in the resource the failure is (skipping for now) todo: do this

    # 4. look inside the raw resource json and pull out the relevant codes that need to be mapped

    # Key: terminology_version_uuid, Value: list containing the codes to load to that terminology
    new_codes_to_load_by_terminology = {}

    for error_service_resource in resources:

        # For a given resource type, identify the actual coding which needs to make it into the concept map
        if error_service_resource.resource_type in (
            ResourceType.CONDITION,
            ResourceType.OBSERVATION,
        ):
            raw_resource = json.loads(error_service_resource.resource)
            raw_coding = raw_resource["code"]
            error_service_resource.load_issues()
        else:
            warnings.warn(
                f"Support for the {error_service_resource.resource_type} resource type has not been implemented"
            )
            continue

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
            ValueSet.load_most_recent_active_version(source_value_set_uuid)
        )
        most_recent_active_source_value_set_version.expand()

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
            code=raw_coding,
            display=raw_coding.get("text"),
            system=None,
            version=None,
            terminology_version_uuid=terminology_to_load_to.uuid,
        )

        if terminology_to_load_to.uuid in new_codes_to_load_by_terminology:
            new_codes_to_load_by_terminology[terminology_to_load_to.uuid].append(
                new_code
            )
        else:
            new_codes_to_load_by_terminology[terminology_to_load_to.uuid] = [new_code]

    print(new_codes_to_load_by_terminology)

    # # Unpack the data structure we created earlier and load the codes to their respective terminologies
    # for terminology_version_uuid, code_list in new_codes_to_load_by_terminology.items():
    #     terminology = Terminology.load(terminology_version_uuid)
    #     terminology.load_new_codes_to_terminology(code_list)


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

                    outstanding_errors.append(
                        {
                            "concept_map_uuid": registry_item.concept_map.uuid,
                            "concept_map_title": registry_item.concept_map.title,
                            "number_of_outstanding_codes": len(recent_codes),
                        }
                    )
    return outstanding_errors


if __name__ == "__main__":
    load_concepts_from_errors()
