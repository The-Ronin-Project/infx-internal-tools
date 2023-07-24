import datetime
import json
from uuid import UUID
from typing import Dict, Tuple, List, Optional
from enum import Enum
from dataclasses import dataclass

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
        return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")


# Function to use the token to access the API
def make_get_request(token, base_url, api_url, params={}, page=1):
    headers = {"Authorization": f"Bearer {token}", "content-type": "application/json"}
    params["page"] = page
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
    update_dt_tm: datetime.datetime
    reprocess_dt_tm: datetime.datetime
    reprocessed_by: str
    token: str

    def __post_init__(self):
        self.issues = []

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
            issue = ErrorServiceIssue(
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
            self.issues.append(issue)

    def filter_issues_by_type(self, issue_type="NOV_CONMAP_LOOKUP"):
        filtered_issues = []
        for issue in self.issues:
            if issue.type == issue_type:
                filtered_issues.append(issue)


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
    update_dt_tm: datetime.datetime
    metadata: str


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

    This function parses each error and identifies the relevant concepts. These concepts are
    then grouped by the originating organization and the type of resource they belong to. The
    results are returned as a dictionary, where each key is a tuple of an organization and a
    resource type, and each value is a list of concepts associated with that key.

    Returns:
        Dict[Tuple[Organization, ResourceType], List[Code]]: A dictionary mapping tuples of
        organization and resource type to lists of concepts extracted from the errors.
    """
    token = get_token()

    # Initialize variables for pagination
    page = 1
    has_more = True

    resources = []
    while has_more:
        all_resources_with_errors_response = make_get_request(
            token=token,
            base_url=f"{DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL}",
            api_url="/resources",
            params={
                "order": "ASC",
                "limit": 25,
                "issue_type": "NOV_CONMAP_LOOKUP",
            },
            page=page,
        )

        # If the response is empty, set has_more to False, otherwise increase the page number
        if not all_resources_with_errors_response:
            has_more = False
        else:
            page += 1

        for resource_data in all_resources_with_errors_response:
            resource = ErrorServiceResource.deserialize(resource_data, token=token)
            resource.load_issues()
            resources.append(resource)

    new_codes_to_load_by_terminology = {}

    for error_service_resource in resources:
        if error_service_resource.resource_type == ResourceType.CONDITION:
            raw_resource = json.loads(error_service_resource.resource)
            raw_coding = raw_resource["code"]
        else:
            raise NotImplementedError(
                "Only support for Conditions has been implemented"
            )

        concept_map_version_for_normalization = (
            lookup_concept_map_version_for_resource_type(
                resource_type=error_service_resource.resource_type,
                organization=error_service_resource.organization,
            )
        )

        source_value_set_uuid = (
            concept_map_version_for_normalization.concept_map.source_value_set_uuid
        )
        most_recent_active_source_value_set_version = (
            ValueSet.load_most_recent_active_version(source_value_set_uuid)
        )
        most_recent_active_source_value_set_version.expand()

        terminologies_in_source_value_set = (
            most_recent_active_source_value_set_version.lookup_terminologies_in_value_set_version()
        )

        if len(terminologies_in_source_value_set) > 1:
            raise Exception(
                "There should only be a single source terminology for a concept map used in data normalization"
            )

        current_terminology_version = terminologies_in_source_value_set[0]

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

    # Unpack the data structure we created earlier and load the codes to their respective terminologies
    for terminology_version_uuid, code_list in new_codes_to_load_by_terminology.items():
        terminology = Terminology.load(terminology_version_uuid)
        terminology.load_new_codes_to_terminology(code_list)


def lookup_concept_map_version_for_resource_type(
    resource_type: ResourceType, organization: Organization
) -> "ConceptMapVersion":
    """
    Returns the specific ConceptMapVersion currently in use for normalizing data with the specified resource_type and organization
    :param resource_type:
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
        if registry_entry.data_element == resource_type.value
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

    # Falling back to check for tenant agnostic entry
    tenant_agnostic = [
        registry_entry
        for registry_entry in filtered_registry
        if registry_entry.tenant_id is None
    ]
    if len(tenant_agnostic) > 0:
        concept_map_version = tenant_agnostic[0].concept_map.most_recent_active_version

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
