import datetime
import pytest
from unittest.mock import patch, Mock
import json

from app.models.models import Organization
import app.concept_maps.models
import app.util.mapping_request_service
import app.models.data_ingestion_registry
import app.tasks
from app.database import get_db


def generate_mock_error_resources():
    # Load the error template
    with open("sample_normalization_error.json") as sample_normalization_error_file:
        sample_normalization_error = sample_normalization_error_file.read()
        sample_normalization_error_json = json.loads(sample_normalization_error)

    # Generate mock condition
    with open("sample_condition_encounter_diagnosis.json") as sample_condition_file:
        sample_condition = sample_condition_file.read()
        sample_condition_json = json.loads(sample_condition)

    # Replace the coding array w/ our new failing codes
    new_coding_array = {
        "coding": [
            {
                "system": "http://projectronin.io/fhir/CodeSystem/mock/condition",
                "version": "1.0",
                "code": "test_concept_1",
                "display": f"Test Concept {datetime.datetime.now()}",
            }
        ],
        "text": f"Test Concept {datetime.datetime.now()}",
    }
    sample_condition_json["code"] = new_coding_array
    serialized_sample_condition = json.dumps(sample_condition_json)

    # Put the mock condition inside the error template
    sample_normalization_error_json[0]["resource"] = serialized_sample_condition
    return sample_normalization_error_json


def generate_mock_error_resources_observation():
    # Load the error template
    with open(
            "sample_normalization_observation_error.json"
    ) as sample_normalization_error_file:
        sample_normalization_error = sample_normalization_error_file.read()
        sample_normalization_error_json = json.loads(sample_normalization_error)

    # Generate mock observation
    with open("sample_observation.json") as sample_observation_file:
        sample_observation = sample_observation_file.read()
        sample_observation_json = json.loads(sample_observation)

    # Replace the coding array w/ our new failing codes
    new_coding_array = {
        "coding": [
            {
                "system": "http://projectronin.io/fhir/CodeSystem/testing/Observation",
                "version": "1",
                "code": "test_concept_1",
                "display": f"Test Observation Concept {datetime.datetime.now()}",
            }
        ],
        "text": f"Test Concept {datetime.datetime.now()}",
    }
    sample_observation_json["code"] = new_coding_array
    serialized_sample_observation = json.dumps(sample_observation_json)

    # Put the mock condition inside the error template
    sample_normalization_error_json[0]["resource"] = serialized_sample_observation
    return sample_normalization_error_json


def generate_mock_error_issues():
    with open("sample_normalization_issue.json") as sample_normalization_issue_file:
        sample_normalization_issue = sample_normalization_issue_file.read()
        sample_normalization_issue_json = json.loads(sample_normalization_issue)
    return sample_normalization_issue_json


def generate_mock_registry():
    with open("sample_registry.json") as sample_registry_file:
        sample_registry_file = sample_registry_file.read()
        sample_registry_json = json.loads(sample_registry_file)
    return sample_registry_json


@patch("app.models.normalization_error_service.make_get_request")
def test_incremental_load_condition_integration(mock_request):
    conn = get_db()
    organization = Organization(id="ronin")

    # Loop through both Condition and Observation resource types
    for resource_type in (
            app.util.mapping_request_service.ResourceType.CONDITION,
    ):
        #
        # PART 1: Loading from Errors to Custom Terminology
        #

        # Generate our mock response for Get Resources API call from Error Service
        mock_resources = generate_mock_error_resources()
        mock_issues = generate_mock_error_issues()

        # Setup our mock responses

    def side_effect(token, base_url, api_url, params):
        if api_url == "/resources":
            return mock_resources
        else:
            return mock_issues

    mock_request.side_effect = side_effect

    # Mock the registry entry
    test_incremental_load_concept_map_version = (
        app.concept_maps.models.ConceptMapVersion(
            "7bff7e50-7d95-46f6-8268-d18a5257327b"
        )
    )

    with patch(
        "app.models.normalization_error_service.lookup_concept_map_version_for_data_element"
    ) as mock_lookup_concept_map:
        mock_lookup_concept_map.return_value = test_incremental_load_concept_map_version

        codes_by_org_and_resource_type = (
            app.util.mapping_request_service.load_concepts_from_errors()
        )
        conn.commit()

    #
    # PART 2: Verify Report Captures New Outstanding Code
    #
    incremental_load_concept_map = app.concept_maps.models.ConceptMap(
        "ae61ee9b-3f55-4d3c-96e7-8c7194b53767"
    )

    mock_registry_for_report = (
        app.util.mapping_request_service.DataNormalizationRegistry()
    )
    mock_registry_for_report.entries = [
        app.models.data_ingestion_registry.DNRegistryEntry(
            resource_type="Condition",
            data_element="Condition.code",
            tenant_id=organization.id,
            source_extension_url="",
            registry_uuid="",
            registry_entry_type="concept_map",
            profile_url="",
            concept_map=incremental_load_concept_map,
        )
    ]
    outstanding_errors = app.util.mapping_request_service.get_outstanding_errors(
        registry=mock_registry_for_report
    )
    number_of_outstanding_codes = outstanding_errors[0].get(
        "number_of_outstanding_codes"
    )
    assert (
        number_of_outstanding_codes > 0
    )  # Verify the new code we added above is in the report

    #
    # PART 3: Load Outstanding Code to New Concept Map Version
    #
    app.tasks.load_outstanding_codes_to_new_concept_map_version(
        incremental_load_concept_map.uuid
    )


@patch("app.models.normalization_error_service.make_get_request")
def test_incremental_load_observation_integration(mock_request):
    conn = get_db()
    organization = Organization(id="ronin")

    resource_type = app.util.mapping_request_service.ResourceType.OBSERVATION

    #
    # PART 1: Loading from Errors to Custom Terminology
    #

    # Generate our mock response for Get Resources API call from Error Service
    mock_resources = generate_mock_error_resources_observation()
    mock_issues = generate_mock_error_issues()

    # Setup our mock responses

    def side_effect(token, base_url, api_url, params):
        if api_url == "/resources":
            return mock_resources
        else:
            return mock_issues

    mock_request.side_effect = side_effect

    # Mock the registry entry
    test_incremental_load_concept_map_version = (
        app.concept_maps.models.ConceptMapVersion(
            "bd2921cf-d19d-48be-b610-68ac2515c5bd"
        )
    )

    with patch(
        "app.models.normalization_error_service.lookup_concept_map_version_for_resource_type"
    ) as mock_lookup_concept_map:
        mock_lookup_concept_map.return_value = test_incremental_load_concept_map_version

        codes_by_org_and_resource_type = (
            app.util.mapping_request_service.load_concepts_from_errors()
        )
        conn.commit()

    #
    # PART 2: Verify Report Captures New Outstanding Code
    #

    # this test has been deprecated since 2023Q4 and this case (within the test) used a deprecated terminology
    # incremental_load_concept_map = app.concept_maps.models.ConceptMap(
    #     "(find a new test case)"
    # )
    #
    # mock_registry_for_report = (
    #     app.util.mapping_request_service.DataNormalizationRegistry()
    # )
    # mock_registry_for_report.entries = [
    #     app.models.data_ingestion_registry.DNRegistryEntry(
    #         resource_type="Observation",
    #         data_element="Observation.code",
    #         tenant_id=organization.id,
    #         source_extension_url="",
    #         registry_uuid="",
    #         registry_entry_type="concept_map",
    #         profile_url="",
    #         concept_map=incremental_load_concept_map,
    #     )
    # ]
    # outstanding_errors = app.util.mapping_request_service.get_outstanding_errors(
    #     registry=mock_registry_for_report
    # )
    # number_of_outstanding_codes = outstanding_errors[0].get(
    #     "number_of_outstanding_codes"
    # )
    # assert (
    #     number_of_outstanding_codes > 0
    # )  # Verify the new code we added above is in the report

    # For now, just testing the first half where we generate an error and load it to the terminology

    # #
    # # PART 3: Load Outstanding Code to New Concept Map Version
    # #
    # app.tasks.load_outstanding_codes_to_new_concept_map_version(
    #     incremental_load_concept_map.uuid
    # )


def test_outstanding_condition_error_concepts_report():
    concept_map = app.concept_maps.models.ConceptMap(
        "ae61ee9b-3f55-4d3c-96e7-8c7194b53767"
    )
    mock_registry = app.models.data_ingestion_registry.DataNormalizationRegistry
    mock_registry.entries = [
        app.models.data_ingestion_registry.DNRegistryEntry(
            resource_type="Condition",
            data_element="Condition.code",
            tenant_id="apposnd",
            source_extension_url="http://projectronin.io/fhir/StructureDefinition/Extension/tenant-sourceConditionCode",
            registry_uuid="f9f82c69-3c26-4990-973b-87cf7ccbb120",
            registry_entry_type="concept_map",
            profile_url="",
            concept_map=concept_map,
        )
    ]

    report = app.util.mapping_request_service.get_outstanding_errors(
        registry=mock_registry
    )
    print(report)


# this test has been deprecated since 2023Q4 and this case (within the test) used a deprecated terminology
# def test_outstanding_observation_error_concepts_report():
#     concept_map = app.concept_maps.models.ConceptMap(
#         "(find a new test case)"
#     )
#     mock_registry = app.models.data_ingestion_registry.DataNormalizationRegistry
#     mock_registry.entries = [
#         app.models.data_ingestion_registry.DNRegistryEntry(
#             resource_type="Observation",
#             data_element="Observation.code",
#             tenant_id="apposnd",
#             source_extension_url="http://projectronin.io/fhir/StructureDefinition/Extension/tenant-sourceObservationCode",
#             registry_uuid="f9f82c69-3c26-4990-973b-87cf7ccbb120",
#             registry_entry_type="concept_map",
#             profile_url="",
#             concept_map=concept_map,
#         )
#     ]
#
#     report = app.util.mapping_request_service.get_outstanding_errors(
#         registry=mock_registry
#     )
#     print(report)
