import datetime
from unittest.mock import patch, Mock
import json

from app.models.models import Organization
from app.concept_maps.models import ConceptMapVersion
import app.models.normalization_error_service


# def generate_sample_concepts():
#     """
#     Each time the test is run, sample data needs to be created.
#     We will use 'Test Concept [timestamp]' as the display, so it's unique every time.
#     Generate two sample concepts, put them in a list
#     :return:
#     """
#     concept1 = Concept(
#         display=f"Test Concept {datetime.datetime.now()}",
#         code="test_concept_1",
#         system="http://projectronin.io/fhir/CodeSystem/mock/condition",
#         version="1.0",
#     )
#     concept2 = Concept(
#         display=f"Test Concept {datetime.datetime.now() + datetime.timedelta(seconds=1)}",
#         code="test_concept_2",
#         system="http://projectronin.io/fhir/CodeSystem/mock/condition",
#         version="1.0",
#     )
#     return [concept1, concept2]


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


def generate_mock_error_issues():
    with open("sample_normalization_issue.json") as sample_normalization_issue_file:
        sample_normalization_issue = sample_normalization_issue_file.read()
        sample_normalization_issue_json = json.loads(sample_normalization_issue)
    return sample_normalization_issue_json


@patch("app.models.normalization_error_service.make_get_request")
def incremental_load_integration_test(mock_request):
    organization = Organization(id="ronin")

    # Set up the mock error response
    # Specifying the resource type as CONDITION
    resource_type = app.models.normalization_error_service.ResourceType.CONDITION

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
    test_incremental_load_concept_map_version = ConceptMapVersion(
        "7bff7e50-7d95-46f6-8268-d18a5257327b"
    )

    with patch(
        "app.models.normalization_error_service.lookup_concept_map_version_for_resource_type"
    ) as mock_lookup_concept_map:
        mock_lookup_concept_map.return_value = test_incremental_load_concept_map_version

        codes_by_org_and_resource_type = (
            app.models.normalization_error_service.load_concepts_from_errors()
        )

    # # Creating identifiers for concept map and version to use in mock function
    # concept_map_uuid = "ae61ee9b-3f55-4d3c-96e7-8c7194b53767"
    # version = 1
    # include_internal_info = True
    #
    # # Creating a mock response for ConceptMapVersion.load method
    # mock_registry_lookup = ConceptMapVersion.load(concept_map_uuid, version)
    #
    # # Mock the load_concepts_from_errors function to return the mock_data
    # # Using Python's mock patching mechanism to replace the original 'load_concepts_from_errors' function
    # # The function will return the mock data instead of executing the original code
    # with patch(
    #     "infx_condition_incremental_load.main.load_concepts_from_errors",
    #     return_value=mock_error_concept_data,
    # ):
    #
    #     # Similar to the above, we are mocking the 'lookup_concept_map_version_for_resource_type' function
    #     # It will return the 'mock_registry_lookup' instead of executing the original code
    #     with patch(
    #         "infx_condition_incremental_load.main.lookup_concept_map_version_for_resource_type",
    #         return_value=mock_registry_lookup,
    #     ):
    #         # Calling the process_errors function, which should use our mock data and functions
    #         process_errors()
