import datetime
import uuid
import unittest
import json
from app.helpers.file_helper import resources_folder
import app.util.mapping_request_service


class MappingRequestServiceTests(unittest.TestCase):
    def test_extract_issue_location(self):
        mapping_request_service = (
            app.util.mapping_request_service.MappingRequestService()
        )

        sample_issue_2 = app.util.mapping_request_service.ErrorServiceIssue(
            create_dt_tm=datetime.datetime.now(),
            description="Tenant source value '260385009' has no target defined in any Observation.valueCodeableConcept concept map for tenant 'mdaoc'",
            id=uuid.UUID("ea088929-a8ba-472c-af6b-e6df5cd0c248"),
            location="Observation.component[0].valueCodeableConcept",
            severity="FAILED",
            status="REPORTED",
            type="NOV_CONMAP_LOOKUP",
            update_dt_tm=None,
            metadata=None,
        )

        location, element, index = mapping_request_service.extract_issue_location(
            sample_issue_2
        )

        self.assertEqual(location, "Observation.component[0].valueCodeableConcept")
        self.assertEqual(element, "Observation.component.valueCodeableConcept")
        self.assertEqual(index, 0)

    def test_extract_coding_attributes_observation_smart_data(self):
        mapping_request_service = (
            app.util.mapping_request_service.MappingRequestService()
        )
        resource_type = app.util.mapping_request_service.ResourceType.OBSERVATION

        with open(resources_folder(__file__, "ObservationWithSmartData.json")) as raw_resource_file:
            raw_resource = json.load(raw_resource_file)
            location = "Observation.component[0].code"
            element = "Observation.component.code"
            index = 0

            (
                found,
                processed_code,
                processed_display,
                depends_on,
                additional_data,
            ) = mapping_request_service.extract_coding_attributes(
                resource_type, raw_resource, location, element, index
            )

            self.assertTrue(found)
            self.assertEqual(processed_code, '{"text":"Line 1"}', )
            self.assertEqual(
                processed_display, "Line 1"
            )
            self.assertEqual(
                depends_on.depends_on_value, '{"coding":[{"code":"SNOMED#260767000","system":"http://snomed.info/sct"},{"code":"EPIC#42384","display":"regional lymph nodes (N)","system":"urn:oid:1.2.840.114350.1.13.412.2.7.2.727688"}],"text":"FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - TNM CLASSIFICATION - AJCC N - REGIONAL LYMPH NODES (N)"}'
            )
            self.assertEqual({"text": "Line 1"}, raw_resource["component"][index]["code"])
            self.assertEqual("Line 1", raw_resource["component"][index]["code"]["text"])
            self.assertEqual(depends_on.depends_on_property, "Observation.code")

            self.assertIn("category", additional_data)
            self.assertEqual(
                additional_data.get("category"),
                "SmartData",
            )

    def test_extract_coding_attributes_observation_empty_component(self):
        mapping_request_service = (
            app.util.mapping_request_service.MappingRequestService()
        )
        resource_type = app.util.mapping_request_service.ResourceType.OBSERVATION

        raw_resource = {
            "resourceType": "Observation",
            "category": [{"text": "SmartData"}],
            "code": {"text": "Example Code"},
            "component": [],
        }
        location = "Observation.component[0].code"
        element = "Observation.component.code"
        index = 0

        (
            found,
            processed_code,
            processed_display,
            depends_on,
            additional_data,
        ) = mapping_request_service.extract_coding_attributes(
            resource_type, raw_resource, location, element, index
        )

        self.assertFalse(found)

    def test_extract_coding_attributes_observation_missing_component(self):
        mapping_request_service = (
            app.util.mapping_request_service.MappingRequestService()
        )
        resource_type = app.util.mapping_request_service.ResourceType.OBSERVATION

        raw_resource = {
            "resourceType": "Observation",
            "category": [{"text": "SmartData"}],
            "code": {"text": "Example Code"},
        }
        location = "Observation.component[0].code"
        element = "Observation.component.code"
        index = 0

        (
            found,
            processed_code,
            processed_display,
            depends_on,
            additional_data,
        ) = mapping_request_service.extract_coding_attributes(
            resource_type, raw_resource, location, element, index
        )

        self.assertFalse(found)
