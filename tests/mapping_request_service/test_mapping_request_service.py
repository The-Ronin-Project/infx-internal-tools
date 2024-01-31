import datetime
import uuid
import unittest
import json

import app.models.mapping_request_service


class MappingRequestServiceTests(unittest.TestCase):
    def test_extract_issue_location(self):
        mapping_request_service = (
            app.models.mapping_request_service.MappingRequestService()
        )

        sample_issue_2 = app.models.mapping_request_service.ErrorServiceIssue(
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
            app.models.mapping_request_service.MappingRequestService()
        )
        resource_type = app.models.mapping_request_service.ResourceType.OBSERVATION

        raw_resource = {
            "resourceType": "Observation",
            "id": "elm4BVYubI16UGX4Tr1KZmTg9woQK1eL.Ovfl8.TQvO28jgzEGXU2H0OrLRjq.HDQareRu3tZA7.heeQpgBe4VQ3",
            "status": "unknown",
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://open.epic.com/FHIR/StructureDefinition/observation-category",
                            "code": "smartdata",
                            "display": "SmartData",
                        }
                    ],
                    "text": "SmartData",
                }
            ],
            "code": {
                "coding": [
                    {"system": "http://snomed.info/sct", "code": "SNOMED#260767000"},
                    {
                        "system": "urn:oid:1.2.840.114350.1.13.412.2.7.2.727688",
                        "code": "EPIC#42384",
                        "display": "regional lymph nodes (N)",
                    },
                ],
                "text": "FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - TNM CLASSIFICATION - AJCC N - REGIONAL LYMPH NODES (N)",
            },
            "subject": {
                "reference": "Patient/kjs5bliyT5sKJBY3F7UY",
                "display": "Person, Name",
            },
            "focus": [{"reference": "Condition/jhvd87JKBDIV"}],
            "issued": "2023-10-15T23:36:51Z",
            "performer": [
                {"reference": "Practitioner/bid688HFV86", "display": "PERSON, NAME"}
            ],
            "component": [
                {
                    "code": {"text": "Line 1"},
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.110",
                                "code": "20000",
                                "display": "pN0",
                            }
                        ],
                        "text": "pN0",
                    },
                }
            ],
        }
        location = "Observation.component[0].code"
        element = "Observation.component.code"
        index = 0

        (
            found,
            processed_code,
            processed_display,
            depends_on_value,
            depends_on_property,
        ) = mapping_request_service.extract_coding_attributes(
            resource_type, raw_resource, location, element, index
        )

        self.assertTrue(found)
        self.assertEqual(processed_code, raw_resource["component"][index]["code"])
        self.assertEqual(
            processed_display, raw_resource["component"][index]["code"]["text"]
        )
        self.assertEqual(depends_on_value, json.dumps(raw_resource["code"]))
        self.assertEqual(depends_on_property, "Observation.code")
