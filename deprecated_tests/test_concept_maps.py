from unittest.mock import MagicMock

import pytest

from mock import patch
from app.helpers.oci_helper import folder_path_for_oci

concept_map = {
    "resourceType": "ConceptMap",
    "title": "TEST Appointment Statuses to FHIR R4 AppointmentStatus",
    "id": "FAKEFAKE-FAKE-FAKE-FAKE-FAKEFAKEFAKE",
    "name": "AppointmentStatusTEST",
    "contact": [{"name": "TEST USER"}],
    "url": "http://projectronin.com/fhir/us/ronin/ConceptMap/FAKEFAKE-FAKE-FAKE-FAKE-FAKEFAKEFAKE",
    "description": "{{pagelink:Ronin-Implementation-Guide-Home/List/Concept-Maps/Appointment-Status-test.page.md}}",
    "purpose": "A semantic map between source Appointment.status and target per the title to provide a base map for tenant-sourceAppointmentStatus logic",
    "publisher": "Project Ronin",
    "experimental": False,
    "date": "2022-09-22",
    "version": 1,
    "group": [
        {
            "source": "http://projectronin.io/fhir/terminologies/ronin_test_appointment_status",
            "sourceVersion": "1.0",
            "target": "http://hl7.org/fhir/appointmentstatus",
            "targetVersion": "4.0.1",
            "element": [
                {
                    "code": "Arrived",
                    "display": "Arrived",
                    "target": [
                        {
                            "code": "arrived",
                            "display": "Arrived",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Canceled",
                    "display": "Canceled",
                    "target": [
                        {
                            "code": "cancelled",
                            "display": "Cancelled",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Scheduled",
                    "display": "Scheduled",
                    "target": [
                        {
                            "code": "booked",
                            "display": "Booked",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Present",
                    "display": "Present",
                    "target": [
                        {
                            "code": "arrived",
                            "display": "Arrived",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Completed",
                    "display": "Completed",
                    "target": [
                        {
                            "code": "fulfilled",
                            "display": "Fulfilled",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "No Show",
                    "display": "No Show",
                    "target": [
                        {
                            "code": "noshow",
                            "display": "No Show",
                            "equivalence": "equivalent",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "HSP Incomplete",
                    "display": "HSP Incomplete",
                    "target": [
                        {
                            "code": "cancelled",
                            "display": "Cancelled",
                            "equivalence": "source-is-narrower-than-target",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "HH Incomplete",
                    "display": "HH Incomplete",
                    "target": [
                        {
                            "code": "cancelled",
                            "display": "Cancelled",
                            "equivalence": "source-is-narrower-than-target",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Left without seen",
                    "display": "Left without seen",
                    "target": [
                        {
                            "code": "noshow",
                            "display": "No Show",
                            "equivalence": "source-is-narrower-than-target",
                            "comment": "null",
                        }
                    ],
                },
                {
                    "code": "Phoned Patient",
                    "display": "Phoned Patient",
                    "target": [
                        {
                            "code": "proposed",
                            "display": "Proposed",
                            "equivalence": "source-is-narrower-than-target",
                            "comment": "null",
                        }
                    ],
                },
            ],
        }
    ],
    "extension": [
        {
            "url": "http://projectronin.io/fhir/ronin.common-fhir-model.uscore-r4/StructureDefinition/Extension/ronin-ConceptMapSchema",
            "valueString": "1.0.0",
        }
    ],
}
concept_map_uuid = concept_map["url"].rsplit("/", 1)[1]
folder = "published"
path = f"DoNotUseTestingConceptMaps/v1/{folder}/{concept_map_uuid}"


def test_folder_path_for_oci():
    assert (
        folder_path_for_oci(concept_map, path, content_type="json")
        == "DoNotUseTestingConceptMaps/v1/published/FAKEFAKE-FAKE-FAKE-FAKE-FAKEFAKEFAKE/1.json"
    )


# not implementing yet looking for a way to mock OCI connection
# @patch("app.helpers.oci_auth.oci_authentication")
# def test_oci_authentication(mock_oci_auth):
#     """
#     this test IS NOT LEGIT NEEDS WORK
#     @param mock_oci_auth: mock oci auth
#     @return: False / 0
#     """
#     assert mock_oci_auth.return_value.list_buckets.call_count == 0
