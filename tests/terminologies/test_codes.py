import datetime
import json
import unittest
import uuid

from pytest import raises
from sqlalchemy import text
from werkzeug.exceptions import NotFound

import app.terminologies.models
import app.models.codes
from app.app import create_app
from app.database import get_db
from app.errors import BadRequestWithCode
from app.terminologies.views import create_code_payload_to_code_list


class CodeClassTests(unittest.TestCase):
    """
    These tests instantiate the Code class directly, as opposed to using API calls as below.
    """
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

        example_codeable_concept_json = """
                                    {
                                      "coding": [
                                        {
                                          "system": "http://hl7.org/fhir/sid/icd-10-cm",
                                          "code": "D59.9"
                                        },
                                        {
                                          "system": "http://snomed.info/sct",
                                          "code": "4854004"
                                        }
                                      ],
                                      "text": "Anemia, hemolytic, acquired (CMS/HCC)"
                                    }
                                    """
        self.example_codeable_concept = app.models.codes.FHIRCodeableConcept.deserialize(example_codeable_concept_json)

        self.example_terminology = app.terminologies.models.Terminology(
            uuid=uuid.uuid4(),
            terminology="sample_terminology",
            version="1",
            effective_start="2024-01-01",
            effective_end="2024-01-10",
            fhir_uri="http://fake_url",
            fhir_terminology=False,
            is_standard=False,
        )

    def tearDown(self) -> None:
        # this executes after each test function, but does not stop lower-level functions from committing db changes
        self.conn.rollback()
        self.conn.close()

    def test_load_simple_code(self):
        # From the "Test ONLY: fake/fhir_uri" terminology v3
        simple_code = app.models.codes.Code.load_from_custom_terminology("4e6e97aa-dd29-4f19-add2-46a56224515d")

        self.assertEqual("Target Concept 2", simple_code.code)
        self.assertEqual("target concept 2", simple_code.display)
        self.assertEqual("fake/fhir_uri", simple_code.system)
        self.assertEqual("3", simple_code.version)

    def test_load_codeable_concept(self):
        # From the "Test ONLY: Codeable Concepts" terminology v1
        codeable_concept = app.models.codes.Code.load_from_custom_terminology("15552373-c736-4b28-b284-6c0009225796")

        self.assertEqual(app.models.codes.FHIRCodeableConcept, type(codeable_concept.code_object))
        self.assertEqual("Anemia, hemolytic, acquired (CMS/HCC)", codeable_concept.code_object.text)

        ordered_coding = sorted(codeable_concept.code_object.coding, key=lambda coding: coding.code)
        first_item = ordered_coding[0]
        second_item = ordered_coding[1]

        self.assertEqual("4854004", first_item.code)
        self.assertEqual("http://snomed.info/sct", first_item.system)

        self.assertEqual("D59.9", second_item.code)
        self.assertEqual("http://hl7.org/fhir/sid/icd-10-cm", second_item.system)

        self.assertEqual("Anemia, hemolytic, acquired (CMS/HCC)", codeable_concept.display)
        self.assertEqual("http://projectronin.io/fhir/CodeSystem/mock/codeableConcepts", codeable_concept.system)
        self.assertEqual("1", codeable_concept.version)

    def test_valid_initialization_with_simple_code(self):
        code = "Test Code"
        display = "Test Display"
        simple_code = app.models.codes.Code.new_code(
            code=code,
            display=display,
            system=None,
            version=None,
            terminology_version=self.example_terminology
        )
        self.assertEqual(code, simple_code.code)
        self.assertEqual(display, simple_code.display)

    def test_valid_initialization_with_codeable_concept(self):
        codeable_concept = app.models.codes.Code(
            code_schema=app.models.codes.RoninCodeSchemas.codeable_concept,
            system=None,
            version=None,
            code=None,
            display=None,
            code_object=self.example_codeable_concept,
            terminology_version=self.example_terminology,
            from_custom_terminology=True,
            custom_terminology_code_uuid=uuid.uuid4()
        )

        self.assertEqual("Anemia, hemolytic, acquired (CMS/HCC)", codeable_concept.display)
        self.assertEqual(2, len(codeable_concept.code_object.coding))

    def test_invalid_initialization_terminology_version_wrong_type(self):
        with self.assertRaises(ValueError):
            app.models.codes.Code(
                code_schema=app.models.codes.RoninCodeSchemas.codeable_concept,
                system=None,
                version=None,
                code=None,
                display=None,
                code_object=self.example_codeable_concept,
                terminology_version=uuid.uuid4(),
                from_custom_terminology=True,
                custom_terminology_code_uuid=uuid.uuid4()
            )

    def test_invalid_initialization_without_code_and_display_for_simple_code_schema(self):
        with self.assertRaises(TypeError):
            # TypeError raised when required positional params not included
            app.models.codes.Code(
                code_schema=app.models.codes.RoninCodeSchemas.code, code_object=None)

        with self.assertRaises(ValueError):
            app.models.codes.Code(
                code=None,
                display=None,
                system=None,
                version=None,
                code_schema=app.models.codes.RoninCodeSchemas.code,
                code_object=None)

    def test_invalid_initialization_with_code_and_display_for_codeable_concept_schema(self):
        with self.assertRaises(ValueError):
            app.models.codes.Code(
                code_schema=app.models.codes.RoninCodeSchemas.codeable_concept,
                code="123",
                display="Test Display",
                system=None,
                version=None,
                code_object=self.example_codeable_concept
            )

    def test_invalid_initialization_missing_code_object_for_codeable_concept_schema(self):
        with self.assertRaises(ValueError):
            app.models.codes.Code(
                code_schema=app.models.codes.RoninCodeSchemas.codeable_concept,
                system=None,
                version=None,
                code=None,
                display=None,
                code_object=None
            )

    def test_invalid_initialization_from_custom_terminology_without_uuid(self):
        with self.assertRaises(ValueError):

            codeable_concept = app.models.codes.Code(
                code_schema=app.models.codes.RoninCodeSchemas.codeable_concept,
                system=None,
                version=None,
                code=None,
                display=None,
                code_object=self.example_codeable_concept,
                terminology_version=self.example_terminology,
                from_custom_terminology=True
            )


class CodeAPITests(unittest.TestCase):
    """
    These tests test the Code class via mocked API calls.

    There are 5 public.terminology_versions rows safe to use in tests that also pass checks to allow codes to be created
    ```
    terminology                    version     uri                 is_standard is_fhir effective_start  _end
    "Test ONLY: fake/fhir_uri"         "3"    "fake/fhir_uri"            false false   "2023-04-12"
    "Test ONLY: http://test_test.com"  "1"    "http://test_test.com"     false false                    "2023-09-01"
    "Test ONLY: Duplicate Insert Test" "1"    ..."/duplicateInsertTest"  false false   "2023-11-09"     "2030-11-16"
    "Test ONLY: Mock FHIR Terminology" "1.0.0" ..."/ronin/mock_fhir"     false true	   "2023-04-05"
    "Test ONLY: Mock Standard Term"... "1.0.0" ..."/ronin/mock_standard" true  false   "2023-02-26"
    ```
    ```
    terminology                        uuid                                   variable            purpose
    "Test ONLY: fake/fhir_uri"         "d2ae0de5-0168-4f54-924a-1f79cf658939" safe_term_uuid_fake has no expiry date
    "Test ONLY: http://test_test.com"  "3c9ed300-0cb8-47af-8c04-a06352a14b8d" safe_term_uuid_test expiry date has passed
    "Test ONLY: Duplicate Insert Test" "d14cbd3a-aabe-4b26-b754-5ae2fbd20949" safe_term_uuid_dupl has future expiry date
    "Test ONLY: Mock FHIR Terminology" "34eb844c-ffff-4462-ad6d-48af68f1e8a1" safe_term_uuid_fhir fhir_terminology true
    "Test ONLY: Mock Standard Term"... "c96200d7-9e30-4a0c-b98e-22d0ff146a99" safe_term_uuid_std  is_standard true
    ```

    How to check for values being written by these tests - in case cleanup is not done properly (Ctrl-C or Stop):

    select * from public.terminology_versions
    where uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939', '3c9ed300-0cb8-47af-8c04-a06352a14b8d',
    'd14cbd3a-aabe-4b26-b754-5ae2fbd20949')

    select * from custom_terminologies.code
    where terminology_version_uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939', '3c9ed300-0cb8-47af-8c04-a06352a14b8d',
    'd14cbd3a-aabe-4b26-b754-5ae2fbd20949')
    order by terminology_version_uuid

    delete from custom_terminologies.code
    where terminology_version_uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939','3c9ed300-0cb8-47af-8c04-a06352a14b8d','d14cbd3a-aabe-4b26-b754-5ae2fbd20949')
    and code like 'test code at 20%'

    """
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.app.config.update({
            "DISABLE_ROLLBACK_AFTER_REQUEST": False
        })
        self.app.config.update({
            "DISABLE_CLOSE_AFTER_REQUEST": False
        })
        # this executes after each test function, but does not stop lower-level functions from committing db changes
        self.conn.rollback()
        self.conn.close()

    safe_term_uuid_fake = "d2ae0de5-0168-4f54-924a-1f79cf658939"
    safe_term_uuid_old = "3c9ed300-0cb8-47af-8c04-a06352a14b8d"
    safe_term_uuid_dupl = "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"
    safe_term_uuid_fhir = "34eb844c-ffff-4462-ad6d-48af68f1e8a1"
    safe_term_uuid_std = "c96200d7-9e30-4a0c-b98e-22d0ff146a99"

    def test_create_codeable_concept_happy(self):
        # Allow us to make multiple requests without database rolling back after each one
        self.app.config.update({
            "DISABLE_ROLLBACK_AFTER_REQUEST": True
        })
        self.app.config.update({
            "DISABLE_CLOSE_AFTER_REQUEST": True
        })

        # First, add a code to a test terminology
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code_schema": "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept",
                        "code_object": {
                            "text": "Automated Codeable Concept for Test",
                            "coding": [
                                {
                                  "code": "D59.9",
                                  "system": "http://hl7.org/fhir/sid/icd-10-cm"
                                },
                                {
                                  "code": "4854004",
                                  "system": "http://snomed.info/sct"
                                }
                            ]
                        },
                        "terminology_version_uuid": self.safe_term_uuid_dupl,
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

        self.assertEqual(self.conn.closed, False)

        # Then, load that terminology and verify the code is there and correct
        terminology = app.terminologies.models.Terminology.load(self.safe_term_uuid_dupl)
        terminology.load_content()

        display_to_code_object = {code.display: code for code in terminology.codes}
        new_code = display_to_code_object.get("Automated Codeable Concept for Test")
        self.assertIsNotNone(new_code)
        if new_code is not None:
            self.assertEqual(new_code.code_schema, app.models.codes.RoninCodeSchemas.codeable_concept)
            self.assertEqual(type(new_code.code), app.models.codes.FHIRCodeableConcept)
            self.assertEqual(new_code.display, "Automated Codeable Concept for Test")

    def test_create_code_happy_future(self):
        """
         happy path - all inputs - expiry date in future
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code at {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_dupl,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

    def test_create_code_happy_forever(self):
        """
        happy path - all inputs - no expiry date
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code at {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

    def test_create_code_happy_no_additional(self):
        """
        happy path - with no additional_data or depends_on
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

    def test_create_code_outdated(self):
        """
        outdated terminology version - expiry date in past
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_old,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.load_new_codes.closed_to_new_codes"
        assert result.get("message") == "Cannot add new codes to a terminology that has ended its effective period."

    def test_create_code_not_unique(self):
        """
        non-unique code value - the code "test code" already exists in the safe_term_uuid_fake Terminology
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": "test code",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.database_error"
        error_text = "(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint"
        assert error_text in result.get("message")

    def test_create_code_fhir(self):
        """
        Cannot add a code to a FHIR terminology
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fhir,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.load_new_codes.closed_to_new_codes"
        assert result.get("message") == "Cannot add new codes to a standard or FHIR terminology."

    def test_create_code_standard(self):
        """
        Cannot add a code to a standard terminology
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_std,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.load_new_codes.closed_to_new_codes"
        assert result.get("message") == "Cannot add new codes to a standard or FHIR terminology."

    def test_create_code_multiple_terminologies(self):
        """
        Cannot add a code if multiple terminologies are input
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_old,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    },
                    {
                        "code": f"""test code {datetime.datetime.utcnow()} 2""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.multiple_terminologies"
        assert result.get("message") == "Cannot create codes in multiple terminologies at the same time"

    def test_create_code_no_terminology(self):
        """
        Cannot add a code if no terminology is input
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.no_terminology"
        assert result.get("message") == "Cannot create codes when no terminology is input"

    def test_create_code_no_code_code(self):
        """
        Cannot add a code if the input payload does not provide a Code.code value
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.code_required"

    def test_create_code_no_code_display(self):
        """
        Cannot add a code if the input payload does not provide a Code.display value
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.display_required"

    def test_load_codes_multiple_terminologies(self):
        """
        Cannot load codes if multiple terminologies are input
        """
        terminology = app.terminologies.models.Terminology.load(self.safe_term_uuid_fake)
        payload = [
            {
                "code": f"""test code {datetime.datetime.utcnow()}""",
                "display": "test display",
                "terminology_version_uuid": self.safe_term_uuid_dupl,
                "depends_on_value": "a",
                "depends_on_display": "b",
                "depends_on_property": "c",
                "depends_on_system": "d",
                "additional_data": {
                    "data": "sweet sweet json"
                }
            },
            {
                "code": f"""test code {datetime.datetime.utcnow()} 2""",
                "display": "test display",
                "terminology_version_uuid": self.safe_term_uuid_fake,
                "depends_on_value": "a",
                "depends_on_display": "b",
                "depends_on_property": "c",
                "depends_on_system": "d",
                "additional_data": {
                    "data": "sweet sweet json"
                }
            }
        ]
        codes = create_code_payload_to_code_list(payload)
        with raises(BadRequestWithCode) as e:
            terminology.load_new_codes_to_terminology(codes)
        result = e.value
        assert result.code == "Terminology.load_new_codes.multiple_terminologies"
        assert result.description == "Cannot load codes to multiple terminologies at the same time"

    def test_load_codes_no_terminology(self):
        """
        Cannot load codes if no terminology is input
        """
        terminology = app.terminologies.models.Terminology.load(self.safe_term_uuid_fake)
        payload = [
            {
                "code": f"""test code {datetime.datetime.utcnow()}""",
                "display": "test display",
                "depends_on_value": "a",
                "depends_on_display": "b",
                "depends_on_property": "c",
                "depends_on_system": "d",
                "additional_data": {
                    "data": "sweet sweet json"
                }
            }
        ]

        with raises(BadRequestWithCode) as e:
            codes = create_code_payload_to_code_list(payload)
            terminology.load_new_codes_to_terminology(codes)

        result = e.value
        assert result.code == "Terminology.create_code.terminology_version_uuid_required"
        assert result.description == "A terminology_version_uuid must be provided for each code to create"

    def test_load_codes_class_conflict(self):
        """
        Cannot load codes if there is a code terminology identified in the input payload
        that is different from the Terminology that is calling load_new_codes_to_terminology()
        """
        terminology = app.terminologies.models.Terminology.load(self.safe_term_uuid_fake)
        payload = [
            {
                "code": f"""test code {datetime.datetime.utcnow()}""",
                "display": "test display",
                "terminology_version_uuid": self.safe_term_uuid_dupl,
                "depends_on_value": "a",
                "depends_on_display": "b",
                "depends_on_property": "c",
                "depends_on_system": "d",
                "additional_data": {
                    "data": "sweet sweet json"
                }
            }
        ]
        codes = create_code_payload_to_code_list(payload)
        with raises(BadRequestWithCode) as e:
            terminology.load_new_codes_to_terminology(codes)
        result = e.value
        assert result.code == "Terminology.load_new_codes.class_conflict"
        assert result.description == f"Cannot load codes to Terminology {self.safe_term_uuid_dupl} using the class for Terminology {self.safe_term_uuid_fake}"


if __name__ == "__main__":
    unittest.main()
