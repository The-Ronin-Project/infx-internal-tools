import unittest
import json

from _pytest.python_api import raises

from app.database import get_db
from app.app import create_app
from app.helpers.id_helper import generate_code_id


class IdHelperTests(unittest.TestCase):
    """
    Dev note: comment out db calls to let any format-only tests run without the database connection they don't need
    """
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_generate_code_id_codeable_concept(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"}, {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}], "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display=display,
            depends_on_property="",
            depends_on_display="",
            depends_on_system="",
            depends_on_value_string=""
        )
        print(code_id)

    def test_hash_method_order_independent_coding(self):
        example_1 = '{"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"}, {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}], "text": "Traumatic hematuria"}'
        display_1 = "Traumatic hematuria"

        example_2 = '{"coding": [{"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}, {"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"}], "text": "Traumatic hematuria"}'
        display_2 = "Traumatic hematuria"

        self.assertNotEqual(example_1, example_2)

        code_id_example_1 = generate_code_id(
            code_string=example_1,
            display=display_1,
            depends_on_property="",
            depends_on_display="",
            depends_on_system="",
            depends_on_value_string=""
        )

        code_id_example_2 = generate_code_id(
            code_string=example_2,
            display=display_2,
            depends_on_property="",
            depends_on_display="",
            depends_on_system="",
            depends_on_value_string=""
        )

        self.assertEqual(code_id_example_1, code_id_example_2)

    def test_hash_method_oid_translation(self):
        example_1 = "{\"coding\": [{\"system\": \"http://hl7.org/fhir/sid/icd-9-cm/diagnosis\", \"code\": \"244.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\"}, {\"system\": \"urn:oid:2.16.840.1.113883.6.90\", \"code\": \"E03.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\"}], \"text\": \"Hypothyroidism due to medicaments and other exogenous substances\"}"
        display_1 = "Hypothyroidism due to medicaments and other exogenous substances"

        example_2 = "{\"coding\": [{\"code\": \"244.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\", \"system\": \"http://hl7.org/fhir/sid/icd-9-cm/diagnosis\"}, {\"code\": \"E03.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\", \"system\": \"http://hl7.org/fhir/sid/icd-10-cm\"}], \"text\": \"Hypothyroidism due to medicaments and other exogenous substances\"}"
        display_2 = "Hypothyroidism due to medicaments and other exogenous substances"

        self.assertNotEqual(example_1, example_2)

        code_id_example_1 = generate_code_id(
            code_string=example_1,
            display=display_1,
            depends_on_property="",
            depends_on_display="",
            depends_on_system="",
            depends_on_value_string=""
        )

        code_id_example_2 = generate_code_id(
            code_string=example_2,
            display=display_2,
            depends_on_property="",
            depends_on_display="",
            depends_on_system="",
            depends_on_value_string=""
        )

        self.assertEqual(code_id_example_1, code_id_example_2)