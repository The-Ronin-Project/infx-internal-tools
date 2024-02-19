import unittest
import json

from _pytest.python_api import raises

from app.database import get_db
from app.app import create_app
from app.helpers.id_helper import generate_code_id, hash_for_code_id, generate_mapping_id_with_source_code_id


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
        self.code_id_result = [
            "744a8a9a41a0025d35eca0d5379bc7a7",
            "4b3daabe7f11f024188aa73678207240",
            "a56bcb3337d5a8841d917d16ce3787a9",
            "b8faa49395fdc7538629dcad59e65b52",
            "a8145d9f6aa4323c045d8388a62061ce",
            "5d0eb4fc1d14102c1dfcffa53ca7bd6f"
        ]
        self.mapping_id_result = [
            "00719ef1d9f3bdf63879a4b50989cc67",
            "b65ae309bf1332d1ba7829cbf27a8a75",
            "90fd88b1677213b872361b2c793362f5",
            "461ac4e5fcff447b93d9e0eb8856529d"
        ]

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def code_changes_unique(self, index: int, code_id: str):
        self.assertEqual(self.code_id_result[index], code_id)
        for i, result in enumerate(self.code_id_result):
            if i != index:
                self.assertNotEqual(self.code_id_result[i], code_id)

    def mapping_changes_unique(self, index: int, mapping_id: str):
        self.assertEqual(self.mapping_id_result[index], mapping_id)
        for i, result in enumerate(self.mapping_id_result):
            if i != index:
                self.assertNotEqual(self.mapping_id_result[i], mapping_id)

    def test_generate_code_id_codeable_concept_typical_optional_missing_3(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

    def test_generate_code_id_codeable_concept_typical_optional_empty_3(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display,
            depends_on_value_string="",
            depends_on_property="",
            depends_on_system="",
            depends_on_display="",
        )
        self.code_changes_unique(index=3, code_id=code_id)

    def test_generate_code_id_codeable_concept_full_v4_depends_on_same_as_v5(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display,
            depends_on_value_string="a",
            depends_on_property="b",
            depends_on_system="c",
            depends_on_display="d",
        )
        self.code_changes_unique(index=4, code_id=code_id)

    def test_generate_code_id_codeable_concept_full_v5_depends_on_same_as_v4(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display,
            depends_on_value_string="abcd",
        )
        self.code_changes_unique(index=4, code_id=code_id)

    def test_generate_code_id_codeable_concept_changes_5(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display,
            depends_on_value_string="a"
        )
        self.code_changes_unique(index=5, code_id=code_id)

    def test_generate_code_id_codeable_concept_changes_0(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
        )
        self.code_changes_unique(index=0, code_id=code_id)

    def test_generate_code_id_codeable_concept_changes_1(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string="a",
        )
        self.code_changes_unique(index=1, code_id=code_id)

    def test_generate_code_id_codeable_concept_changes_2(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            depends_on_value_string="a",
        )
        self.code_changes_unique(index=2, code_id=code_id)

    def test_generate_code_id_codeable_concept_changes_system_no_value(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        with raises(ValueError) as e:
            generate_code_id(
                code_string=example_codeable_concept_json,
                depends_on_system="a",
            )
        result = e.value
        assert str(result) == "If there is a depends_on_system there must be a depends_on_value"

    def test_generate_code_id_codeable_concept_changes_display_no_value(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        with raises(ValueError) as e:
            generate_code_id(
                code_string=example_codeable_concept_json,
                depends_on_display="a",
            )
        result = e.value
        assert str(result) == "If there is a depends_on_display there must be a depends_on_value"

    def test_generate_code_id_codeable_concept_changes_property_no_value(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        with raises(ValueError) as e:
            generate_code_id(
                code_string=example_codeable_concept_json,
                depends_on_property="a",
            )
        result = e.value
        assert str(result) == "If there is a depends_on_property there must be a depends_on_value"

    def test_generate_code_id_codeable_concept_input_empty(self):
        with raises(ValueError) as e:
            generate_code_id(code_string="")
        result = e.value
        assert str(result) == "Cannot use an empty code_string for a code_id"

    def test_generate_code_id_codeable_concept_input_none(self):
        with raises(ValueError) as e:
            generate_code_id(code_string=None)
        result = e.value
        assert str(result) == "Cannot use an empty code_string for a code_id"

    def test_hash_for_code_id_codeable_concept_input_empty(self):
        with raises(ValueError) as e:
            hash_for_code_id(code_string="")
        result = e.value
        assert str(result) == "Cannot use an empty code_string for a code_id"

    def test_hash_for_code_id_codeable_concept_input_none(self):
        with raises(ValueError) as e:
            hash_for_code_id(code_string=None)
        result = e.value
        assert str(result) == "Cannot use an empty code_string for a code_id"

    def test_generate_code_id_order_independent_coding(self):
        example_1 = '{"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"}, {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}], "text": "Traumatic hematuria"}'
        display_1 = "Traumatic hematuria"

        example_2 = '{"coding": [{"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}, {"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"}], "text": "Traumatic hematuria"}'
        display_2 = "Traumatic hematuria"

        self.assertNotEqual(example_1, example_2)

        code_id_example_1 = generate_code_id(
            code_string=example_1,
            display_string=display_1,
        )

        code_id_example_2 = generate_code_id(
            code_string=example_2,
            display_string=display_2,
        )

        self.assertEqual(code_id_example_1, code_id_example_2)

    def test_generate_code_id_oid_translation(self):
        example_1 = "{\"coding\": [{\"system\": \"http://hl7.org/fhir/sid/icd-9-cm/diagnosis\", \"code\": \"244.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\"}, {\"system\": \"urn:oid:2.16.840.1.113883.6.90\", \"code\": \"E03.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\"}], \"text\": \"Hypothyroidism due to medicaments and other exogenous substances\"}"
        display_1 = "Hypothyroidism due to medicaments and other exogenous substances"

        example_2 = "{\"coding\": [{\"code\": \"244.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\", \"system\": \"http://hl7.org/fhir/sid/icd-9-cm/diagnosis\"}, {\"code\": \"E03.2\", \"display\": \"Hypothyroidism due to medicaments and other exogenous substances\", \"system\": \"http://hl7.org/fhir/sid/icd-10-cm\"}], \"text\": \"Hypothyroidism due to medicaments and other exogenous substances\"}"
        display_2 = "Hypothyroidism due to medicaments and other exogenous substances"

        self.assertNotEqual(example_1, example_2)

        code_id_example_1 = generate_code_id(
            code_string=example_1,
            display_string=display_1,
        )

        code_id_example_2 = generate_code_id(
            code_string=example_2,
            display_string=display_2,
        )

        self.assertEqual(code_id_example_1, code_id_example_2)

    def test_generate_mapping_id_missing_no_input(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id()
        result = e.value
        assert str(
            result) == "generate_mapping_id_with_source_code_id() missing 3 required positional arguments: 'source_code_id', 'relationship_code', and 'target_concept_code'"

    def test_generate_mapping_id_missing_rel_target(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 2 required positional arguments: 'relationship_code' and 'target_concept_code'"

    def test_generate_mapping_id_missing_code_target(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(relationship_code="a")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 2 required positional arguments: 'source_code_id' and 'target_concept_code'"

    def test_generate_mapping_id_missing_code_rel(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(target_concept_code="a")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 2 required positional arguments: 'source_code_id' and 'relationship_code'"

    def test_generate_mapping_id_missing_code(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(relationship_code="a", target_concept_code="b")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 1 required positional argument: 'source_code_id'"

    def test_generate_mapping_id_empty_code(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="", relationship_code="a", target_concept_code="b")
        result = e.value
        assert str(result) == "Cannot create mapping_id without a source_code_id"

    def test_generate_mapping_id_none_code(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id=None, relationship_code="a", target_concept_code="b")
        result = e.value
        assert str(result) == "Cannot create mapping_id without a source_code_id"

    def test_generate_mapping_id_missing_rel(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", target_concept_code="b")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 1 required positional argument: 'relationship_code'"

    def test_generate_mapping_id_empty_rel(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", relationship_code="", target_concept_code="b")
        result = e.value
        assert str(result) == "Cannot create mapping_id without a relationship_code"

    def test_generate_mapping_id_none_rel(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", relationship_code=None, target_concept_code="b")
        result = e.value
        assert str(result) == "Cannot create mapping_id without a relationship_code"

    def test_generate_mapping_id_missing_target(self):
        with raises(TypeError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", relationship_code="b")
        result = e.value
        assert str(result) == "generate_mapping_id_with_source_code_id() missing 1 required positional argument: 'target_concept_code'"

    def test_generate_mapping_id_empty_target(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", relationship_code="b", target_concept_code="")
        result = e.value
        assert str(result) == "Cannot create mapping_id without a target_concept_code"

    def test_generate_mapping_id_none_target(self):
        with raises(ValueError) as e:
            generate_mapping_id_with_source_code_id(source_code_id="a", relationship_code="b", target_concept_code=None)
        result = e.value
        assert str(result) == "Cannot create mapping_id without a target_concept_code"

    def test_generate_mapping_id_full_3_0(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

        mapping_id = generate_mapping_id_with_source_code_id(
            source_code_id=code_id,
            relationship_code="source-is-narrower-than-target",
            target_concept_code="484348",
            target_concept_display="omega-3 acid ethyl esters (USP)",
            target_concept_system="RxNorm",
        )
        self.mapping_changes_unique(index=0, mapping_id=mapping_id)

    def test_generate_mapping_id_all_required_optional_missing_3_1(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

        mapping_id = generate_mapping_id_with_source_code_id(
            source_code_id=code_id,
            relationship_code="source-is-narrower-than-target",
            target_concept_code="484348",
        )
        self.mapping_changes_unique(index=1, mapping_id=mapping_id)

    def test_generate_mapping_id_all_required_optional_empty_3_1(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

        mapping_id = generate_mapping_id_with_source_code_id(
            source_code_id=code_id,
            relationship_code="source-is-narrower-than-target",
            target_concept_code="484348",
            target_concept_display="",
            target_concept_system="",
        )
        self.mapping_changes_unique(index=1, mapping_id=mapping_id)

    def test_generate_mapping_id_changes_3_2(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

        mapping_id = generate_mapping_id_with_source_code_id(
            source_code_id=code_id,
            relationship_code="source-is-narrower-than-target",
            target_concept_code="484348",
            target_concept_display="a",
        )
        self.mapping_changes_unique(index=2, mapping_id=mapping_id)

    def test_generate_mapping_id_changes_3_3(self):
        example_codeable_concept = {"coding": [{"code": "R31.9", "system": "urn:oid:2.16.840.1.113883.6.90"},
                                               {"code": "95567008", "system": "urn:oid:2.16.840.1.113883.6.96"}],
                                    "text": "Traumatic hematuria"}
        display = example_codeable_concept.get("text")

        example_codeable_concept_json = json.dumps(example_codeable_concept)

        code_id = generate_code_id(
            code_string=example_codeable_concept_json,
            display_string=display
        )
        self.code_changes_unique(index=3, code_id=code_id)

        mapping_id = generate_mapping_id_with_source_code_id(
            source_code_id=code_id,
            relationship_code="source-is-narrower-than-target",
            target_concept_code="484348",
            target_concept_system="a",
        )
        self.mapping_changes_unique(index=3, mapping_id=mapping_id)
