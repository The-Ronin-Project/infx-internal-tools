import hashlib
import json
import unittest
from unittest.mock import patch, Mock

from pytest import raises
from sqlalchemy import text

import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.database import get_db
from app.app import create_app
from app.errors import NotFoundException


class ValueSetTests(unittest.TestCase):
    """
    There are 8 value_sets.value_set rows safe to use in tests.
    ```
    uuid                                    title                                           description
    "ca75b03c-1763-44fd-9bfa-4fe015ff809c"	"Test ONLY: Mirth Validation Test Observations" "For automated test in Mirth"
    "c7c37780-e727-42f6-9d1b-d823d75171ad"	"Test ONLY: Test August 29"                     "no map + diabetes"
    "50ead103-a8c9-4aae-b5f0-f1e51b264323"	"Test ONLY: Test Condition Incremental Load"... "testing source terminology for condition incremental load"
    "236b88af-40c2-4d59-b319-a5e68865afdc"	"Test ONLY: test fhir and vs description"       "the value set description goes here"
    "ccba9765-66ee-4742-a656-4e37d0811958"	"Test ONLY: Test Observation Incremental"...	"Observations for Incremental Load testing"
    "fc82ec39-7b9f-4d74-9a34-adf86db1a50f"	"Test ONLY: Automated Testing Value Set"	    "For automated testing in infx-internal-tools"
    "b5f97703-abf3-4fc0-aa49-f8851a3fced4"	"Test ONLY: Test ValueSet for diffs"            "This valueset will have a small number of codes for diff check"
    "477195c0-8a91-11ec-ac15-073d0cb083df"	"Test ONLY: Testing Value Set test. Yay"        "Various codes and code systems test"
    "e49af176-189f-4536-8231-e58a261ed36d"	"Test ONLY: Concept Map Versioning Target"...   made up target concepts and inactive codes" (has no ValueSetVersion)

    ```
    You may want various status of value sets for different test cases. To find those with active status:
    ```
    select * from
    value_sets.value_set_version
    where value_set_uuid in (
    'ca75b03c-1763-44fd-9bfa-4fe015ff809c',
    'c7c37780-e727-42f6-9d1b-d823d75171ad',
    '50ead103-a8c9-4aae-b5f0-f1e51b264323',
    '236b88af-40c2-4d59-b319-a5e68865afdc',
    'ccba9765-66ee-4742-a656-4e37d0811958',
    'fc82ec39-7b9f-4d74-9a34-adf86db1a50f',
    'b5f97703-abf3-4fc0-aa49-f8851a3fced4',
    '477195c0-8a91-11ec-ac15-073d0cb083df',
    'e49af176-189f-4536-8231-e58a261ed36d'
    )
    and status = 'active'

    ```
    To see all past versions of safe value sets, regardless of status, use this query:
    ```
    select * from
    value_sets.value_set_version
    where value_set_uuid in (
    'ca75b03c-1763-44fd-9bfa-4fe015ff809c',
    'c7c37780-e727-42f6-9d1b-d823d75171ad',
    '50ead103-a8c9-4aae-b5f0-f1e51b264323',
    '236b88af-40c2-4d59-b319-a5e68865afdc',
    'ccba9765-66ee-4742-a656-4e37d0811958',
    'fc82ec39-7b9f-4d74-9a34-adf86db1a50f',
    'b5f97703-abf3-4fc0-aa49-f8851a3fced4',
    '477195c0-8a91-11ec-ac15-073d0cb083df'
    )
    order by value_set_uuid desc

    ```
    """

    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update(
            {
                "TESTING": True,
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    # UUID values: value_sets.value_set
    # version and status may change over time; before using in a test, double-check with a query: see class doc above
    # expected status is active:
    safe_value_set_uuid_obsrv_mirth = "ca75b03c-1763-44fd-9bfa-4fe015ff809c"
    safe_value_set_uuid_nomap_diab = "c7c37780-e727-42f6-9d1b-d823d75171ad"
    safe_value_set_uuid_cond_load = "50ead103-a8c9-4aae-b5f0-f1e51b264323"
    safe_value_set_uuid_obsrv_load = "ccba9765-66ee-4742-a656-4e37d0811958"
    safe_value_set_uuid_code_systems = "477195c0-8a91-11ec-ac15-073d0cb083df"
    # expected status is obsolete:
    safe_value_set_uuid_code_diffs = "b5f97703-abf3-4fc0-aa49-f8851a3fced4"
    safe_value_set_uuid_descrip = "236b88af-40c2-4d59-b319-a5e68865afdc"
    # expected status is in progress:
    safe_value_set_uuid_auto_tool = "fc82ec39-7b9f-4d74-9a34-adf86db1a50f"

    # UUID values: (as needed) value_sets.value_set_version
    safe_value_set_uuid_auto_tool_version = "58e792d9-1264-4f18-b16e-6292cb7ca597"

    # UUID values: (as needed) value_sets.expansion
    safe_value_set_uuid_auto_tool_expansion = "640e5226-79a6-11ee-93aa-b2cb39228ed3"

    # UUID values: public.terminology_versions (documentation see test_codes.py)
    safe_term_uuid_fake = "d2ae0de5-0168-4f54-924a-1f79cf658939"
    safe_term_uuid_old = "3c9ed300-0cb8-47af-8c04-a06352a14b8d"
    safe_term_uuid_dupl = "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"

    # UUID value of Test ONLY: Custom Terminology Value Set version
    custom_terminology_value_set_version = "b8de6b05-5f0e-4a9d-a872-7cb265a52311"
    # UUID value of Testing ONLY: Custom Terminology Codeable Concept Value Set version
    codeable_concept_custom_terminology_value_set_version = (
        "8c73a151-8bf8-46d1-bae6-e275c1e4a14e"
    )
    # UUID value of Testing ONLY: UCUM Value Set version
    ucum_value_set_version = "a7f8a7a2-7294-4dd8-b3d4-ffac95054af1"

    # UUID value of Testing ONLY: SNOMED Value Set version
    snomed_value_set_version = "43d730b4-7191-4d92-b3ce-b61dc31998c7"

    def test_value_set_expand(self):
        """
        Expand the 'Automated Testing Value Set' value set and verify the outputs,
        catch lower level error if value set version uuid isn't valid
        """
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.safe_value_set_uuid_auto_tool_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(11509, len(value_set_version.expansion))

    def test_custom_terminology_value_set(self):
        """
        Tests making a new expanison and loading the expansion for custom terminology value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.custom_terminology_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(28, len(value_set_version.expansion))

        expected_subset_codes = ["N2", "N0b", "N3"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.custom_terminology_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 28)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "N2"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ),
            {"expansion_uuid": expansion_uuid, "code_to_check": code_to_check},
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(
            app.models.codes.RoninCodeSchemas.code.value, result.code_schema
        )
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual(
            "http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures",
            result.system,
        )
        self.assertEqual("3", result.version)

    def test_codeable_concept_custom_terminology_value_set(self):
        """
        Tests making a new expanison and loading the expansion for custom terminology value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.codeable_concept_custom_terminology_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(1, len(value_set_version.expansion))

        expected_code_object_json = {
            "coding": [
                {"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": "D59.9"},
                {"system": "http://snomed.info/sct", "code": "4854004"},
            ],
            "text": "Anemia, hemolytic, acquired (CMS/HCC)",
        }
        example_codeable_concept = app.models.codes.FHIRCodeableConcept.deserialize(
            expected_code_object_json
        )
        actual_code_object = list(value_set_version.expansion)[0]
        # Check that each expected code in the subset is present in the actual codes
        self.assertEqual(type(example_codeable_concept), type(actual_code_object.code))
        self.assertEqual(
            "Anemia, hemolytic, acquired (CMS/HCC)", actual_code_object.display
        )

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.codeable_concept_custom_terminology_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 1)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        target_display_to_check = list(value_set_version.expansion)[0].display

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and display=:target_display_to_check
                """
            ),
            {
                "expansion_uuid": expansion_uuid,
                "target_display_to_check": target_display_to_check,
            },
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(
            app.models.codes.RoninCodeSchemas.codeable_concept.value, result.code_schema
        )
        # self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNotNone(result.code_jsonb)
        self.assertEqual(result.display, target_display_to_check)

    def test_snomed_value_set(self):
        """
        Tests making a new expanison and loading the expansion for custom terminology value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.snomed_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(1988, len(value_set_version.expansion))

        expected_subset_codes = ["9058002", "697897003", "845006"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.snomed_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 1988)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "9058002"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ),
            {"expansion_uuid": expansion_uuid, "code_to_check": code_to_check},
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(
            app.models.codes.RoninCodeSchemas.code.value, result.code_schema
        )
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual(
            "http://snomed.info/sct",
            result.system,
        )
        self.assertEqual("2023-09-01", result.version)

    def test_ucum_value_set(self):
        """
        Tests making a new expanison and loading the expansion for custom terminology value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.ucum_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(2, len(value_set_version.expansion))

        expected_subset_codes = ["cm", "[in_i]"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.ucum_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 2)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "cm"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ),
            {"expansion_uuid": expansion_uuid, "code_to_check": code_to_check},
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(
            app.models.codes.RoninCodeSchemas.code.value, result.code_schema
        )
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual(
            "http://unitsofmeasure.org",
            result.system,
        )
        self.assertEqual("3.0.0", result.version)

    def test_value_set_not_found(self):
        with raises(NotFoundException) as e:
            app.value_sets.models.ValueSet.load(self.safe_term_uuid_dupl)
        result = e.value
        assert (
            result.message
            == f"No Value Set found with UUID: {self.safe_term_uuid_dupl}"
        )

    def test_value_set_version_not_found(self):
        with raises(NotFoundException) as e:
            app.value_sets.models.ValueSetVersion.load(self.safe_term_uuid_dupl)
        result = e.value
        assert (
            result.message
            == f"No Value Set Version found with UUID: {self.safe_term_uuid_dupl}"
        )

    def test_expansion_report(self):
        response = self.client.get(
            f"/ValueSets/expansions/{self.safe_value_set_uuid_auto_tool_expansion}/report"
        )
        assert (
            hashlib.md5(response.data).hexdigest() == "8cb508cafbae9fba542270698aa9db9e"
        )


if __name__ == "__main__":
    unittest.main()
