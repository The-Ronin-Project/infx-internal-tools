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
    # UUID value of "Testing ONLY: Custom RxNorm Value Set" version
    custom_rxnorm_value_set_version = "64be42af-ded8-40b4-838e-56df16a18ba8"
    # UUID value of Test Only: LOINC Value Set value set version
    loinc_value_set_version = "7e34e437-add3-4224-9018-31e447c26d26"

    icd_10_cm_value_set_version = "f50ab829-110a-493a-be22-a49ec18c2161"
    fhir_terminology_value_set_version = "770f63eb-793f-4be5-9870-510c05f5801c"

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
        Tests making a new expansion and loading the expansion for custom terminology value set.
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
            ), {
                "expansion_uuid": expansion_uuid,
                "code_to_check": code_to_check
            }
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(app.models.codes.RoninCodeSchemas.code.value, result.code_schema)
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual("http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures", result.system)
        self.assertEqual("3", result.version)

    def test_loinc_value_set(self):
        """
        Tests making a new expanison and loading the expansion for a LOINC value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.loinc_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(1, len(value_set_version.expansion))

        expected_subset_codes = ["41021-7"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.loinc_value_set_version
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
        code_to_check = "41021-7"

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
            "http://loinc.org",
            result.system,
        )
        self.assertEqual("2.76", result.version)

    def test_rxnorm_value_set(self):
        """
        Tests making a new expansion and loading the expansion for rxnorm value set.
        """

        expected_value_set_size = 3829
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.custom_rxnorm_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(expected_value_set_size, len(value_set_version.expansion))

        expected_subset_codes = ["2584864", "845182", "2637048"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #
        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.custom_rxnorm_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())

        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), expected_value_set_size)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "845182"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ), {
                "expansion_uuid": expansion_uuid,
                "code_to_check": code_to_check
            }
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(app.models.codes.RoninCodeSchemas.code.value, result.code_schema)
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual("http://www.nlm.nih.gov/research/umls/rxnorm", result.system)
        self.assertEqual("2024-02-05", result.version)

    def test_icd_10_cm_value_set(self):
        """
        Tests making a new expansion and loading the expansion for ICD-10 CM value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.icd_10_cm_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(8, len(value_set_version.expansion))

        expected_subset_codes = ["R05", "R04.2"]
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.icd_10_cm_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 8)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "R05"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ), {
                "expansion_uuid": expansion_uuid,
                "code_to_check": code_to_check
            }
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(app.models.codes.RoninCodeSchemas.code.value, result.code_schema)
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual("http://hl7.org/fhir/sid/icd-10-cm", result.system)
        self.assertEqual("2024", result.version)

    def test_fhir_terminology_value_set(self):
        """
        Tests making a new expansion and loading the expansion for FHIR Terminology value set.
        """
        #
        # Step 1: Expand the value set and verify the expansion
        #
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.fhir_terminology_value_set_version
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(7, len(value_set_version.expansion))

        expected_subset_codes = ['sms', 'email', 'phone']
        actual_codes = [code.code for code in value_set_version.expansion]
        # Check that each expected code in the subset is present in the actual codes
        for expected_code in expected_subset_codes:
            self.assertIn(expected_code, actual_codes)

        #
        # Step 2: Test loading the expansion from the database
        #

        del value_set_version
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            self.fhir_terminology_value_set_version
        )
        self.assertTrue(value_set_version.expansion_already_exists())
        # value_set_version.expand()
        value_set_version.load_current_expansion()
        current_expansion = value_set_version.expansion

        self.assertEqual(len(current_expansion), 7)

        #
        # Step 3: Directly query the database and verify a complete row
        #
        expansion_uuid = value_set_version.expansion_uuid
        code_to_check = "sms"

        result = self.conn.execute(
            text(
                """
                select * from value_sets.expansion_member_data
                where expansion_uuid=:expansion_uuid
                and code_simple=:code_to_check
                """
            ), {
                "expansion_uuid": expansion_uuid,
                "code_to_check": code_to_check
            }
        ).one_or_none()

        self.assertIsNotNone(result)
        self.assertEqual(app.models.codes.RoninCodeSchemas.code.value, result.code_schema)
        self.assertEqual(code_to_check, result.code_simple)
        self.assertIsNone(result.code_jsonb)
        self.assertEqual("http://hl7.org/fhir/contact-point-system", result.system)
        self.assertEqual("4.0.1", result.version)

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
