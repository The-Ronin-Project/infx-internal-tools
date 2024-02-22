import json
import unittest

from _pytest.python_api import raises

import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.database import get_db
from app.app import create_app
from app.errors import BadRequestWithCode


class RuleTests(unittest.TestCase):
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

    def test_loinc_rule(self):
        terminology_version = app.terminologies.models.Terminology.load(
            "554805c6-4ad1-4504-b8c7-3bab4e5196fd"
        )  # LOINC 2.74
        rule = app.value_sets.models.LOINCRule(
            uuid=None,
            position=None,
            description=None,
            prop="component",
            operator="=",
            value='{"Complete blood count W Auto Differential panel"}',
            include=True,
            value_set_version=None,
            fhir_system="http://loinc.org",
            terminology_version=terminology_version,
        )
        rule.execute()

        assert len(rule.results) == 1

        first_item = list(rule.results)[0]

        self.assertEqual(
            first_item.code, "57021-8", "The wrong LOINC code was provided"
        )
        self.assertEqual(
            first_item.display,
            "CBC W Auto Differential panel - Blood",
            "The wrong display was provided",
        )
        assert first_item.system == "http://loinc.org"
        assert first_item.version == "2.74"

    def test_snomed_rule(self):
        terminology_version = app.terminologies.models.Terminology.load(
            "306ae926-50aa-41d1-8ec8-1df123b0cd77"
        )
        rule = app.value_sets.models.SNOMEDRule(
            uuid=None,
            position=None,
            description=None,
            prop="ecl",
            operator="=",
            value="<<  73211009 |Diabetes mellitus|",
            include=True,
            value_set_version=None,
            fhir_system="http://snomed.info/sct",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(124, len(rule.results))

        first_item = list(rule.results)[0]

        self.assertEqual(first_item.system, "http://snomed.info/sct")
        self.assertEqual(first_item.version, "2023-03-01")

    def test_custom_terminology_entire_code_system_rule(self):
        """
        Tests that the include_entire_code_system rule for custom terminologies works
        """

        # Test Concept Map Versioning Source Terminology
        terminology_version = app.terminologies.models.Terminology.load(
            "d2b9133e-1566-4e06-a75e-e6b5c25aef85"
        )

        rule = app.value_sets.models.CustomTerminologyRule(
            uuid=None,
            position=None,
            description=None,
            prop="include_entire_code_system",
            operator="=",
            value="true",
            include=True,
            value_set_version=None,
            fhir_system="http://projectronin.io/fhir/CodeSystem/ronin/TestConceptMapVersioningSourceTerminology",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(len(rule.results), 11)

        first_code = list(rule.results)[0]
        self.assertEqual(
            first_code.system,
            "http://projectronin.io/fhir/CodeSystem/ronin/TestConceptMapVersioningSourceTerminology",
        )
        self.assertEqual(first_code.version, "1")

    def test_custom_terminology_code_rule(self):
        """
        Tests that the code_rule rule for custom terminologies works
        """

        terminology_version = app.terminologies.models.Terminology.load(
            "e28d33cb-a09c-4202-b0f1-f71fa20ffb14"
        )  # Custom_Cancer Staging Nomenclature version 3

        rule = app.value_sets.models.CustomTerminologyRule(
            uuid=None,
            position=None,
            description=None,
            prop="code",
            operator="in",
            value="N1",
            include=True,
            value_set_version=None,
            fhir_system="http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(len(rule.results), 1)

        first_code = list(rule.results)[0]
        self.assertEqual(
            first_code.system,
            "http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures",
        )
        self.assertEqual(first_code.version, "3")
        self.assertEqual(first_code.display, "Node stage N1")

    def test_custom_terminology_display_rule(self):
        """
        Tests that the display_regex rule for custom terminologies works
        """

        terminology_version = app.terminologies.models.Terminology.load(
            "e28d33cb-a09c-4202-b0f1-f71fa20ffb14"
        )  # Custom_Cancer Staging Nomenclature version 3

        rule = app.value_sets.models.CustomTerminologyRule(
            uuid=None,
            position=None,
            description=None,
            prop="display",
            operator="regex",
            value="Node stage N1",
            include=True,
            value_set_version=None,
            fhir_system="http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(len(rule.results), 1)

        first_code = list(rule.results)[0]
        self.assertEqual(
            first_code.system,
            "http://projectronin.io/fhir/CodeSystem/agnostic/AJCCStagingNomenclatures",
        )
        self.assertEqual(first_code.code, "N1")

    def test_rxnorm_rule(self):
        terminology_version = app.terminologies.models.Terminology.load(
            "4e78774b-059d-4c98-ae13-6a669c5ec783"
        )
        rule = app.value_sets.models.RxNormRule(
            uuid=terminology_version,  # Most recent version of RxNorm
            position=None,
            description=None,
            prop="term_type",
            operator="=",
            value="BPCK",  # The first value from the rule
            include=True,
            value_set_version=None,
            fhir_system="http://www.nlm.nih.gov/research/umls/rxnorm",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertLessEqual(
            684, len(rule.results)
        )  # expect RxNorm size to grow over time: 684 was July 2023

        first_code = list(rule.results)[0]
        self.assertEqual(
            first_code.system, "http://www.nlm.nih.gov/research/umls/rxnorm"
        )
        self.assertEqual(first_code.version, "2023-07-03")

    def test_rxnorm_expansion(self):
        terminology_version = app.terminologies.models.Terminology.load(
            "37ec7673-357a-4749-ac11-805dff145842"
        )  # Jan 2024 RxNorm version
        rule = app.value_sets.models.RxNormRule(
            uuid=terminology_version,  # Most recent version of RxNorm
            position=None,
            description=None,
            prop="term_type",
            operator="=",
            value="BPCK",  # The first value from the rule
            include=True,
            value_set_version=None,
            fhir_system="http://www.nlm.nih.gov/research/umls/rxnorm",
            terminology_version=terminology_version,
        )
        rule.execute()

        first_code = list(rule.results)[0]
        self.assertEqual(
            first_code.system, "http://www.nlm.nih.gov/research/umls/rxnorm"
        )
        self.assertEqual(first_code.version, "2024-01-02")
        self.assertLessEqual(
            4388, len(rule.results)
        )  # 4388 is the correct count for 691 RxNorm codes with BPCK as the term type and 3697 codes with status of quantified in January 2024; we expect this count to increase over time

    def test_icd10_cm_rule(self):
        terminology_version = app.terminologies.models.Terminology.load(
            "1808dad4-1cbe-4ff1-aa0c-8a7bdc104ad5"
        )
        rule = app.value_sets.models.ICD10CMRule(
            uuid=terminology_version,
            position=None,
            description=None,
            prop="code",
            operator="self-and-descendents",
            value="F10",
            include=True,
            value_set_version="",
            fhir_system="http://hl7.org/fhir/sid/icd-10-cm",
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(74, len(rule.results))

        first_code = list(rule.results)[0]
        self.assertEqual(first_code.system, "http://hl7.org/fhir/sid/icd-10-cm")
        self.assertEqual(first_code.version, "2023")

    def test_execute_rules_directly(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "component",
                        "operator": "in",
                        "value": '{"Alpha-1-Fetoprotein"}',
                        "include": True,
                        "terminology_version": "7c19e704-19d9-412b-90c3-79c5fb99ebe8",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 25

    def test_icd_10_cm_in_section(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "in-section",
                        "value": "d66586d4-5ed0-11ec-8f1f-00163e90ea35",
                        "include": True,
                        "terminology_version": "1ea19640-63e6-4e1b-b82f-be444ba395b4",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 32

    def test_icd_10_cm_in_chapter(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "in-chapter",
                        "value": "3f830074-5ed1-11ec-8f1f-00163e90ea35",
                        "include": True,
                        "terminology_version": "1ea19640-63e6-4e1b-b82f-be444ba395b4",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 1191

    def test_icd_10_pcs_has_body_system(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-body-system",
                        "value": [" Eye "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 1290

    def test_icd_10_pcs_has_root_operation(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-root-operation",
                        "value": [" Magnetic Resonance Imaging (MRI) "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        print(response.json)
        assert len(response.json) == 421

    def test_icd_10_pcs_has_device(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-device",
                        "value": [" Unenhanced and Enhanced "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 314

    def test_icd_10_pcs_has_body_part(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-body-part",
                        "value": [" Spinal Canal "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 152

    def test_icd_10_pcs_has_approach(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-approach",
                        "value": [" High Osmolar "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 581

    def test_icd_10_pcs_has_qualifier(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "has-qualifier",
                        "value": [" Atrium"],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    }
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 18

    def test_icd_10_pcs_multi_rule(self):
        response = self.client.post(
            "/ValueSets/rule_set/execute",
            data=json.dumps(
                [
                    {
                        "property": "code",
                        "operator": "in-section",
                        "value": ["Medical and Surgical "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "has-body-system",
                        "value": [" Central Nervous System and Cranial Nerves "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "has-root-operation",
                        "value": [" Bypass "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "has-body-part",
                        "value": [" Spinal Canal "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "in",
                        "value": ["001U077"],
                        "include": False,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "has-approach",
                        "value": [" Open "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                    {
                        "property": "code",
                        "operator": "has-device",
                        "value": [" Autologous Tissue Substitute "],
                        "include": True,
                        "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a",
                    },
                ]
            ),
            content_type="application/json",
        )
        assert len(response.json) == 4

    def test_update_single_rule(self):
        """
        rule 731913f0-32ca-11ee-90ef-5386acde3123 has
            value set version c316bbc8-1489-4320-9268-9edf9bedf7f1 and
            terminology 554805c6-4ad1-4504-b8c7-3bab4e5196fd
        """
        # happy path: valid rule UUID and value terminology version UUID
        response = self.client.patch(
            "/ValueSetRules/731913f0-32ca-11ee-90ef-5386acde3123",
            data=json.dumps(
                {"new_terminology_version_uuid": "554805c6-4ad1-4504-b8c7-3bab4e5196fd"}
            ),
            content_type="application/json",
        )
        assert response.text == "OK"

        # invalid rule UUID provided in URL: gives VSRule.load() NotFoundException
        response = self.client.patch(
            "/ValueSetRules/11111111-1111-1111-1111-111111111111",
            data=json.dumps(
                {"new_terminology_version_uuid": "554805c6-4ad1-4504-b8c7-3bab4e5196fd"}
            ),
            content_type="application/json",
        )
        result = response.json
        assert (
            result.get("message")
            == "No Value Set Rule found with UUID: 11111111-1111-1111-1111-111111111111"
        )

        # No rule UUID provided in URL: gives app.views.update_single_rule() API URL endpoint failure
        response = self.client.patch(
            "/ValueSetRules/",
            data=json.dumps(
                {"new_terminology_version_uuid": "554805c6-4ad1-4504-b8c7-3bab4e5196fd"}
            ),
            content_type="application/json",
        )
        result = response.json
        assert (
            result.get("message")
            == "The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again."
        )

        # None input to VSRule.load(): BadRequestWithCode for empty ID
        with raises(BadRequestWithCode):
            app.value_sets.models.VSRule.load(None)

        # No terminology UUID provided in parameters: VSRule.update() BadRequestWithCode for empty ID
        response = self.client.patch(
            "/ValueSetRules/731913f0-32ca-11ee-90ef-5386acde3123",
            data=json.dumps({}),
            content_type="application/json",
        )
        result = response.json
        assert result.get("code") == "ValueSetRule.update.empty"
        assert (
            result.get("message")
            == "Cannot update Value Set Rule: empty Terminology Version ID"
        )


if __name__ == "__main__":
    unittest.main()
