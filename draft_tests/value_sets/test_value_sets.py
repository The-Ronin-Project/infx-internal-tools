import unittest
import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.database import get_db


class ValueSetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_value_set_expand(self):
        """
        Expand the 'Automated Testing Value Set' value set and verify the outputs
        """
        # ToDo: add better, more stable rules to this
        value_set_version_uuid = '58e792d9-1264-4f18-b16e-6292cb7ca597'
        value_set_version = app.value_sets.models.ValueSetVersion.load(value_set_version_uuid)
        value_set_version.expand(force_new=True)

        self.assertEqual(11509, len(value_set_version.expansion))


class RuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_loinc_rule(self):
        terminology_version = app.terminologies.models.Terminology.load('554805c6-4ad1-4504-b8c7-3bab4e5196fd')  # LOINC 2.74
        rule = app.value_sets.models.LOINCRule(
            uuid=None,
            position=None,
            description=None,
            prop='component',
            operator='=',
            value='{"Complete blood count W Auto Differential panel"}',
            include=True,
            value_set_version=None,
            fhir_system='http://loinc.org',
            terminology_version=terminology_version,
        )
        rule.execute()

        assert len(rule.results) == 1

        first_item = list(rule.results)[0]

        self.assertEqual(first_item.code, '57021-8', 'The wrong LOINC code was provided')
        self.assertEqual(first_item.display, 'CBC W Auto Differential panel - Blood', 'The wrong display was provided')
        assert first_item.system == 'http://loinc.org'
        assert first_item.version == '2.74'

    def test_snomed_rule(self):
        terminology_version = app.terminologies.models.Terminology.load('306ae926-50aa-41d1-8ec8-1df123b0cd77')
        rule = app.value_sets.models.SNOMEDRule(
            uuid=None,
            position=None,
            description=None,
            prop='ecl',
            operator='=',
            value='<<  73211009 |Diabetes mellitus|',
            include=True,
            value_set_version=None,
            fhir_system='http://snomed.info/sct',
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(124, len(rule.results))

        first_item = list(rule.results)[0]

        self.assertEqual(first_item.system, 'http://snomed.info/sct')
        self.assertEqual(first_item.version, '2023-03-01')


    def test_custom_terminology_rule(self):
        """
        Tests that the include_entire_code_system rule for custom terminologies works
        """

        # Test Concept Map Versioning Source Terminology
        terminology_version = app.terminologies.models.Terminology.load('d2b9133e-1566-4e06-a75e-e6b5c25aef85')

        rule = app.value_sets.models.CustomTerminologyRule(
            uuid=None,
            position=None,
            description=None,
            prop='include_entire_code_system',
            operator='=',
            value='true',
            include=True,
            value_set_version=None,
            fhir_system='http://projectronin.io/fhir/CodeSystem/ronin/TestConceptMapVersioningSourceTerminology',
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(len(rule.results), 11)

        first_code = list(rule.results)[0]
        self.assertEqual(first_code.system, 'http://projectronin.io/fhir/CodeSystem/ronin/TestConceptMapVersioningSourceTerminology')
        self.assertEqual(first_code.version, '1')

    def test_rxnorm_rule(self):
        # todo: how can we have a stable test if RxNorm is always changing?
        terminology_version = app.terminologies.models.Terminology.load('4e78774b-059d-4c98-ae13-6a669c5ec783')
        rule = app.value_sets.models.RxNormRule(
            uuid=terminology_version,  # Most recent version of RxNorm
            position=None,
            description=None,
            prop='term_type',
            operator='=',
            value='BPCK',  # The first value from the rule
            include=True,
            value_set_version=None,
            fhir_system='http://www.nlm.nih.gov/research/umls/rxnorm',
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(684, len(rule.results))

        first_code = list(rule.results)[0]
        self.assertEqual(first_code.system,
                         'http://www.nlm.nih.gov/research/umls/rxnorm')
        self.assertEqual(first_code.version, '2023-07-03')

    def test_icd10_cm_rule(self):
        terminology_version = app.terminologies.models.Terminology.load('1808dad4-1cbe-4ff1-aa0c-8a7bdc104ad5')
        rule = app.value_sets.models.ICD10CMRule(
            uuid=terminology_version,
            position=None,
            description=None,
            prop='code',
            operator='self-and-descendents',
            value='F10',
            include=True,
            value_set_version='',
            fhir_system='http://hl7.org/fhir/sid/icd-10-cm',
            terminology_version=terminology_version,
        )
        rule.execute()

        self.assertEqual(74, len(rule.results))

        first_code = list(rule.results)[0]
        self.assertEqual(first_code.system,
                         'http://hl7.org/fhir/sid/icd-10-cm')
        self.assertEqual(first_code.version, '2023')


if __name__ == '__main__':
    unittest.main()
