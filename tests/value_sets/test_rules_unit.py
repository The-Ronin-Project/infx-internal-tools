import unittest

import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.app import create_app


class LOINCRuleUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app()
        self.app.config.update(
            {
                "TESTING": True,
            }
        )
        self.client = self.app.test_client()

    def get_test_loinc_rule(self, test_value):
        return app.value_sets.models.LOINCRule(
            uuid=None,
            position=None,
            description=None,
            prop="component",
            operator="=",
            value=test_value,
            include=True,
            value_set_version=None,
            fhir_system="http://loinc.org",
            terminology_version="dummy_terminonlogy_version_uuid",
        )

    def test_split_value_simple(self):
        """
        Given a simple value of a string of delimited elements
        When split_list is called
        Then the delimited values are split into a list
        """

        test_list = [
            "Alpha-1-Fetoprotein",
            "Alpha-1-Fetoprotein Ab",
            "Alpha-1-Fetoprotein.tumor marker",
        ]
        test_value = ",".join(test_list)
        rule = self.get_test_loinc_rule(test_value)

        actual_list = rule.split_value
        self.assertEqual(test_list, actual_list)

    def test_split_value_with_brackets_quotes_and_commas(self):
        """
        Given a value of a string of quoted and delimited elements in brackets with commas
        When split_list is called
        Then the delimited values are split into a list
        """

        test_list = [
            ",Alpha-1-Fetoprotein",
            "Alpha-1-Fetoprotein, Ab",
            "Alpha-1-Fetoprotein.tumor marker,",
        ]
        quoted_values = ",".join(f'"{item}"' for item in test_list)
        test_value = f"{{{quoted_values}}}"
        rule = self.get_test_loinc_rule(test_value)

        actual_list = rule.split_value
        self.assertEqual(test_list, actual_list)

    def test_split_value_with_endlines(self):
        """
        Given a value of a string of delimited elements with end-line characters
        When split_list is called
        Then the delimited values are split into a list
        """

        test_list = [
            "Alpha-1\n-Fetoprotein",
            "Alpha-1-Fetoprotein Ab\r",
            "\r\nAlpha-1-Fetoprotein.tumor marker",
        ]
        test_value = ",".join(test_list)
        rule = self.get_test_loinc_rule(test_value)

        actual_list = rule.split_value
        expected_value = [
            item.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
            for item in test_list
        ]
        self.assertEqual(expected_value, actual_list)


if __name__ == "__main__":
    unittest.main()
