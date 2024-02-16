import unittest

from _pytest.python_api import raises

from app.database import get_db
from app.app import create_app
from app.helpers.data_helper import hash_string, escape_sql_input_value


class DataMigrationTests(unittest.TestCase):
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

    def test_hash_string(self):
        normalized_code = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'s neuroma of right foot"}'
        hashed_code = hash_string(normalized_code)
        assert hashed_code == "1f1d59b2559e305ca874a0a66726796a"

    def test_escape_sql_input_value_no_change(self):
        input_value = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Mortons neuroma of right foot"}'
        test_result = escape_sql_input_value(input_value)
        assert test_result == input_value

    def test_escape_sql_input_value_single_quote(self):
        input_value = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'s neuroma of right foot"}'
        sql_escaped = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'\'s neuroma of right foot"}'
        test_result = escape_sql_input_value(input_value)
        assert test_result == sql_escaped

    def test_escape_sql_input_value_colon_confused_with_sql_binding(self):
        input_value = '{"coding": [{"code": "271350002", "display": "Urine microscopy: leukocytes present (finding)", "system": "http://snomed.info/sct"}, {"code": "791.7", "display": "Urine micr.:leukocytes present", "system": "http://hl7.org/fhir/sid/icd-9-cm/diagnosis"}, {"code": "R82.81", "display": "Urine micr.:leukocytes present", "system": "urn:oid:2.16.840.1.113883.6.90"}], "text": "Urine micr.:leukocytes present"}'
        sql_escaped = '{"coding": [{"code": "271350002", "display": "Urine microscopy: leukocytes present (finding)", "system": "http://snomed.info/sct"}, {"code": "791.7", "display": "Urine micr.\:leukocytes present", "system": "http://hl7.org/fhir/sid/icd-9-cm/diagnosis"}, {"code": "R82.81", "display": "Urine micr.\:leukocytes present", "system": "urn:oid:2.16.840.1.113883.6.90"}], "text": "Urine micr.\:leukocytes present"}'
        test_result = escape_sql_input_value(input_value)
        assert test_result == sql_escaped
