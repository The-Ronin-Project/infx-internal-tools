import unittest

from _pytest.python_api import raises

from app.database import get_db
from app.app import create_app
from app.errors import BadDataError
from app.helpers.data_helper import hash_string, escape_sql_input_value, normalized_source_ratio, load_json_string, \
    cleanup_json_string, serialize_json_object, normalized_source_codeable_concept


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

    def test_normalized_source_ratio_no_change(self):
        input_value = '''{
                "numerator": {
                    "value": 7.5,
                    "unit": "mL",
                    "system": "http://unitsofmeasure.org",
                    "code": "mL"
                },
                "denominator": {
                    "value": 10,
                    "unit": "mL",
                    "system": "http://unitsofmeasure.org",
                    "code": "mL"
                }
            }'''
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_ratio(input_object)
        normalized_string = serialize_json_object(normalized_object)
        assert normalized_string == input_string

    def test_normalized_source_ratio_missing_some(self):
        input_value = '''{
                "numerator": {
                    "value": 7.5,
                    "unit": "mL"
                },
                "denominator": {
                    "value": 10,
                    "code": "mL"
                }
            }'''
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_ratio(input_object)
        normalized_string = serialize_json_object(normalized_object)
        assert normalized_string == input_string

    def test_normalized_source_ratio_extra_attributes(self):
        input_value = '''{
                "numerator": {
                    "value": 7.5,
                    "extension":"extra 1",
                    "unit": "mL",
                    "system": "http://unitsofmeasure.org",
                    "code": "mL"
                },
                "extension":"extra 2",
                "denominator": {
                    "value": 10,
                    "unit": "mL",
                    "system": "http://unitsofmeasure.org",
                    "code": "mL",
                    "extension":"extra 3"
                }
            }'''
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_ratio(input_object)
        normalized_string = serialize_json_object(normalized_object)
        normalized_expected = '{"denominator":{"code":"mL","system":"http://unitsofmeasure.org","unit":"mL","value":10},"numerator":{"code":"mL","system":"http://unitsofmeasure.org","unit":"mL","value":7.5}}'
        assert normalized_expected == normalized_string

    def test_normalized_source_ratio_missing_numerator(self):
        input_value = '''{
                 "denominator": {
                     "value": 10,
                     "unit": "mL",
                     "system": "http://unitsofmeasure.org",
                     "code": "mL"
                 }
             }'''
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        with raises(BadDataError) as e:
            normalized_source_ratio(input_object)
        result = e.value
        assert result.code == "Ratio.schema"
        assert result.description == "Ratio was expected, but one or more attributes is missing"
        assert result.errors == "Invalid Ratio"

    def test_normalized_source_ratio_missing_denominator(self):
        input_value = '''{
                "numerator": {
                    "value": 7.5,
                    "unit": "mL",
                    "system": "http://unitsofmeasure.org",
                    "code": "mL"
                }
            }'''
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        with raises(BadDataError) as e:
            normalized_source_ratio(input_object)
        result = e.value
        assert result.code == "Ratio.schema"
        assert result.description == "Ratio was expected, but one or more attributes is missing"
        assert result.errors == "Invalid Ratio"

    def test_normalized_source_ratio_missing_all(self):
        input_value = '{"extension":"extra"}'
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        with raises(BadDataError) as e:
            normalized_source_ratio(input_object)
        result = e.value
        assert result.code == "Ratio.schema"
        assert result.description == "Ratio was expected, but one or more attributes is missing"
        assert result.errors == "Invalid Ratio"

    def test_normalized_source_ratio_empty_object(self):
        input_object = {}
        with raises(BadDataError) as e:
            normalized_source_ratio(input_object)
        result = e.value
        assert result.code == "Ratio.schema"
        assert result.description == "Ratio was expected, but one or more attributes is missing"
        assert result.errors == "Invalid Ratio"

    def test_normalized_source_ratio_none_object(self):
        input_object = None
        normalized_object = normalized_source_ratio(input_object)
        assert normalized_object is None

    def test_normalized_source_codeable_concept_no_change(self):
        input_value = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'s neuroma of right foot"}'
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_codeable_concept(input_object)
        normalized_string = serialize_json_object(normalized_object)
        assert normalized_string == input_string

    def test_normalized_source_codeable_concept_removes_userSelected(self):
        code = '{"coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        input_string = cleanup_json_string(code)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_codeable_concept(input_object)
        normalized_string = serialize_json_object(normalized_object)
        assert normalized_string == normalized_code

    def test_normalized_source_codeable_concept_removes_id_and_userSelected(self):
        code = '{"id":"ronin-12345","coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        input_string = cleanup_json_string(code)
        input_object = load_json_string(input_string)
        normalized_object = normalized_source_codeable_concept(input_object)
        normalized_string = serialize_json_object(normalized_object)
        assert normalized_string == normalized_code

    def test_normalized_source_codeable_concept_missing_all(self):
        input_value = '{"extension":"extra"}'
        input_string = cleanup_json_string(input_value)
        input_object = load_json_string(input_string)
        with raises(BadDataError) as e:
            normalized_source_codeable_concept(input_object)
        result = e.value
        assert result.code == "CodeableConcept.schema"
        assert result.description == "CodeableConcept was expected, but the object has no coding or text attribute"
        assert result.errors == "Invalid CodeableConcept"

    def test_normalized_source_codeable_concept_empty_object(self):
        input_object = {}
        with raises(BadDataError) as e:
            normalized_source_codeable_concept(input_object)
        result = e.value
        assert result.code == "CodeableConcept.schema"
        assert result.description == "CodeableConcept was expected, but the object has no coding or text attribute"
        assert result.errors == "Invalid CodeableConcept"

    def test_normalized_source_codeable_concept_none_object(self):
        input_object = None
        normalized_object = normalized_source_codeable_concept(input_object)
        assert normalized_object is None

    def test_serialize_json_object_none_object(self):
        input_object = None
        serialized = serialize_json_object(input_object)
        assert serialized == ""

    def test_serialize_json_object_empty_object(self):
        input_object = {}
        serialized = serialize_json_object(input_object)
        assert serialized == "{}"