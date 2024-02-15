import unittest

from app.database import get_db
from app.app import create_app
from app.helpers.format_helper import prepare_dynamic_value_for_storage, DataExtensionUrl, \
    convert_source_concept_spark_export_string_to_json_string_unordered, \
    convert_source_concept_text_only_spark_export_string_to_json_string, \
    convert_source_concept_spark_export_string_to_json_string_normalized_ordered, prepare_code_and_display_for_storage, \
    prepare_depends_on_value_for_storage, filter_unsafe_depends_on_value


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

    def test_prepare_code_and_display_for_storage_null_code(self):
        code = None
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: value is None"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "value is null"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_null_display_means_string(self):
        code = "Potassium Level"
        display = None
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "string"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_empty_code(self):
        code = ""
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: value is ''"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_empty_code_null_display_means_null(self):
        code = ""
        display = None
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_null_code_empty_display(self):
        code = None
        display = ""
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: value is None"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "value is null"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_empty_code_empty_display(self):
        code = ""
        display = ""
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: value is ''"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_bracketed_null(self):
        code = "[null]"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: value is '[null]' or '[null, null]' or '[null, null, null]'"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_simply_null(self):
        code = "null"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_text_line_1(self):
        code = '{"text":"Line 1"}'
        code_text = "Line 1"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == code
        assert result[3] == code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_text_only(self):
        code = '{"text": "INV-(2018-0382) AZD2811 IVPB in 250 mL"}'
        normalized_code = '{"text":"INV-(2018-0382) AZD2811 IVPB in 250 mL"}'
        code_text = "INV-(2018-0382) AZD2811 IVPB in 250 mL"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_fake_valueCodeableConcept(self):
        code = 'valueCodeableConcept:{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: invalid JSON for json.loads()"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_prefix_component(self):
        code = '{"component":{"valueCodeableConcept":{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}}}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_prefix_component_list_single(self):
        code = '{"component":[{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}],"text":"Mucinous adenocarcinoma"}}]}'
        normalized_code = '{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}],"text":"Mucinous adenocarcinoma"}'
        code_text = "Mucinous adenocarcinoma"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_prefix_component_list_multi(self):
        code = '{"component":[{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}]},"text":"Mucinous adenocarcinoma"},{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.508"}]},"text":"Mucinous adenocarcinoma"}]}'
        key_sorted_code = '{"component":[{"text":"Mucinous adenocarcinoma","valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}]}},{"text":"Mucinous adenocarcinoma","valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.508"}]}}]}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: invalid JSON for CodeableConcept"
        assert result[1] == key_sorted_code
        assert result[2] is None
        assert result[3] == key_sorted_code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_prefix_valueCodeableConcept(self):
        code = '{"valueCodeableConcept":{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_uuid(self):
        code = "016eba87-a1cc-4e10-b0d5-fa7665f6714b"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: uuid"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "016eba87-a1cc-4e10-b0d5-fa7665f6714b"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_good_json_double_quotes(self):
        code = '{"coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_good_json_double_quotes_and_apos(self):
        code = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":292241000119108,"system":"http://snomed.info/sct"}],"text":"Morton\'s neuroma of right foot"}'
        normalized_code = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'s neuroma of right foot"}'
        sql_escaped = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'\'s neuroma of right foot"}'
        code_text = "Morton\'s neuroma of right foot"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_good_json_code_with_version(self):
        code = '{"coding": [{"system": "http://projectronin.io/fhir/CodeSystem/mock/condition", "version": "1.0", "code": "test_concept_1", "display": "Test Concept 2023-06-27 13:51:27.236517"}], "text": "Test Concept 2023-06-27 13:51:27.236540"}'
        normalized_code = '{"coding":[{"code":"test_concept_1","display":"Test Concept 2023-06-27 13:51:27.236517","system":"http://projectronin.io/fhir/CodeSystem/mock/condition","version":"1.0"}],"text":"Test Concept 2023-06-27 13:51:27.236540"}'
        code_text = "Test Concept 2023-06-27 13:51:27.236540"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_good_json_code_with_long_coding_list_1(self):
        code = '{"coding": [{"system": "http://projectronin.io/fhir/CodeSystem/mock/condition", "version": "1.0", "code": "test_concept_1", "display": "Test Concept 2023-06-27 13:51:27.236517"}], "text": "Test Concept 2023-06-27 13:51:27.236540"}'
        normalized_code = '{"coding":[{"code":"test_concept_1","display":"Test Concept 2023-06-27 13:51:27.236517","system":"http://projectronin.io/fhir/CodeSystem/mock/condition","version":"1.0"}],"text":"Test Concept 2023-06-27 13:51:27.236540"}'
        code_text = "Test Concept 2023-06-27 13:51:27.236540"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_bad_json_single_quotes(self):
        code = "{'coding':[{'display':'Potassium Level','code':'21704910','system':'https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72','userSelected':true},{'code':'2823-3','system':'http://loinc.org','display':'Potassium [Moles/volume] in Serum or Plasma','userSelected':false}],'text':'Potassium Level'}"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: invalid JSON for json.loads()"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_bad_json_unexpected_key(self):
        code = '{"unexpectedKey":{"coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}}'
        key_sorted_code = '{"unexpectedKey":{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: invalid JSON for CodeableConcept"
        assert result[1] == key_sorted_code
        assert result[2] is None
        assert result[3] == key_sorted_code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_spark_with_failure(self):
        code = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct, 1.0}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis, 2.0}, {R19.8}], Positive Murphy's Sign}"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "format issue: spark"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_dynamic_value_for_storage_spark_with_display(self):
        code = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {R19.8, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-10-cm}], Positive Murphy's Sign}"
        goal_json_string = '{"coding":[{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        normalized_code = '{"coding":[{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        sql_escaped = '{"coding":[{"code":"787.99","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'\'s Sign"}'
        code_text = "Positive Murphy's Sign"
        display = "overwrite upon success"
        result = prepare_dynamic_value_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_dynamic_values_for_storage_spark_with_text_only_real_case(self):
        code = "{Line 1}"
        goal_json_string = '{"text":"Line 1"}'
        json_string = convert_source_concept_text_only_spark_export_string_to_json_string(code)
        assert json_string == goal_json_string
        code_text = "Line 1"
        display = "overwrite upon success"
        result = prepare_dynamic_value_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_dynamic_values_for_storage_spark_with_text_only_fake_case(self):
        code = "{anything}"
        goal_json_string = '{"text":"anything"}'
        json_string = convert_source_concept_text_only_spark_export_string_to_json_string(code)
        assert json_string == goal_json_string
        code_text = "anything"
        display = "overwrite upon success"
        result = prepare_dynamic_value_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_spark_with_null_text_and_empty_display(self):
        code = "{[{1, Present and non-brisk, urn:oid:1.2.840.114350.1.13.412.2.7.4.696784.55042}], null}"
        goal_json_string = '{"coding":[{"code":"1","display":"Present and non-brisk","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.696784.55042"}]}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] is None

    def test_prepare_dynamic_value_for_storage_spark_with_null_display_system_single_quote(self):
        code = "{[{G57.61, null, http://hl7.org/fhir/sid/icd-10-cm}, {292241000119108, null, http://snomed.info/sct}], Morton's neuroma of right foot}"
        goal_json_string = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":"292241000119108","system":"http://snomed.info/sct"}],"text":"Morton\'s neuroma of right foot"}'
        sql_escaped = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":"292241000119108","system":"http://snomed.info/sct"}],"text":"Morton\'\'s neuroma of right foot"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        code_text = "Morton's neuroma of right foot"
        display = "overwrite upon success"
        result = prepare_dynamic_value_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_dynamic_values_for_storage_spark_with_commas_in_display(self):
        code = "{[{723620004, Requires vaccination (finding), http://snomed.info/sct}, {V06.8, Need for diphtheria, tetanus, pertussis, and Hib vaccination, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {Z23, Need for diphtheria, tetanus, pertussis, and Hib vaccination, http://hl7.org/fhir/sid/icd-10-cm}], Need for diphtheria, tetanus, pertussis, and Hib vaccination}"
        goal_json_string = '{"coding":[{"code":"723620004","display":"Requires vaccination (finding)","system":"http://snomed.info/sct"},{"code":"V06.8","display":"Need for diphtheria, tetanus, pertussis, and Hib vaccination","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"Z23","display":"Need for diphtheria, tetanus, pertussis, and Hib vaccination","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Need for diphtheria, tetanus, pertussis, and Hib vaccination"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        code_text = "Need for diphtheria, tetanus, pertussis, and Hib vaccination"
        display = "overwrite upon success"
        result = prepare_dynamic_value_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_dynamic_values_for_storage_spark_with_long_coding_list(self):
        code = "{[{A09AA02, http://www.whocc.no/atc}, {65328, urn:oid:2.16.840.1.113883.6.208}, {65329, urn:oid:2.16.840.1.113883.6.208}, {65330, urn:oid:2.16.840.1.113883.6.208}, {67625, urn:oid:2.16.840.1.113883.6.208}, {70893, urn:oid:2.16.840.1.113883.6.208}, {743, http://www.nlm.nih.gov/research/umls/rxnorm}, {6406, http://www.nlm.nih.gov/research/umls/rxnorm}, {8031, http://www.nlm.nih.gov/research/umls/rxnorm}, {48470, http://www.nlm.nih.gov/research/umls/rxnorm}, {204305, http://www.nlm.nih.gov/research/umls/rxnorm}, {204306, http://www.nlm.nih.gov/research/umls/rxnorm}, {217712, http://www.nlm.nih.gov/research/umls/rxnorm}, {217933, http://www.nlm.nih.gov/research/umls/rxnorm}, {218027, http://www.nlm.nih.gov/research/umls/rxnorm}, {219066, http://www.nlm.nih.gov/research/umls/rxnorm}, {219073, http://www.nlm.nih.gov/research/umls/rxnorm}, {219074, http://www.nlm.nih.gov/research/umls/rxnorm}, {219095, http://www.nlm.nih.gov/research/umls/rxnorm}, {219475, http://www.nlm.nih.gov/research/umls/rxnorm}, {220607, http://www.nlm.nih.gov/research/umls/rxnorm}, {220831, http://www.nlm.nih.gov/research/umls/rxnorm}, {221049, http://www.nlm.nih.gov/research/umls/rxnorm}, {228015, http://www.nlm.nih.gov/research/umls/rxnorm}, {352895, http://www.nlm.nih.gov/research/umls/rxnorm}, {392491, http://www.nlm.nih.gov/research/umls/rxnorm}, {541208, http://www.nlm.nih.gov/research/umls/rxnorm}, {546427, http://www.nlm.nih.gov/research/umls/rxnorm}, {546431, http://www.nlm.nih.gov/research/umls/rxnorm}, {546435, http://www.nlm.nih.gov/research/umls/rxnorm}, {546439, http://www.nlm.nih.gov/research/umls/rxnorm}, {546443, http://www.nlm.nih.gov/research/umls/rxnorm}, {797520, http://www.nlm.nih.gov/research/umls/rxnorm}, {1089860, http://www.nlm.nih.gov/research/umls/rxnorm}, {1117097, http://www.nlm.nih.gov/research/umls/rxnorm}, {1245754, http://www.nlm.nih.gov/research/umls/rxnorm}, {1245781, http://www.nlm.nih.gov/research/umls/rxnorm}, {1294123, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372686, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372702, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372724, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372736, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372743, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372760, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372764, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372765, http://www.nlm.nih.gov/research/umls/rxnorm}], CREON ORAL}"
        goal_unordered = '{"coding":[{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_unordered
        normalized_code = convert_source_concept_spark_export_string_to_json_string_normalized_ordered(code)
        goal_normalized = '{"coding":[{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        assert normalized_code == goal_normalized
        code_text = "CREON ORAL"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    # (removed) tests for reversed code and display column check were here, but issue is not present in current data

    def test_prepare_code_and_display_for_storage_good_code_string(self):
        code = "85354-9"
        display = "Systolic Blood Pressure"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_depends_on_value_for_storage_empty_string(self):
        depends_on_value = ""
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None

    def test_prepare_depends_on_value_for_storage_null(self):
        depends_on_value = None
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None

    def test_prepare_depends_on_value_for_storage_spark_null1(self):
        depends_on_value = "[null]"
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None

    def test_prepare_depends_on_value_for_storage_spark_null2(self):
        depends_on_value = "[null, null, null]"
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None

    def test_prepare_depends_on_value_for_storage_spark_rejected(self):
        depends_on_value = "[projectronin]"
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None

    def test_prepare_depends_on_value_for_storage_happy_string(self):
        depends_on_value = "Today's whim"
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is "string"
        assert result[1] is "Today's whim"
        assert result[2] is None
        assert result[3] is "Today's whim"

    def test_prepare_depends_on_value_for_storage_happy_object(self):
        unordered_value = '{"coding":[{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        normalized_value = '{"coding":[{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        result = prepare_depends_on_value_for_storage(unordered_value)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_value
        assert result[3] == normalized_value

    def test_prepare_depends_on_value_for_storage_happy_spark(self):
        unordered_value = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {R19.8, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-10-cm}], Positive Murphy's Sign}"
        normalized_value = '{"coding":[{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        sql_escaped = '{"coding":[{"code":"787.99","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'\'s Sign"}'
        result = prepare_depends_on_value_for_storage(unordered_value)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_value

    def test_filter_unsafe_depends_on_value_rejected(self):
        depends_on_value = "[projectronin]"
        result = filter_unsafe_depends_on_value(depends_on_value)
        assert result[0] is None
        assert result[1] == depends_on_value

    def test_filter_unsafe_depends_on_value_accepted_object(self):
        depends_on_value = '{"coding":[{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        result = filter_unsafe_depends_on_value(depends_on_value)
        assert result[0] == depends_on_value
        assert result[1] is None

    def test_filter_unsafe_depends_on_value_accepted_spark(self):
        # No existing data has safe spark values, and we do not use spark anymore, so we do not accept ANY spark format
        depends_on_value = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {R19.8, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-10-cm}], Positive Murphy's Sign}"
        result = filter_unsafe_depends_on_value(depends_on_value)
        assert result[0] is None
        assert result[1] == depends_on_value


if __name__ == '__main__':
    unittest.main()
