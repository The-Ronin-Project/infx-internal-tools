import unittest

from app.database import get_db
from app.app import create_app
from app.helpers.format_helper import DataExtensionUrl, \
    prepare_code_and_display_for_storage, \
    prepare_depends_on_value_for_storage, normalized_data_dictionary_string, normalized_codeable_concept_string


class FormatHelperTests(unittest.TestCase):
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

    def test_prepare_code_and_display_for_storage_null_display_means_code(self):
        code = "Potassium Level"
        display = None
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_depends_on_value_for_storage_null_display_means_string(self):
        code = "Potassium Level"
        display = None
        result = prepare_depends_on_value_for_storage(code)
        assert result[0] == "string"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code

    def test_prepare_code_and_display_for_storage_all_digits(self):
        code = "12345678"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_depends_on_value_for_storage_all_digits(self):
        code = "12345678"
        result = prepare_depends_on_value_for_storage(code)
        assert result[0] == "string"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code

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

    def test_normalized_codeable_concept_string(self):
        object_value = {"coding":[{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}
        ordered_value = '{"coding":[{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        normalized = normalized_codeable_concept_string(object_value)
        assert normalized == ordered_value

    def test_normalized_data_dictionary_string(self):
        dictionary = {"c": "ccc", "a": "aaa", "b": "bbb"}
        serialized = normalized_data_dictionary_string(dictionary)
        assert serialized == '{"a":"aaa","b":"bbb","c":"ccc"}'


if __name__ == '__main__':
    unittest.main()
