import unittest

from app.database import get_db
from app.app import create_app
from app.helpers.data_helper import normalized_source_codeable_concept
from app.helpers.format_helper import DataExtensionUrl, \
    prepare_code_and_display_for_storage_migration, \
    prepare_depends_on_value_for_storage, normalized_data_dictionary_string, normalized_codeable_concept_string, \
    prepare_depends_on_attributes_for_code_id_migration, prepare_depends_on_attributes_for_code_id
from app.models.codes import DependsOnData


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

    def test_prepare_code_and_display_for_storage_migration_null_code(self):
        code = None
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: value is None"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "value is null"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_null_display_means_code(self):
        code = "Potassium Level"
        display = None
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_depends_on_value_for_storage_null_display_means_string(self):
        code = "Potassium Level"
        result = prepare_depends_on_value_for_storage(code)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_all_digits(self):
        code = "12345678"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_depends_on_value_for_storage_all_digits(self):
        code = "12345678"
        result = prepare_depends_on_value_for_storage(code)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_empty_code(self):
        code = ""
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: value is ''"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_empty_code_null_display_means_null(self):
        code = ""
        display = None
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_null_code_empty_display(self):
        code = None
        display = ""
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: value is None"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "value is null"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_empty_code_empty_display(self):
        code = ""
        display = ""
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: value is ''"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_text_line_1(self):
        code = '{"text":"Line 1"}'
        code_text = "Line 1"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == code
        assert result[3] == code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_text_only(self):
        code = '{"text": "INV-(2018-0382) AZD2811 IVPB in 250 mL"}'
        normalized_code = '{"text":"INV-(2018-0382) AZD2811 IVPB in 250 mL"}'
        code_text = "INV-(2018-0382) AZD2811 IVPB in 250 mL"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_good_json_double_quotes(self):
        code = '{"coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_good_json_double_quotes_and_apos(self):
        code = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":292241000119108,"system":"http://snomed.info/sct"}],"text":"Morton\'s neuroma of right foot"}'
        normalized_code = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'s neuroma of right foot"}'
        sql_escaped = '{"coding":[{"code":292241000119108,"system":"http://snomed.info/sct"},{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Morton\'\'s neuroma of right foot"}'
        code_text = "Morton\'s neuroma of right foot"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_good_json_code_with_version(self):
        code = '{"coding": [{"system": "http://projectronin.io/fhir/CodeSystem/mock/condition", "version": "1.0", "code": "test_concept_1", "display": "Test Concept 2023-06-27 13:51:27.236517"}], "text": "Test Concept 2023-06-27 13:51:27.236540"}'
        normalized_code = '{"coding":[{"code":"test_concept_1","display":"Test Concept 2023-06-27 13:51:27.236517","system":"http://projectronin.io/fhir/CodeSystem/mock/condition","version":"1.0"}],"text":"Test Concept 2023-06-27 13:51:27.236540"}'
        code_text = "Test Concept 2023-06-27 13:51:27.236540"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_good_json_code_with_long_coding_list_1(self):
        code = '{"coding": [{"system": "http://projectronin.io/fhir/CodeSystem/mock/condition", "version": "1.0", "code": "test_concept_1", "display": "Test Concept 2023-06-27 13:51:27.236517"}], "text": "Test Concept 2023-06-27 13:51:27.236540"}'
        normalized_code = '{"coding":[{"code":"test_concept_1","display":"Test Concept 2023-06-27 13:51:27.236517","system":"http://projectronin.io/fhir/CodeSystem/mock/condition","version":"1.0"}],"text":"Test Concept 2023-06-27 13:51:27.236540"}'
        code_text = "Test Concept 2023-06-27 13:51:27.236540"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_bad_json_single_quotes(self):
        code = "{'coding':[{'display':'Potassium Level','code':'21704910','system':'https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72','userSelected':true},{'code':'2823-3','system':'http://loinc.org','display':'Potassium [Moles/volume] in Serum or Plasma','userSelected':false}],'text':'Potassium Level'}"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_bad_json_unexpected_key(self):
        code = '{"unexpectedKey":{"coding":[{"display":"Potassium Level","code":"21704910","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","system":"http://loinc.org","display":"Potassium [Moles/volume] in Serum or Plasma","userSelected":false}],"text":"Potassium Level"}}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_good_code_string(self):
        code = "85354-9"
        display = "Systolic Blood Pressure"
        result = prepare_code_and_display_for_storage_migration(code, display)
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


    def test_prepare_depends_on_value_for_storage_happy_string_dropped(self):
        depends_on_value = "Today's whim"
        result = prepare_depends_on_value_for_storage(depends_on_value)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None


    def test_prepare_depends_on_value_for_storage_happy_string_converted(self):
        depends_on_value = "FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - PROGNOSTIC INDICATORS - KI-67 (%)"
        normalized_string = '{"text":"FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - PROGNOSTIC INDICATORS - KI-67 (%)"}'
        depends_on_property_text = "Observation.code.text"
        depends_on_property = "Observation.code"
        result = prepare_depends_on_value_for_storage(depends_on_value, depends_on_property_text)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_string
        assert result[3] == normalized_string
        assert result[4] == depends_on_property

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

    def test_prepare_codeable_concept_for_storage_version_is_preserved_if_present(self):
        unordered_value = '{"coding": [{"system": "http://projectronin.io/fhir/CodeSystem/mock/condition", "version": "1.0", "code": "test_concept_1", "display": "Test Concept 2023-06-27 13:51:27.236517"}], "text": "Test Concept 2023-06-27 13:51:27.236540"}'
        normalized_value = '{"coding":[{"code":"test_concept_1","display":"Test Concept 2023-06-27 13:51:27.236517","system":"http://projectronin.io/fhir/CodeSystem/mock/condition","version":"1.0"}],"text":"Test Concept 2023-06-27 13:51:27.236540"}'
        result = prepare_depends_on_value_for_storage(unordered_value)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_value
        assert result[3] == normalized_value

    def test_prepare_codeable_concept_for_storage_5_codes_in_coding_random_order(self):
        unordered_value = '{"text":"Temp", "coding":[{"code":"6","display":"Temp","system":"urn:oid:1.2.840.114350.1.13.412.2.7.2.707679"}, {"code":"8310-5","display":"Body temperature","system":"http://loinc.org"}, {"code":"8310-5","system":"urn:oid:1.2.246.537.6.96"}, {"code":"t.8c3xqZed921mVK294OU1Q0","display":"Temp","system":"http://open.epic.com/FHIR/STU3/StructureDefinition/observation-flowsheet-id"}, {"code":"8716-3","display":"Vital signs","system":"http://loinc.org"}]}'
        normalized_value = '{"coding":[{"code":"8310-5","system":"urn:oid:1.2.246.537.6.96"},{"code":"6","display":"Temp","system":"urn:oid:1.2.840.114350.1.13.412.2.7.2.707679"},{"code":"t.8c3xqZed921mVK294OU1Q0","display":"Temp","system":"http://open.epic.com/FHIR/STU3/StructureDefinition/observation-flowsheet-id"},{"code":"8716-3","display":"Vital signs","system":"http://loinc.org"},{"code":"8310-5","display":"Body temperature","system":"http://loinc.org"}],"text":"Temp"}'
        result = prepare_depends_on_value_for_storage(unordered_value)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_value
        assert result[3] == normalized_value

    def test_prepare_codeable_concept_for_storage_9_codes_in_coding_fully_reversed(self):
        unordered_value = '{"coding":[{"code":"656065","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.772305"},{"code":"2827","display":"Globulin","system":"urn:oid:1.2.840.114350.1.13.297.3.7.2.768282"},{"code":"10015","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.532"},{"code":"GLOB","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.1130"},{"code":"GLOBULIN","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.506"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.311"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.539"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.43"},{"code":"10834-0","display":"Globulin [Mass/volume] in Serum by calculation","system":"http://loinc.org"}],"text":"Globulin"}'
        normalized_value = '{"coding":[{"code":"10834-0","display":"Globulin [Mass/volume] in Serum by calculation","system":"http://loinc.org"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.43"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.539"},{"code":"2827","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.311"},{"code":"GLOBULIN","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.506"},{"code":"GLOB","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.1130"},{"code":"10015","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.532"},{"code":"2827","display":"Globulin","system":"urn:oid:1.2.840.114350.1.13.297.3.7.2.768282"},{"code":"656065","system":"urn:oid:1.2.840.114350.1.13.297.3.7.5.737384.772305"}],"text":"Globulin"}'
        result = prepare_depends_on_value_for_storage(unordered_value)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_value
        assert result[3] == normalized_value

    def test_depends_on_format_for_code_id(self):
        test_object_1 = DependsOnData(
            depends_on_value="a",
            depends_on_property="b",
            depends_on_system="c",
            depends_on_display="d"
        )
        test_object_2 = DependsOnData(
            depends_on_value="abcd"
        )
        result_1 = prepare_depends_on_attributes_for_code_id(test_object_1)
        result_2 = prepare_depends_on_attributes_for_code_id(test_object_2)
        self.assertEquals("abcd", result_1)
        self.assertEquals(result_2, result_1)

    def test_depends_on_format_for_code_id_migration(self):
        result_1 = prepare_depends_on_attributes_for_code_id_migration(
            depends_on_value_string="a",
            depends_on_property="b",
            depends_on_system="c",
            depends_on_display="d"
        )
        result_2 = prepare_depends_on_attributes_for_code_id_migration(
            depends_on_value_string="abcd"
        )
        self.assertEquals("abcd", result_1)
        self.assertEquals(result_2, result_1)


if __name__ == '__main__':
    unittest.main()
