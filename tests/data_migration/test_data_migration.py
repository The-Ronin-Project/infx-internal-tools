import unittest

from app.database import get_db
from app.app import create_app
from app.enum.concept_maps_for_systems import ConceptMapsForSystems
from app.enum.concept_maps_for_content import ConceptMapsForContent
from app.helpers.format_helper import filter_unsafe_depends_on_value, prepare_depends_on_value_for_storage, \
    DataExtensionUrl, prepare_code_and_display_for_storage_migration, \
    convert_source_concept_spark_export_string_to_json_string_normalized_ordered, \
    convert_source_concept_spark_export_string_to_json_string_unordered, \
    convert_source_concept_text_only_spark_export_string_to_json_string
from app.util.data_migration import get_v5_concept_map_uuids_in_n_blocks_for_parallel_process


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

    def test_concept_map_uuid_content(self):
        uuid_list = "(\n"
        for en in ConceptMapsForContent:
            uuid_list += f"'{en.value}',\n"
        uuid_list = uuid_list[:-2]
        uuid_list += "\n)"
        assert uuid_list == """(
'03659ed9-c591-4bbc-9bcf-37260e0e402f',
'06c4fc33-c94c-49bf-91e8-839c2f934e84',
'0ab9b876-132a-4bf8-9e6d-3de7707c23c4',
'0e1c7d9b-b760-4cb8-8b74-ee994e3cd35a',
'143f108b-4696-4aa2-a7d6-48d80941e199',
'19b3cf4b-2821-4fc0-9938-252bca569e9d',
'1c09013f-1481-4847-bbdf-d5e284105b42',
'1c586ea6-de0f-4080-803e-4108dce744da',
'1d97362b-4f78-4bf8-aa7d-36fd298c7771',
'1e2a3170-3c62-4228-8203-e53b3eb879c8',
'1e6f1c31-7e12-4cf7-903b-d571ae5b17cd',
'23cac82a-e73f-48a9-bc10-ff11964d2c00',
'249529ba-545e-4857-b68f-74d3f9cabe10',
'2b8d4526-9a66-468a-a60d-85883a15ab7c',
'2c353c65-e4d7-4932-b518-7bc42d98772d',
'2d2ae352-9534-4cc8-ada1-b5653e950ded',
'2d52e869-3897-480a-be8b-ce3d2d8e45be',
'302c4c8c-8445-475d-b490-39a0fc798b6b',
'305dd5b4-713d-4a0e-859a-bcad0ac1dee5',
'338f38d4-6edb-4fec-9feb-4ed512ff4596',
'343e7f9b-7fa8-430e-9107-d5bba0a365dc',
'3466798e-0522-4be1-8922-2b8a85dd279c',
'35c3428d-a499-4184-bae4-d3202dd7a76f',
'3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3',
'3d137a1a-5131-4110-8960-10ad1d28410f',
'3db8eb6e-fdc4-4db6-b867-e292f92a34a2',
'455789fe-8146-4596-b21c-14d5c4fa9fff',
'4e1dc626-f3eb-4660-840f-da93872bd514',
'4f225da9-cae9-45b8-8772-0b176c701588',
'4ffae118-778f-4df9-bd73-aece934b521b',
'520693a2-6774-4851-a43e-f08f53274237',
'55bd727f-7ba1-4a2a-9b3a-cba0c7b39486',
'57936e0c-7e47-480b-a6b1-a16dedc7d98c',
'5dfcbcec-deb1-43b1-b91c-7c9134b7fc0d',
'615434d9-7a4f-456d-affe-4c2d87845a37',
'64e33cd2-4b51-43e1-9d3f-27b7f9f679fa',
'65b3ea9e-2f5e-4008-90b0-0f67a3a1c7dd',
'67097264-0976-410d-af9b-3ba6d6633100',
'6b737eda-0c55-40d9-8393-808b46b9e80a',
'6df05955-4e0c-4a53-870a-66f5daa5b67b',
'7002458e-e4e6-4eef-9a17-8cec74d5befe',
'71cc7cd1-55fc-460f-92e1-6f70ea212aa1',
'724e5ab7-d561-4d2a-90fd-8ca56fd521e6',
'76c0e95e-5459-416d-8190-f9cb45d8814b',
'7b7541e7-3b1b-4864-a6b3-d992214b3b2b',
'7c65abbe-ab6f-4cc0-abe1-226f8f26c83b',
'7feee7c2-a303-425a-a9d3-d75973e3bd4d',
'81636f4c-cb12-44c0-921b-df2b102fe3df',
'8324a752-9c3e-4a98-8839-6e6a767bfb67',
'84dbea39-6b40-44e1-b79a-e0f790b65488',
'85d2335c-791e-4db5-a98d-c0c32949a65e',
'8a8a82d6-bd9e-4676-919b-e26637ace742',
'8b6f82c0-d39e-436c-81ce-eb9a3c70655e',
'8b99faba-2159-486b-84ce-af13ed6698c0',
'8f648ad7-1dfb-46e1-872f-598ece845624',
'918a6449-fa62-4abb-9919-5f88529911d9',
'96b5358e-3194-491f-b28b-c89ee9ff22bf',
'9827e7a8-be2f-4934-b895-386b9d5c2427',
'9c34d139-8cc2-474a-8844-2a0fd3ca282c',
'9e6055c1-7739-4042-b8e6-76161536a3b1',
'9f521e40-e41c-4f34-ac63-3779a00220d6',
'a16746af-d966-4c7c-a16d-7f58d3258708',
'a24e4273-6949-48b6-bc3f-719bc9750272',
'a2ce50a7-cfb9-497d-902e-fdb632743e77',
'a6eccd3d-cccb-47b8-8c05-cf3b67cd60d5',
'b1706cc9-30d1-4c03-8c6b-47701fa2bfc6',
'b644fbf3-3456-4eaa-8f98-88ebcfe25505',
'beeb96f8-47aa-4108-8fd9-d54af9c34ec2',
'c1108bbe-d6ed-4698-a111-cf2275407ab6',
'c504f599-6bf6-4865-8220-bb199e3d1809',
'c50e711b-aa73-4179-a386-8e161ef3c61c',
'c57e0f66-9e7f-45a5-a796-9b0715684ca2',
'ca7e8d9c-3627-4d2d-b4f6-d4c433d19f91',
'caeba74b-3f08-4545-b3f3-774efc93add7',
'cbb85d16-b976-4277-abba-4ba533ec81f9',
'ce7b980c-f0d3-4742-b526-4462045b4221',
'd1feb2f7-3591-4aa4-aab8-e2023f84f530',
'd259f29f-7576-4614-b440-1aa61937e8b9',
'd78bb852-875b-4dee-b1d8-be7b1e622967',
'd854b3f0-a161-4932-952b-5d53c9bcc560',
'e25086d6-a642-485f-8e3f-62d76ccfa343',
'e68cc741-7d9f-4c3f-b8c1-ef827f240134',
'e7734e09-da3b-45f6-a845-24583d6709fb',
'eae7f857-77d0-427b-bcd7-7db16404a737',
'ed01d5bd-176c-4910-9867-185f844f6965',
'ef731708-e333-4933-af74-6bf97cb4077e',
'f0fcd3eb-09b9-47a8-b338-32d35e3eee95',
'f39f59d8-0ebb-4e6a-a76a-d64b891eeadb',
'f4c9c05e-fbb8-4fb0-9775-a7fa7ae581d7',
'f5810c79-0287-489e-968c-6e5878b5a571',
'f64aa0b9-2457-43f7-8fc2-7a86dadce107',
'f9ce5fae-d05e-4ccd-a9f7-99cba4ba2d78',
'3a0ce96a-6a94-4304-a6a8-68132e30885b'
)"""

    def test_concept_map_systems(self):
        uuid_list = "(\n"
        for en in ConceptMapsForSystems:
            uuid_list += f"'{en.value}',\n"
        uuid_list = uuid_list[:-2]
        uuid_list += "\n)"
        assert uuid_list == """(
'1109aaac-b4da-4df2-8e74-587cec6d13cf',
'1229b87f-dfcf-4ba4-b998-372eec5ddcd6',
'2bb85526-7759-487d-8366-1d4b48508c7b',
'307799e4-ea26-464e-80ed-843558c4f1b9',
'30fef431-7ade-4891-a67e-0b7216629c45',
'3379cd57-783f-4db2-b2b0-a4797d976020',
'394f1f10-8027-4e69-9750-b2a3776aa58c',
'503e4d7f-f6b9-4923-9a53-7353f5e1193b',
'71cf28b4-b998-45bf-aba6-772be10e8c11',
'7a6e1a03-a36f-47d9-badd-645516b4c9fc',
'a6bec72f-7ee6-4ea5-9fb4-c632db602bc0',
'ae61ee9b-3f55-4d3c-96e7-8c7194b53767',
'c7d0f5d3-8e94-4985-8bac-9793c36605a2',
'c9644018-ba8c-41b6-92f1-15568bb679c4',
'f469524c-83fa-461c-976d-4e4a818713f8',
'e94578e1-1545-4955-8e62-7788b9a1cac0',
'27570634-1f4a-4c05-b8e3-c6e8fc226056'
)"""

    def test_get_concept_map_uuids_in_4_blocks_for_parallel_process_happy(self):
        original_count = len(ConceptMapsForContent) + len(ConceptMapsForSystems)
        number_of_blocks = 4
        test_list = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks)
        assert len(test_list) == number_of_blocks
        test_count = 0
        for x in test_list:
            test_count += len(x)
        assert test_count == original_count

    def test_get_concept_map_uuids_in_8_blocks_for_parallel_process_happy(self):
        original_count = len(ConceptMapsForContent) + len(ConceptMapsForSystems)
        number_of_blocks = 8
        test_list = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks)
        assert len(test_list) == number_of_blocks
        test_count = 0
        for x in test_list:
            test_count += len(x)
        assert test_count == original_count

    def test_get_concept_map_uuids_in_15_blocks_for_parallel_process_happy(self):
        original_count = len(ConceptMapsForContent) + len(ConceptMapsForSystems)
        number_of_blocks = 15
        test_list = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks)
        assert len(test_list) == number_of_blocks
        test_count = 0
        for x in test_list:
            test_count += len(x)
        assert test_count == original_count

    def test_get_concept_map_uuids_in_0_blocks_for_parallel_process_fail(self):
        number_of_blocks = 0
        test_list = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks)
        assert len(test_list) == 0

    def test_get_concept_map_uuids_in_64_blocks_for_parallel_process_fail(self):
        number_of_blocks = 64
        test_list = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks)
        assert len(test_list) == 0

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

    def test_prepare_code_and_display_for_storage_migration_bracketed_null(self):
        code = "[null]"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: value is '[null]' or '[null, null]' or '[null, null, null]'"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_quoted_null_is_simply_a_string(self):
        code = "null"
        display = "Potassium Level"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_fake_valueCodeableConcept(self):
        code = 'valueCodeableConcept:{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_prefix_component(self):
        code = '{"component":{"valueCodeableConcept":{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}}}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_prefix_component_list_single(self):
        code = '{"component":[{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}],"text":"Mucinous adenocarcinoma"}}]}'
        normalized_code = '{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}],"text":"Mucinous adenocarcinoma"}'
        code_text = "Mucinous adenocarcinoma"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_prefix_component_list_multi(self):
        code = '{"component":[{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}]},"text":"Mucinous adenocarcinoma"},{"valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.508"}]},"text":"Mucinous adenocarcinoma"}]}'
        key_sorted_code = '{"component":[{"text":"Mucinous adenocarcinoma","valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.507"}]}},{"text":"Mucinous adenocarcinoma","valueCodeableConcept":{"coding":[{"code":"8480/3","display":"Mucinous adenocarcinoma","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.838471.508"}]}}]}'
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_prefix_valueCodeableConcept(self):
        code = '{"valueCodeableConcept":{"coding":[{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72","userSelected":true},{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org","userSelected":false}],"text":"Potassium Level"}}'
        normalized_code = '{"coding":[{"code":"2823-3","display":"Potassium [Moles/volume] in Serum or Plasma","system":"http://loinc.org"},{"code":"21704910","display":"Potassium Level","system":"https://fhir.cerner.com/ec2458f2-1e24-41c8-b71b-0e701af7583d/codeSet/72"}],"text":"Potassium Level"}'
        code_text = "Potassium Level"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_uuid(self):
        code = "016eba87-a1cc-4e10-b0d5-fa7665f6714b"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "code"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == "016eba87-a1cc-4e10-b0d5-fa7665f6714b"
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_spark_with_failure(self):
        code = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct, 1.0}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis, 2.0}, {R19.8}], Positive Murphy's Sign}"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == "format issue: spark"
        assert result[1] == code
        assert result[2] is None
        assert result[3] == code
        assert result[4] == display

    def test_prepare_code_and_display_for_storage_migration_spark_with_display(self):
        code = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {R19.8, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-10-cm}], Positive Murphy's Sign}"
        goal_json_string = '{"coding":[{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        normalized_code = '{"coding":[{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        sql_escaped = '{"coding":[{"code":"787.99","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'\'s Sign"}'
        code_text = "Positive Murphy's Sign"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_spark_with_text_only_real_case(self):
        code = "{Line 1}"
        goal_json_string = '{"text":"Line 1"}'
        json_string = convert_source_concept_text_only_spark_export_string_to_json_string(code)
        assert json_string == goal_json_string
        code_text = "Line 1"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_spark_with_text_only_fake_case(self):
        code = "{anything}"
        goal_json_string = '{"text":"anything"}'
        json_string = convert_source_concept_text_only_spark_export_string_to_json_string(code)
        assert json_string == goal_json_string
        code_text = "anything"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_spark_with_null_text_and_empty_display(self):
        code = "{[{1, Present and non-brisk, urn:oid:1.2.840.114350.1.13.412.2.7.4.696784.55042}], null}"
        goal_json_string = '{"coding":[{"code":"1","display":"Present and non-brisk","system":"urn:oid:1.2.840.114350.1.13.412.2.7.4.696784.55042"}]}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(code, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] is None

    def test_prepare_code_and_display_for_storage_migration_spark_with_null_display_system_single_quote(self):
        code = "{[{G57.61, null, http://hl7.org/fhir/sid/icd-10-cm}, {292241000119108, null, http://snomed.info/sct}], Morton's neuroma of right foot}"
        goal_json_string = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":"292241000119108","system":"http://snomed.info/sct"}],"text":"Morton\'s neuroma of right foot"}'
        sql_escaped = '{"coding":[{"code":"G57.61","system":"http://hl7.org/fhir/sid/icd-10-cm"},{"code":"292241000119108","system":"http://snomed.info/sct"}],"text":"Morton\'\'s neuroma of right foot"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        code_text = "Morton's neuroma of right foot"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_spark_with_commas_in_display(self):
        code = "{[{723620004, Requires vaccination (finding), http://snomed.info/sct}, {V06.8, Need for diphtheria, tetanus, pertussis, and Hib vaccination, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {Z23, Need for diphtheria, tetanus, pertussis, and Hib vaccination, http://hl7.org/fhir/sid/icd-10-cm}], Need for diphtheria, tetanus, pertussis, and Hib vaccination}"
        goal_json_string = '{"coding":[{"code":"723620004","display":"Requires vaccination (finding)","system":"http://snomed.info/sct"},{"code":"V06.8","display":"Need for diphtheria, tetanus, pertussis, and Hib vaccination","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"Z23","display":"Need for diphtheria, tetanus, pertussis, and Hib vaccination","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Need for diphtheria, tetanus, pertussis, and Hib vaccination"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_json_string
        code_text = "Need for diphtheria, tetanus, pertussis, and Hib vaccination"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == goal_json_string
        assert result[3] == goal_json_string
        assert result[4] == code_text

    def test_prepare_code_and_display_for_storage_migration_spark_with_long_coding_list(self):
        code = "{[{A09AA02, http://www.whocc.no/atc}, {65328, urn:oid:2.16.840.1.113883.6.208}, {65329, urn:oid:2.16.840.1.113883.6.208}, {65330, urn:oid:2.16.840.1.113883.6.208}, {67625, urn:oid:2.16.840.1.113883.6.208}, {70893, urn:oid:2.16.840.1.113883.6.208}, {743, http://www.nlm.nih.gov/research/umls/rxnorm}, {6406, http://www.nlm.nih.gov/research/umls/rxnorm}, {8031, http://www.nlm.nih.gov/research/umls/rxnorm}, {48470, http://www.nlm.nih.gov/research/umls/rxnorm}, {204305, http://www.nlm.nih.gov/research/umls/rxnorm}, {204306, http://www.nlm.nih.gov/research/umls/rxnorm}, {217712, http://www.nlm.nih.gov/research/umls/rxnorm}, {217933, http://www.nlm.nih.gov/research/umls/rxnorm}, {218027, http://www.nlm.nih.gov/research/umls/rxnorm}, {219066, http://www.nlm.nih.gov/research/umls/rxnorm}, {219073, http://www.nlm.nih.gov/research/umls/rxnorm}, {219074, http://www.nlm.nih.gov/research/umls/rxnorm}, {219095, http://www.nlm.nih.gov/research/umls/rxnorm}, {219475, http://www.nlm.nih.gov/research/umls/rxnorm}, {220607, http://www.nlm.nih.gov/research/umls/rxnorm}, {220831, http://www.nlm.nih.gov/research/umls/rxnorm}, {221049, http://www.nlm.nih.gov/research/umls/rxnorm}, {228015, http://www.nlm.nih.gov/research/umls/rxnorm}, {352895, http://www.nlm.nih.gov/research/umls/rxnorm}, {392491, http://www.nlm.nih.gov/research/umls/rxnorm}, {541208, http://www.nlm.nih.gov/research/umls/rxnorm}, {546427, http://www.nlm.nih.gov/research/umls/rxnorm}, {546431, http://www.nlm.nih.gov/research/umls/rxnorm}, {546435, http://www.nlm.nih.gov/research/umls/rxnorm}, {546439, http://www.nlm.nih.gov/research/umls/rxnorm}, {546443, http://www.nlm.nih.gov/research/umls/rxnorm}, {797520, http://www.nlm.nih.gov/research/umls/rxnorm}, {1089860, http://www.nlm.nih.gov/research/umls/rxnorm}, {1117097, http://www.nlm.nih.gov/research/umls/rxnorm}, {1245754, http://www.nlm.nih.gov/research/umls/rxnorm}, {1245781, http://www.nlm.nih.gov/research/umls/rxnorm}, {1294123, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372686, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372702, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372724, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372736, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372743, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372760, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372764, http://www.nlm.nih.gov/research/umls/rxnorm}, {1372765, http://www.nlm.nih.gov/research/umls/rxnorm}], CREON ORAL}"
        goal_unordered = '{"coding":[{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(code)
        assert json_string == goal_unordered
        normalized_code = convert_source_concept_spark_export_string_to_json_string_normalized_ordered(code)
        goal_normalized = '{"coding":[{"code":"8031","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"352895","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219095","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204305","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"541208","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"797520","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372765","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"228015","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546443","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65330","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"A09AA02","system":"http://www.whocc.no/atc"},{"code":"217933","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"217712","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65329","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1372743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219066","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"65328","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"1245781","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220831","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"743","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"67625","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"6406","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"70893","system":"urn:oid:2.16.840.1.113883.6.208"},{"code":"546439","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"221049","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372736","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1089860","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372764","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"392491","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372724","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372760","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1117097","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546427","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372702","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"48470","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219475","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219074","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"219073","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"220607","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1372686","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1294123","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546431","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"204306","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"218027","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"546435","system":"http://www.nlm.nih.gov/research/umls/rxnorm"},{"code":"1245754","system":"http://www.nlm.nih.gov/research/umls/rxnorm"}],"text":"CREON ORAL"}'
        assert normalized_code == goal_normalized
        code_text = "CREON ORAL"
        display = "overwrite upon success"
        result = prepare_code_and_display_for_storage_migration(json_string, display)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == normalized_code
        assert result[3] == normalized_code
        assert result[4] == code_text

    def test_prepare_depends_on_value_for_storage_spark_null1(self):
        depends_on_value = "[null]"
        property = "Medication.ingredient.strength"
        result = prepare_depends_on_value_for_storage(depends_on_value, property)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_depends_on_value_for_storage_spark_null2(self):
        depends_on_value = "[null, null, null]"
        property = "Medication.ingredient.strength"
        result = prepare_depends_on_value_for_storage(depends_on_value, property)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_depends_on_value_for_storage_spark_rejected(self):
        depends_on_value = "[projectronin]"
        property = "Medication.ingredient.strength"
        result = prepare_depends_on_value_for_storage(depends_on_value, property)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is None

    def test_prepare_depends_on_value_for_storage_happy_spark(self):
        unordered_value = "{[{300352008, Murphy's sign positive (situation), http://snomed.info/sct}, {787.99, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-9-cm/diagnosis}, {R19.8, Positive Murphy's Sign, http://hl7.org/fhir/sid/icd-10-cm}], Positive Murphy's Sign}"
        property = "Observation.code"
        normalized_value = '{"coding":[{"code":"787.99","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'s Sign"}'
        sql_escaped = '{"coding":[{"code":"787.99","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-9-cm/diagnosis"},{"code":"300352008","display":"Murphy\'\'s sign positive (situation)","system":"http://snomed.info/sct"},{"code":"R19.8","display":"Positive Murphy\'\'s Sign","system":"http://hl7.org/fhir/sid/icd-10-cm"}],"text":"Positive Murphy\'\'s Sign"}'
        result = prepare_depends_on_value_for_storage(unordered_value, property)
        assert result[0] == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value
        assert result[1] is None
        assert result[2] == sql_escaped
        assert result[3] == normalized_value
        assert result[4] is property


if __name__ == '__main__':
    unittest.main()
