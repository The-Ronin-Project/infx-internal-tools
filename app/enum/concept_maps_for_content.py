from enum import Enum


class ConceptMapsForContent(Enum):
    """
    UUID values for concept maps for use by INFX Content (and for publication to artifacts in OCI for use by Ronin).
    Use these to generate dynamic lists of concept map version UUIDs, value set UUIDs, flexible registry UUIDs, etc.

    Change History:
    - List validated by Content team 2023Q4 for Concept Map schema update v4 to v5 - verified no changes since v4 list
    - List validated by Content team 2023Q3 for Concept Map schema update v3 to v4 - verified the v4 list
    """
    P1941_OBSERVATION_VALUE = "03659ed9-c591-4bbc-9bcf-37260e0e402f"
    P1941_PROCEDURE = "06c4fc33-c94c-49bf-91e8-839c2f934e84"
    CERNCODE_PROCEDURE = "0ab9b876-132a-4bf8-9e6d-3de7707c23c4"
    RONINCERNER_MEDICATION = "0e1c7d9b-b760-4cb8-8b74-ee994e3cd35a"
    APPOSND_CONTACT_POINT_SYSTEM = "143f108b-4696-4aa2-a7d6-48d80941e199"
    RONINCERNER_CARE_PLAN_CATEGORY = "19b3cf4b-2821-4fc0-9938-252bca569e9d"
    P1941_CONTACT_POINT_USE = "1c09013f-1481-4847-bbdf-d5e284105b42"
    PSJ_CANCER_TYPE_TO_CHOKUTO = "1c586ea6-de0f-4080-803e-4108dce744da"
    RONINEPIC_CONDITION_TO_CERNER = "1d97362b-4f78-4bf8-aa7d-36fd298c7771"
    APPOSND_CONTACT_POINT_USE = "1e2a3170-3c62-4228-8203-e53b3eb879c8"
    PSJTST_CARE_PLAN_CATEGORY = "1e6f1c31-7e12-4cf7-903b-d571ae5b17cd"
    RONINCERNER_OBSERVATION_VALUE = "23cac82a-e73f-48a9-bc10-ff11964d2c00"
    RONINCERNER_PROCEDURE = "249529ba-545e-4857-b68f-74d3f9cabe10"
    RONINEPIC_APPOINTMENT_STATUS = "2b8d4526-9a66-468a-a60d-85883a15ab7c"
    PSJ_DOCUMENT_REFERENCE_TYPE = "2c353c65-e4d7-4932-b518-7bc42d98772d"
    P1941_CONTACT_POINT_SYSTEM = "2d2ae352-9534-4cc8-ada1-b5653e950ded"
    MDATST_APPOINTMENT_STATUS = "2d52e869-3897-480a-be8b-ce3d2d8e45be"
    CERNCODE_OBSERVATION_VALUE = "302c4c8c-8445-475d-b490-39a0fc798b6b"
    RONINCERNER_OBSERVATION = "305dd5b4-713d-4a0e-859a-bcad0ac1dee5"
    CERNCODE_CARE_PLAN_CATEGORY = "338f38d4-6edb-4fec-9feb-4ed512ff4596"
    MDA_OBSERVATION_VALUE = "343e7f9b-7fa8-430e-9107-d5bba0a365dc"
    PSJTST_MEDICATION = "3466798e-0522-4be1-8922-2b8a85dd279c"
    CERNCODE_CONDITION = "35c3428d-a499-4184-bae4-d3202dd7a76f"
    APPOSND_CONDITION = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"
    P1941_CARE_PLAN_CATEGORY = "3d137a1a-5131-4110-8960-10ad1d28410f"
    PSJ_MEDICATION = "3db8eb6e-fdc4-4db6-b867-e292f92a34a2"
    MDATST_OBSERVATION = "455789fe-8146-4596-b21c-14d5c4fa9fff"
    APPOSND_PROCEDURE = "4e1dc626-f3eb-4660-840f-da93872bd514"
    RONINEPIC_MEDICATION = "4f225da9-cae9-45b8-8772-0b176c701588"
    PSJ_APPOINTMENT_STATUS = "4ffae118-778f-4df9-bd73-aece934b521b"
    CERNCODE_CONTACT_POINT_USE = "520693a2-6774-4851-a43e-f08f53274237"
    MDATST_DOCUMENT_REFERENCE_TYPE = "55bd727f-7ba1-4a2a-9b3a-cba0c7b39486"
    MDATST_CONDITION = "57936e0c-7e47-480b-a6b1-a16dedc7d98c"
    APPOSND_CARE_PLAN_CATEGORY = "5dfcbcec-deb1-43b1-b91c-7c9134b7fc0d"
    MDA_OBSERVATION = "615434d9-7a4f-456d-affe-4c2d87845a37"
    MDA_APPOINTMENT_STATUS = "64e33cd2-4b51-43e1-9d3f-27b7f9f679fa"
    MDA_CARE_PLAN_CATEGORY = "65b3ea9e-2f5e-4008-90b0-0f67a3a1c7dd"
    SURVEY_ASSIGNMENT_TO_CHOKUTO = "67097264-0976-410d-af9b-3ba6d6633100"
    MDATST_CONTACT_POINT_SYSTEM = "6b737eda-0c55-40d9-8393-808b46b9e80a"
    RONINEPIC_CARE_PLAN_CATEGORY = "6df05955-4e0c-4a53-870a-66f5daa5b67b"
    APPOSND_MEDICATION = "7002458e-e4e6-4eef-9a17-8cec74d5befe"
    PSJTST_OBSERVATION = "71cc7cd1-55fc-460f-92e1-6f70ea212aa1"
    RONINEPIC_CONTACT_POINT_USE = "724e5ab7-d561-4d2a-90fd-8ca56fd521e6"
    RONINEPIC_OBSERVATION = "76c0e95e-5459-416d-8190-f9cb45d8814b"
    APPOSND_OBSERVATION_VALUE = "7b7541e7-3b1b-4864-a6b3-d992214b3b2b"
    PSJTST_CONTACT_POINT_SYSTEM = "7c65abbe-ab6f-4cc0-abe1-226f8f26c83b"
    RONINCERNER_CONTACT_POINT_SYSTEM = "7feee7c2-a303-425a-a9d3-d75973e3bd4d"
    RONINCERNER_DOCUMENT_REFERENCE_TYPE = "81636f4c-cb12-44c0-921b-df2b102fe3df"
    RONINCERNER_CONDITION = "8324a752-9c3e-4a98-8839-6e6a767bfb67"
    PSJ_CONTACT_POINT_USE = "84dbea39-6b40-44e1-b79a-e0f790b65488"
    APPOSND_OBSERVATION = "85d2335c-791e-4db5-a98d-c0c32949a65e"
    MDATST_CONTACT_POINT_USE = "8a8a82d6-bd9e-4676-919b-e26637ace742"
    RONINCERNER_CONTACT_POINT_USE = "8b6f82c0-d39e-436c-81ce-eb9a3c70655e"
    PSJ_CONTACT_POINT_SYSTEM = "8b99faba-2159-486b-84ce-af13ed6698c0"
    PSJ_CONDITION = "8f648ad7-1dfb-46e1-872f-598ece845624"
    PSJ_OBSERVATION = "918a6449-fa62-4abb-9919-5f88529911d9"
    PSJTST_APPOINTMENT_STATUS = "96b5358e-3194-491f-b28b-c89ee9ff22bf"
    APPOSND_DOCUMENT_REFERENCE_TYPE = "9827e7a8-be2f-4934-b895-386b9d5c2427"
    RONINEPIC_OBSERVATION_VALUE = "9c34d139-8cc2-474a-8844-2a0fd3ca282c"
    CERNCODE_APPOINTMENT_STATUS = "9e6055c1-7739-4042-b8e6-76161536a3b1"
    RONINCERNER_APPOINTMENT_STATUS = "9f521e40-e41c-4f34-ac63-3779a00220d6"
    MDA_CONTACT_POINT_USE = "a16746af-d966-4c7c-a16d-7f58d3258708"
    MDATST_MEDICATION = "a24e4273-6949-48b6-bc3f-719bc9750272"
    MDA_PROCEDURE = "a2ce50a7-cfb9-497d-902e-fdb632743e77"
    RONINEPIC_PROCEDURE = "a6eccd3d-cccb-47b8-8c05-cf3b67cd60d5"
    PSJTST_OBSERVATION_VALUE = "b1706cc9-30d1-4c03-8c6b-47701fa2bfc6"
    PSJ_PROCEDURE = "b644fbf3-3456-4eaa-8f98-88ebcfe25505"
    P1941_OBSERVATION = "beeb96f8-47aa-4108-8fd9-d54af9c34ec2"
    MDATST_OBSERVATION_VALUE = "c1108bbe-d6ed-4698-a111-cf2275407ab6"
    MDA_CONDITION = "c504f599-6bf6-4865-8220-bb199e3d1809"
    PSJTST_CONTACT_POINT_USE = "c50e711b-aa73-4179-a386-8e161ef3c61c"
    PSJTST_PROCEDURE = "c57e0f66-9e7f-45a5-a796-9b0715684ca2"
    PSJ_CARE_PLAN_CATEGORY = "ca7e8d9c-3627-4d2d-b4f6-d4c433d19f91"
    CERNCODE_DOCUMENT_REFERENCE_TYPE = "caeba74b-3f08-4545-b3f3-774efc93add7"
    P1941_MEDICATION = "cbb85d16-b976-4277-abba-4ba533ec81f9"
    PSJ_OBSERVATION_VALUE = "ce7b980c-f0d3-4742-b526-4462045b4221"
    RONINEPIC_CONTACT_POINT_SYSTEM = "d1feb2f7-3591-4aa4-aab8-e2023f84f530"
    P1941E_DOCUMENT_REFERENCE_TYPE = "d259f29f-7576-4614-b440-1aa61937e8b9"
    MDA_MEDICATION = "d78bb852-875b-4dee-b1d8-be7b1e622967"
    P1941_CONDITION = "d854b3f0-a161-4932-952b-5d53c9bcc560"
    MDATSTC_ARE_PLAN_CATEGORY = "e25086d6-a642-485f-8e3f-62d76ccfa343"
    APPOSND_APPOINTMENT_STATUS = "e68cc741-7d9f-4c3f-b8c1-ef827f240134"
    MDA_DOCUMENT_REFERENCE_TYPE = "e7734e09-da3b-45f6-a845-24583d6709fb"
    MDA_CONTACT_POINT_SYSTEM = "eae7f857-77d0-427b-bcd7-7db16404a737"
    CENCODE_MEDICATION = "ed01d5bd-176c-4910-9867-185f844f6965"
    CERNCODE_OBSERVATION = "ef731708-e333-4933-af74-6bf97cb4077e"
    PSJTST_CONDITION = "f0fcd3eb-09b9-47a8-b338-32d35e3eee95"
    CERNCODE_CONTACT_POINT_SYSTEM = "f39f59d8-0ebb-4e6a-a76a-d64b891eeadb"
    PSJTST_DOCUMENT_REFERENCE_TYPE = "f4c9c05e-fbb8-4fb0-9775-a7fa7ae581d7"
    P1941_APPOINTMENT_STATUS = "f5810c79-0287-489e-968c-6e5878b5a571"
    RONINEPIC_DOCUMENT_REFERENCE_TYPE = "f64aa0b9-2457-43f7-8fc2-7a86dadce107"
    MDATST_PROCEDURE = "f9ce5fae-d05e-4ccd-a9f7-99cba4ba2d78"
    NCCN_CANCER_TYPE_TO_CHOKUTO = "3a0ce96a-6a94-4304-a6a8-68132e30885b"
