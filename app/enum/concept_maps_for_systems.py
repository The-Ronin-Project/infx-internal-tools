from enum import Enum


class ConceptMapsForSystems(Enum):
    """
    UUID values for concept maps for use by INFX Systems.
    Use these to generate dynamic lists of concept map version UUIDs, value set UUIDs, flexible registry UUIDs, etc.

    There are 21 concept_maps.concept_map rows safe to use in tests.
    ```
    uuid                                   title                                  source_value_set_uuid
    "1109aaac-b4da-4df2-8e74-587cec6d13cf" "Test ONLY: Mirth Validation Obser"... "ca75b03c-1763-44fd-9bfa-4fe015ff809c"
    "1229b87f-dfcf-4ba4-b998-372eec5ddcd6" "Test ONLY: Test August 29 no map"...  "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc"
    "2bb85526-7759-487d-8366-1d4b48508c7b" "Test ONLY: Test 3 Nov 2022 2"         "0d4d231d-c686-459a-916e-93b064ef8c31"
    "307799e4-ea26-464e-80ed-843558c4f1b9" "Test ONLY: test"                      "1cdb27d0-73cc-11ec-a7b4-098fdcb4878b"
    "30fef431-7ade-4891-a67e-0b7216629c45" "Test ONLY: for debugging versioning"  "39e51788-b4e3-4a87-863a-9ec77c91572d"
    "3379cd57-783f-4db2-b2b0-a4797d976020" "Test ONLY: INFX-1383 Test RxNorm"...  "5beb5190-2613-43c3-8d79-e44a8073af5f"
    "394f1f10-8027-4e69-9750-b2a3776aa58c" "Test ONLY: Test 3 Nov 2022"           "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc"
    "503e4d7f-f6b9-4923-9a53-7353f5e1193b" "Test ONLY: Test INFX-1439"            "6723e4e0-a931-11ec-b504-a1b53549dd88"
    "71cf28b4-b998-45bf-aba6-772be10e8c11" "Test ONLY: INFX-1376  Diabetes"       "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc"
    "7a6e1a03-a36f-47d9-badd-645516b4c9fc" "Test ONLY: Test November 3 2022"      "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc"
    "a6bec72f-7ee6-4ea5-9fb4-c632db602bc0" "Test ONLY: infx2148testconceptmap"... "447f2d7b-e954-42da-b1f3-1ce7dd4d37d8"
    "ae61ee9b-3f55-4d3c-96e7-8c7194b53767" "Test ONLY: Test Condition Incremr"... "50ead103-a8c9-4aae-b5f0-f1e51b264323"
    "c7d0f5d3-8e94-4985-8bac-9793c36605a2" "Test ONLY: infx-1376 custom termi"... "1aeebc8e-bb48-4da7-b3ed-08674f47f490"
    "c9644018-ba8c-41b6-92f1-15568bb679c4" "Test ONLY: INFX-1376 FHIR Test Ma"... "1aeebc8e-bb48-4da7-b3ed-08674f47f490"
    "f469524c-83fa-461c-976d-4e4a818713f8" "Test ONLY: Test 3 Nov 2022 3"         "0d4d231d-c686-459a-916e-93b064ef8c31"
    ```

    ```
    uuid                                   target_value_set_uuid                  use_case_uuid
    "1109aaac-b4da-4df2-8e74-587cec6d13cf" "c86bf3d2-b6ba-4f05-83dd-f08c48dcca8a" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "1229b87f-dfcf-4ba4-b998-372eec5ddcd6" "c7c37780-e727-42f6-9d1b-d823d75171ad" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "2bb85526-7759-487d-8366-1d4b48508c7b" "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc" None
    "307799e4-ea26-464e-80ed-843558c4f1b9" "3db398a0-95a7-11ec-afa6-73d090577892" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "30fef431-7ade-4891-a67e-0b7216629c45" "b55fb14d-5d4e-4b1d-a068-fc1702e70dd2" "1d0e8532-b55d-4466-9471-6a6bf60eb313"
    "3379cd57-783f-4db2-b2b0-a4797d976020" "2410767b-a606-464b-97d3-ca92177945f2" "4f535e28-b8c4-41c8-b8e4-94020a90dd8f"
    "394f1f10-8027-4e69-9750-b2a3776aa58c" "0d4d231d-c686-459a-916e-93b064ef8c31" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "503e4d7f-f6b9-4923-9a53-7353f5e1193b" "25a804f0-a932-11ec-b504-a1b53549dd88" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "71cf28b4-b998-45bf-aba6-772be10e8c11" "0d4d231d-c686-459a-916e-93b064ef8c31" "b06745a1-76de-4974-b795-2cd6413d7d46"
    "7a6e1a03-a36f-47d9-badd-645516b4c9fc" "0d4d231d-c686-459a-916e-93b064ef8c31" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "a6bec72f-7ee6-4ea5-9fb4-c632db602bc0" "e49af176-189f-4536-8231-e58a261ed36d" None
    "ae61ee9b-3f55-4d3c-96e7-8c7194b53767" "8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    "c7d0f5d3-8e94-4985-8bac-9793c36605a2" "d1bd136b-4972-460f-a479-ae8206ee33d6" "b06745a1-76de-4974-b795-2cd6413d7d46"
    "c9644018-ba8c-41b6-92f1-15568bb679c4" "d1bd136b-4972-460f-a479-ae8206ee33d6" "b06745a1-76de-4974-b795-2cd6413d7d46"
    "f469524c-83fa-461c-976d-4e4a818713f8" "ddcbf55b-312d-4dd9-965d-f72c4bc51ddc" "bf161bfc-05f2-4b02-bd05-6a51bb884065"
    ```
    """
    TEST_ONLY_MIRTH_OBSERVATION = "1109aaac-b4da-4df2-8e74-587cec6d13cf"
    TEST_ONLY_AUG_29_NO_MAP = "1229b87f-dfcf-4ba4-b998-372eec5ddcd6"
    TEST_ONLY_NOV_2022_3_2 = "2bb85526-7759-487d-8366-1d4b48508c7b"
    TEST_ONLY_MISCELLANEOUS_1 = "307799e4-ea26-464e-80ed-843558c4f1b9"
    TEST_ONLY_VERSIONING = "30fef431-7ade-4891-a67e-0b7216629c45"
    TEST_ONLY_INFX_1383_RXNORM_1 = "3379cd57-783f-4db2-b2b0-a4797d976020"
    TEST_ONLY_NOV_2022_3_1 = "394f1f10-8027-4e69-9750-b2a3776aa58c"
    # TEST_ONLY_INFX_2148_1 = "49a9d481-c4f8-464e-82de-21f43027b0e4"
    TEST_ONLY_INFX_1439 = "503e4d7f-f6b9-4923-9a53-7353f5e1193b"
    # TEST_ONLY_INFX_2148_2 = "52d8c0f9-c9e7-4345-a31f-e9a6ae9f3913"
    TEST_ONLY_INFX_1376_DIABETES = "71cf28b4-b998-45bf-aba6-772be10e8c11"
    TEST_ONLY_NOV_2022_1 = "7a6e1a03-a36f-47d9-badd-645516b4c9fc"
    # TEST_ONLY_INFX_1383_RXNORM_2 = "89a6b716-38e7-422f-8c92-c7a7243c6fbf"
    TEST_ONLY_INFX_2148_3 = "a6bec72f-7ee6-4ea5-9fb4-c632db602bc0"
    TEST_ONLY_CONDITION_INCREMENTAL_LOAD = "ae61ee9b-3f55-4d3c-96e7-8c7194b53767"
    TEST_ONLY_INFX_1376_CUSTOM_VS_FHIR_2 = "c7d0f5d3-8e94-4985-8bac-9793c36605a2"
    TEST_ONLY_INFX_1376_CUSTOM_VS_FHIR_1 = "c9644018-ba8c-41b6-92f1-15568bb679c4"
    # TEST_ONLY_MISCELLANEOUS_3 = "e9229d03-526e-423f-ad57-c52f2ea4475e"
    # TEST_ONLY_MISCELLANEOUS_2 = "f38902e7-bc7e-4890-a506-81f5b75c4cd7"
    TEST_ONLY_NOV_2022_3_3 = "f469524c-83fa-461c-976d-4e4a818713f8"
    TEST_OBSERVATIONS_CODEABLE_CONCEPTS = "e94578e1-1545-4955-8e62-7788b9a1cac0"
    TEST_CONDITIONS_CODEABLE_CONCEPTS = "27570634-1f4a-4c05-b8e3-c6e8fc226056"
