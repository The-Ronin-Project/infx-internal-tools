# import asyncio
import asyncio
import datetime
import logging
import random
import time
import uuid
from enum import Enum
import traceback
import sys

# from decouple import config
from sqlalchemy import text

from app.helpers.data_helper import serialize_json_object
from app.helpers.format_helper import prepare_dynamic_value_for_sql_issue, prepare_data_dictionary_for_sql, IssuePrefix, \
    prepare_additional_data_for_sql
from app.helpers.id_helper import hash_for_code_id
from app.helpers.message_helper import message_exception_summary, message_exception_classname

# from sqlalchemy.dialects.postgresql import UUID as UUID_column_type
# from werkzeug.exceptions import BadRequest

LOGGER = logging.getLogger()
TOTAL_LIMIT = 1300000   # overall limit on number of records processed
QUERY_LIMIT = 500       # per-request limit on number of records to process - neither smaller nor larger values help
REPORT_LIMIT = 1000     # how often to log statistics while in progress
RETRY_LIMIT = 40        # number of times to retry database connection when it drops mid-loop on long runs, >30 helps
RETRY_SLEEP = 120       # time between retries - the most successful interval has consistently been 120, not more/less
FIRST_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")
LAST_UUID = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
DUPLICATE_CODE_PREFIX = "duplicate code_id: "

# Values for breaking into UUID groups
UUID_START = [
    uuid.UUID("00000000-0000-0000-0000-000000000000"),
    uuid.UUID("10000000-0000-0000-0000-000000000000"),
    uuid.UUID("20000000-0000-0000-0000-000000000000"),
    uuid.UUID("30000000-0000-0000-0000-000000000000"),
    uuid.UUID("40000000-0000-0000-0000-000000000000"),
    uuid.UUID("50000000-0000-0000-0000-000000000000"),
    uuid.UUID("60000000-0000-0000-0000-000000000000"),
    uuid.UUID("70000000-0000-0000-0000-000000000000"),
    uuid.UUID("80000000-0000-0000-0000-000000000000"),
    uuid.UUID("90000000-0000-0000-0000-000000000000"),
    uuid.UUID("a0000000-0000-0000-0000-000000000000"),
    uuid.UUID("b0000000-0000-0000-0000-000000000000"),
    uuid.UUID("c0000000-0000-0000-0000-000000000000"),
    uuid.UUID("d0000000-0000-0000-0000-000000000000"),
    uuid.UUID("e0000000-0000-0000-0000-000000000000"),
    uuid.UUID("f0000000-0000-0000-0000-000000000000")
]
# Top level group of 16
UUID_END = [
    uuid.UUID("0fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("1fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("2fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("3fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("4fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("5fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("6fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("7fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("8fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("9fffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("afffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("bfffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("cfffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("dfffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("efffffff-ffff-ffff-ffff-ffffffffffff"),
    uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
]
# other convenient values
START_UUID = UUID_START[0]
END_UUID = UUID_END[-1]

class ConceptMapsForContent(Enum):
    """
    UUID values for concept maps for use by INFX Content (and for publication to artifacts in OCI for use by Ronin).
    Use these to generate dynamic lists of concept map version UUIDs, value set UUIDs, flexible registry UUIDs, etc.
    """
    P1941_OBSERVATION_VALUE = "03659ed9-c591-4bbc-9bcf-37260e0e402f"
    p1941_PROCEDURE = "06c4fc33-c94c-49bf-91e8-839c2f934e84"
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


class ConceptMapsForSystems(Enum):
    """
    UUID values for concept maps for use by INFX Systems.
    Use these to generate dynamic lists of concept map version UUIDs, value set UUIDs, flexible registry UUIDs, etc.
    """
    TEST_ONLY_MIRTH_OBSERVATION = "1109aaac-b4da-4df2-8e74-587cec6d13cf"
    TEST_ONLY_AUG_29_NO_MAP = "1229b87f-dfcf-4ba4-b998-372eec5ddcd6"
    TEST_ONLY_NOV_2022_3_2 = "2bb85526-7759-487d-8366-1d4b48508c7b"
    TEST_ONLY_MISCELLANEOUS_1 = "307799e4-ea26-464e-80ed-843558c4f1b9"
    TEST_ONLY_VERSIONING = "30fef431-7ade-4891-a67e-0b7216629c45"
    TEST_ONLY_INFX_1383_RXNORM_1 = "3379cd57-783f-4db2-b2b0-a4797d976020"
    TEST_ONLY_NOV_2022_3_1 = "394f1f10-8027-4e69-9750-b2a3776aa58c"
    TEST_ONLY_INFX_2148_1 = "49a9d481-c4f8-464e-82de-21f43027b0e4"
    TEST_ONLY_INFX_1439 = "503e4d7f-f6b9-4923-9a53-7353f5e1193b"
    TEST_ONLY_INFX_2148_2 = "52d8c0f9-c9e7-4345-a31f-e9a6ae9f3913"
    TEST_ONLY_OBSERVATION_INCREMENTAL_LOAD = "684fe9e6-72b4-43db-b2f6-e66b81a997f7"
    TEST_ONLY_INFX_1376_DIABETES = "71cf28b4-b998-45bf-aba6-772be10e8c11"
    TEST_ONLY_NOV_2022_1 = "7a6e1a03-a36f-47d9-badd-645516b4c9fc"
    TEST_ONLY_INFX_1383_RXNORM_2 = "89a6b716-38e7-422f-8c92-c7a7243c6fbf"
    TEST_ONLY_INFX_2148_3 = "a6bec72f-7ee6-4ea5-9fb4-c632db602bc0"
    TEST_ONLY_CONDITION_INCREMENTAL_LOAD = "ae61ee9b-3f55-4d3c-96e7-8c7194b53767"
    TEST_ONLY_INFX_1376_CUSTOM_VS_FHIR_2 = "c7d0f5d3-8e94-4985-8bac-9793c36605a2"
    TEST_ONLY_INFX_1376_CUSTOM_VS_FHIR_1 = "c9644018-ba8c-41b6-92f1-15568bb679c4"
    TEST_ONLY_MISCELLANEOUS_3 = "e9229d03-526e-423f-ad57-c52f2ea4475e"
    TEST_ONLY_MISCELLANEOUS_2 = "f38902e7-bc7e-4890-a506-81f5b75c4cd7"
    TEST_ONLY_NOV_2022_3_3 = "f469524c-83fa-461c-976d-4e4a818713f8"


def query_code_uuid_latest_versions() -> str:
    """
    Gets test concept map codes for the typical cases, using only codes from the latest version of each terminology
    """
    return """
        WITH RankedVersions AS (
            SELECT 
                uuid,
                terminology,
                version,
                fhir_uri,
                effective_start,
                effective_end,
                ROW_NUMBER() OVER (
                    PARTITION BY terminology 
                    ORDER BY CAST(version AS float) DESC
                ) AS rn
            FROM 
                public.terminology_versions
            WHERE 
                is_standard = false 
                AND fhir_terminology = false
                AND version != 'N/A'
                AND version != '1.0.0'
        )
        SELECT 
            ctc.uuid
        FROM
            custom_terminologies.code ctc
        JOIN
            RankedVersions rv
            on rv.uuid = ctc.terminology_version_uuid
        WHERE 
            rv.rn = 1
    """


def query_code_uuid_string_versions() -> str:
    """
    Gets test concept map codes for the non-typical cases, using only codes from the latest version of each terminology
    """
    return """
        WITH StringVersions AS (
        SELECT 
            uuid,
            terminology,
            version,
            fhir_uri,
            effective_start,
            effective_end
        FROM 
            public.terminology_versions
        WHERE 
            is_standard = false 
            AND fhir_terminology = false
            AND (version = 'N/A' OR version = '1.0.0')
        )
        SELECT 
            ctc.uuid
        FROM
            custom_terminologies.code ctc
        JOIN
            StringVersions sv
            on sv.uuid = ctc.terminology_version_uuid
    """


def query_code_uuid_test_only() -> str:
    """
    Gets UUIDs for a small number of codes used in INFX Systems "Test ONLY" concept maps.
    Does not screen out older terminology versions as the other query_code_uuid_* functions do.
    """
    return """
        select distinct
        ctc.uuid
        from concept_maps.concept_relationship cr
        join concept_maps.source_concept sc on cr.source_concept_uuid = sc.uuid
        join concept_maps.concept_map_version cmv on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_map cm on cmv.concept_map_uuid = cm.uuid	
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        join public.terminology_versions ptv on ctc.terminology_version_uuid = ptv.uuid
        where cm.title like 'Test ONLY%' 
    """


def print_start(time_start, uuid_start, uuid_end):
    LOGGER.warning(
        f"\nSTART: {uuid_start} to {uuid_end} - page size: {QUERY_LIMIT} - report every: {REPORT_LIMIT} rows\n")


def print_progress(time_start, total_processed, last_previous_uuid, statistics=None):
    # Save log bloat for big runs by providing this line: you can comment it out when you want the extra log messages
    statistics = None

    time_end = datetime.datetime.now()
    time_elapsed = time_end - time_start
    if REPORT_LIMIT > 0:
        row_average_time = time_elapsed / REPORT_LIMIT
    else:
        row_average_time = 0
    if statistics is None:
        stat_report = ""
    else:
        summary = datetime.timedelta(0)
        for s in statistics:
            summary += s
        stat_report = (
            f"\n     Execution time breakdown:\n"
            f"       stat_select_batch:          {statistics[0]}\n"
            f"       stat_migrate_code_value:    {statistics[1]}\n"
            f"       stat_migrate_depends_on:    {statistics[2]}\n"
            f"       stat_migrate_advisory_data: {statistics[3]}\n"
            f"       stat_id_formation:          {statistics[4]}\n"
            f"       stat_insert_formation:      {statistics[5]}\n"
            f"       stat_values_formation:      {statistics[6]}\n"
            f"       stat_insert_success:        {statistics[7]}\n"
            f"       stat_insert_fail:           {statistics[8]}\n"
            f"       stat_fail_cause_dup_case_1: {statistics[9]}\n"
            f"       stat_fail_cause_dup_case_2: {statistics[10]}\n"
            f"     Total:                        {summary}"
        )
    LOGGER.warning(
        f"Accumulated rows: {total_processed}"
        f" - Time/1K: ({1000 * row_average_time})"
        f" - /100K: ({100000 * row_average_time})"
        f" - Last old_uuid: {last_previous_uuid}{stat_report}"
    )


def convert_empty_to_null(input_string: str):
    """
    conversion helper for data migration: when an empty column was represented as "" (empty string) return None
    """
    if input_string == "" or input_string == "null":
        return None
    else:
        return input_string


def migrate_database_table(
    table_name: str=None,
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    The Postman API endpoint runs this function.
    To enable parallel processing, this function operates on segments of rows, organized by hexadecimal UUID value.
    For normal activities, provide all inputs explicitly. Trigger random segment choice only for temporary tests in dev.
    @param table_name - full name of database table in clinical-content, for example "custom_terminologies.code".
    @param granularity - how much to break up segments internally, as shown.
        granularity 0 = 0x16 = 0 levels = do not segment at all = process ALL UUIDs from segment_start to 15 (inclusive)
        granularity 1 = 1 level per segment = 1x16 groups (16 segments) - process 1 segment, given by segment_start.
        for now, 1 is the only level of granularity greater than 0 that is supported; we can add more levels if needed
    @param segment_start - which segment of UUIDs to process; input 0-15 to indicate first character 1, 2, 3....d, e, f
        If granularity is 1, and segment_count is 1, and segment_start is None, process 1 segment chosen at random.
        In all other cases, when segment_start is None or <=0, process with segment_start = 0.
    @param segment_count - how many UUID segments to process; beginning at segment_start:
        If granularity is 1, and segment_count is 1, and segment_start is None, process 1 segment chosen at random.
        In all other cases, when segment_count is None or <=0, process ALL segments from segment_start to 15 (inclusive)
    """
    if table_name is None:
        return
    if table_name == "custom_terminologies.code":
        migrate_custom_terminologies_code(granularity, segment_start, segment_count)
    elif table_name == "concept_maps.source_concept":
        migrate_concept_maps_source_concept(granularity, segment_start, segment_count)
    elif table_name == "concept_maps.concept_relationship":
        migrate_concept_maps_concept_relationship(granularity, segment_start, segment_count)
    elif table_name == "value_sets.expansion_member":
        migrate_value_sets_expansion_member(granularity, segment_start, segment_count)
    else:
        return


def migrate_custom_terminologies_code(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "custom_terminologies.code"
    """
    from app.database import get_db

    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    # adjust inputs if unsupported values - note - range is (>= start, < end) - randint is (>= start, <= end)
    if segment_start is None or segment_start not in range(0, 16):
        if granularity > 0:
            segment_start = random.randint(0, 15)
        else:
            segment_start = 0
    uuid_start = UUID_START[segment_start]
    if granularity <= 0:
        uuid_end = UUID_END[-1]
    else:
        uuid_end = UUID_END[segment_start + segment_count - 1]

    full_time_start = datetime.datetime.now()
    total_processed = 0
    last_previous_uuid = uuid_start
    total_limit = TOTAL_LIMIT
    query_limit = QUERY_LIMIT
    retry_count = 0
    done = False
    insert_start = f"""
        INSERT INTO custom_terminologies.code_poc
        (
            uuid, 
            display,  
            code_schema,
            code_simple,
            code_jsonb,
            code_id,
            terminology_version_uuid,
            created_date,
            additional_data, 
    """
    insert_depends_on = f"""
            depends_on_value_jsonb,
            depends_on_value_schema,  
            depends_on_value_simple, 
            depends_on_property, 
            depends_on_system, 
            depends_on_display,
    """
    insert_old_uuid = f"""
            old_uuid
        )
        VALUES
        (
            :uuid, 
            :display,  
            :code_schema,
            :code_simple,
    """
    # :code_jsonb is added here at runtime
    insert_code_id = f"""
            :code_id,
            :terminology_version_uuid,
            :created_date,
            :additional_data,
    """
    # :depends_on_value_jsonb is added here at runtime
    insert_depends_on_binding = f""" 
            :depends_on_value_schema,
            :depends_on_value_simple,
            :depends_on_property, 
            :depends_on_system, 
            :depends_on_display,
    """
    insert_end = f"""
            :old_uuid
        )      
    """

    insert_issue_start = f"""
        INSERT INTO custom_terminologies.code_poc_issue
        (
            uuid, 
            display,  
            code_schema,
            code_simple,
            code_jsonb,
            code_id,
            terminology_version_uuid,
            created_date,
            additional_data, 
    """
    insert_issue_depends_on = f"""
            depends_on_value_jsonb,
            depends_on_value_schema,  
            depends_on_value_simple, 
            depends_on_property, 
            depends_on_system, 
            depends_on_display,
    """
    insert_issue_old_uuid = f"""
            old_uuid,
            issue_type
        )
        VALUES
        (
            :uuid, 
            :display,  
            :code_schema,
            :code_simple,
    """
    # :code_jsonb is added here at runtime
    insert_issue_code_id = f"""
            :code_id,
            :terminology_version_uuid,
            :created_date,
            :additional_data,
    """
    # :depends_on_value_jsonb is added here at runtime
    insert_depends_on_binding = f""" 
            :depends_on_value_schema,
            :depends_on_value_simple,
            :depends_on_property, 
            :depends_on_system, 
            :depends_on_display,
    """
    insert_issue_end = f"""
            :old_uuid,
            :issue_type
        )      
    """

    # main loop
    try:

        print_start(full_time_start, uuid_start, uuid_end)
        while not done:
            time_start: datetime = None
            time_select_batch: datetime = None
            time_migrate_code_value: datetime = None
            time_migrate_depends_on: datetime = None
            time_migrate_advisory_data: datetime = None
            time_id_formation: datetime = None
            time_insert_formation: datetime = None
            time_values_formation: datetime = None
            time_insert_success: datetime = None
            time_insert_fail: datetime = None
            time_fail_cause_dup_case_1: datetime = None
            time_fail_cause_dup_case_2: datetime = None
            delta_zero = datetime.timedelta(0)
            stat_select_batch = delta_zero
            stat_migrate_code_value = delta_zero
            stat_migrate_depends_on = delta_zero
            stat_migrate_advisory_data = delta_zero
            stat_id_formation = delta_zero
            stat_insert_formation = delta_zero
            stat_values_formation = delta_zero
            stat_insert_success = delta_zero
            stat_insert_fail = delta_zero
            stat_fail_cause_dup_case_1 = delta_zero
            stat_fail_cause_dup_case_2 = delta_zero
            try:
                time_start = datetime.datetime.now()
                conn = get_db()

                # processing by sequential UUID is non-optimal as it could miss new codes;
                # the right way is by created_date, and also to make users stop using the database during migration;
                # however, processing by sequential UUIDs rapidly discovers a rich gumbo of formatting cases
                query = f"""
                        select uuid, display, code, terminology_version_uuid,
                        created_date, additional_data, 
                        depends_on_property, depends_on_system, depends_on_value, depends_on_display
                        from custom_terminologies.code 
                        where uuid >= :last_previous_uuid 
                        and (
                        uuid in ({query_code_uuid_latest_versions()}) or
                        uuid in ({query_code_uuid_string_versions()}) or
                        uuid in ({query_code_uuid_test_only()})
                        )
                        and uuid not in (select old_uuid from custom_terminologies.code_poc) 
                        and uuid not in (select old_uuid from custom_terminologies.code_poc_issue)
                        order by uuid asc
                        limit :query_limit
                        """
                result = conn.execute(
                    text(query),
                    {
                        "last_previous_uuid": str(last_previous_uuid),
                        "query_limit": query_limit,
                    },
                ).fetchall()
                time_select_batch = datetime.datetime.now()
                stat_select_batch = time_select_batch - time_start

                # process the results from this batch
                count = len(result)
                if count == 0 or (
                    count == 1 and result[0].uuid == last_previous_uuid
                ):
                    done = True
                else:
                    for row in result:

                        # init
                        last_previous_uuid = row.uuid
                        if last_previous_uuid > uuid_end:
                            done = True
                            break  # for row in result
                        skip = False
                        duplicate_code = False
                        old_uuid_duplicate = "duplicate key value violates unique constraint \"old_uuid\""
                        issue_old_uuid_duplicate = "duplicate key value violates unique constraint \"issue_old_uuid\""
                        code_duplicate = f"duplicate key value violates unique constraint \"code_id\""
                        issue_type = "unknown"
                        total_processed += 1

                        # code - migrate 1 old code column to 4 new columns
                        (
                            code_schema,
                            code_simple,
                            code_jsonb,
                            code_string,
                            rejected
                        ) = prepare_dynamic_value_for_sql_issue(row.code, row.display)
                        time_migrate_code_value = datetime.datetime.now()
                        stat_migrate_code_value = time_migrate_code_value - time_select_batch

                        # depends_on_value - migrate 1 old depends_on_value column to 3 new columns
                        # also copy the other depends_on columns - convert all invalid "" values to None
                        (
                            depends_on_value_schema,
                            depends_on_value_simple,
                            depends_on_value_jsonb,
                            depends_on_value_string,
                            rejected_depends_on_value
                        ) = prepare_dynamic_value_for_sql_issue(row.depends_on_value)
                        has_depends_on = (rejected_depends_on_value is None and (depends_on_value_string is not None))
                        depends_on_property = convert_empty_to_null(row.depends_on_property)
                        depends_on_system = convert_empty_to_null(row.depends_on_system)
                        depends_on_display = convert_empty_to_null(row.depends_on_display)
                        time_migrate_depends_on = datetime.datetime.now()
                        stat_migrate_depends_on = time_migrate_depends_on - time_migrate_code_value

                        # additional data
                        info = prepare_additional_data_for_sql(row.additional_data, rejected_depends_on_value)
                        time_migrate_advisory_data = datetime.datetime.now()
                        stat_migrate_advisory_data = time_migrate_advisory_data - time_migrate_depends_on

                        # code_id
                        code_id = hash_for_code_id(
                            code_string,
                            row.display,
                            depends_on_value_string,
                            depends_on_property,
                            depends_on_system,
                            depends_on_display
                        )
                        time_id_formation = datetime.datetime.now()
                        stat_id_formation = time_id_formation - time_migrate_advisory_data

                        # uuid
                        new_uuid = uuid.uuid4()

                        # jsonb columns get special handling
                        if code_jsonb is None:
                            insert_code_jsonb = " :code_jsonb, "
                        else:
                            insert_code_jsonb = f" '{code_jsonb}'::jsonb, "
                        if has_depends_on:
                            if depends_on_value_jsonb is None:
                                insert_depends_on_value_jsonb = " :depends_on_value_jsonb, "
                            else:
                                insert_depends_on_value_jsonb = f" '{depends_on_value_jsonb}'::jsonb, "

                        # insert_query vs. insert_issue_query - send issue rows to the issue table - segment order:
                        #   insert_start
                        #   insert_depends_on (if has_depends_on)
                        #   insert_old_uuid
                        #   insert_code_jsonb
                        #   insert_code_id
                        #   insert_depends_on_value_jsonb (if has_depends_on)
                        #   insert_depends_on_binding (if has_depends_on)
                        #   insert_end
                        if has_depends_on:
                            insert_query = (
                                insert_start
                                + insert_depends_on
                                + insert_old_uuid
                                + insert_code_jsonb  # 'sql_escaped'::jsonb
                                + insert_code_id
                                + insert_depends_on_value_jsonb  # 'sql_escaped'::jsonb
                                + insert_depends_on_binding
                                + insert_end
                            )
                            insert_issue_query = (
                                insert_issue_start
                                + insert_issue_depends_on
                                + insert_issue_old_uuid
                                + insert_code_jsonb   # 'sql_escaped'::jsonb
                                + insert_issue_code_id
                                + insert_depends_on_value_jsonb   # 'sql_escaped'::jsonb
                                + insert_depends_on_binding
                                + insert_issue_end
                            )
                        else:
                            insert_query = (
                                insert_start
                                + insert_old_uuid
                                + insert_code_jsonb  # 'sql_escaped'::jsonb
                                + insert_code_id
                                + insert_end
                            )
                            insert_issue_query = (
                                insert_issue_start
                                + insert_issue_old_uuid
                                + insert_code_jsonb   # 'sql_escaped'::jsonb
                                + insert_issue_code_id
                                + insert_issue_end
                            )
                        time_insert_formation = datetime.datetime.now()
                        stat_insert_formation = time_insert_formation - time_id_formation

                        # insert values
                        insert_values = {
                            "uuid": new_uuid,
                            "display": row.display,
                            "code_schema": code_schema,
                            "code_simple": code_simple,
                            "code_id": code_id,
                            "terminology_version_uuid": row.terminology_version_uuid,
                            "created_date": row.created_date,
                            "additional_data": info,
                            "old_uuid": row.uuid,
                        }

                        # jsonb
                        if code_jsonb is None:
                            insert_values.update({"code_jsonb": code_jsonb})

                        # depends_on
                        if has_depends_on:
                            insert_values.update({"depends_on_value_jsonb": depends_on_value_jsonb})
                            insert_values.update({"depends_on_value_schema": depends_on_value_schema})
                            insert_values.update({"depends_on_value_simple": depends_on_value_simple})
                            insert_values.update({"depends_on_property": depends_on_property})
                            insert_values.update({"depends_on_system": depends_on_system})
                            insert_values.update({"depends_on_display": depends_on_display})

                        # issue_type
                        has_issue = False
                        code_issue = None
                        if code_schema is not None and (
                            IssuePrefix.COLUMN_VALUE_FORMAT.value in code_schema
                        ):
                            has_issue = True
                            insert_values["code_schema"] = None
                            code_issue = code_schema + " (code)"
                            issue_type = code_issue
                        if has_issue is True:
                            query = insert_issue_query
                            insert_values.update({
                                "issue_type": issue_type
                            })
                        else:
                            query = insert_query
                        time_values_formation = datetime.datetime.now()
                        stat_values_formation = time_values_formation - time_insert_formation

                        # query
                        try:
                            result = conn.execute(
                                text(query),
                                insert_values
                            )
                            conn.commit()
                            time_insert_success = datetime.datetime.now()
                            stat_insert_success = time_insert_success - time_values_formation

                        except Exception as e:
                            conn.rollback()
                            error_summary = message_exception_summary(e)
                            time_insert_fail = datetime.datetime.now()
                            stat_insert_fail = time_insert_fail - time_values_formation

                            if old_uuid_duplicate in error_summary or issue_old_uuid_duplicate in error_summary:
                                skip = True
                            elif code_duplicate in error_summary:
                                duplicate_code = True
                                try:
                                    issue_type = insert_values.get("issue_type")
                                    if issue_type is not None:
                                        insert_values["issue_type"] = ", ".join([issue_type, code_duplicate])
                                    else:
                                        insert_values.update({
                                            "issue_type": f"{DUPLICATE_CODE_PREFIX}{code_id}",
                                        })
                                    result = conn.execute(
                                        text(insert_issue_query),
                                        insert_values
                                    )
                                    conn.commit()
                                    time_fail_cause_dup_case_1 = datetime.datetime.now()
                                    stat_fail_cause_dup_case_1 = time_fail_cause_dup_case_1 - time_insert_fail

                                except Exception as ex:
                                    conn.rollback()
                                    error_summary = message_exception_summary(e)
                                    time_fail_cause_dup_case_2 = datetime.datetime.now()
                                    stat_fail_cause_dup_case_2 = time_fail_cause_dup_case_2 - time_insert_fail

                                    if issue_old_uuid_duplicate in error_summary or code_duplicate in error_summary:
                                        skip = True
                                    else:
                                        raise ex
                            else:
                                raise e
                    # end: for r in result

            except Exception as e:
                error_summary = message_exception_summary(e)
                call_ended = "EOF detected"
                connection_dropped = "connection to server"
                if (call_ended in error_summary or connection_dropped in error_summary) and retry_count < RETRY_LIMIT:
                    conn.close()
                    LOGGER.warning(f"RETRY - retry #{retry_count} of {RETRY_LIMIT}, at interval {RETRY_SLEEP} seconds")
                    retry_count += 1
                    time.sleep(RETRY_SLEEP)
                    done = False
                else:
                    done = True
                    raise e
            finally:
                if done or total_processed > 0 and (total_processed % REPORT_LIMIT == 0):
                    statistics = [
                        stat_select_batch,
                        stat_migrate_code_value,
                        stat_migrate_depends_on,
                        stat_migrate_advisory_data,
                        stat_id_formation,
                        stat_insert_formation,
                        stat_values_formation,
                        stat_insert_success,
                        stat_insert_fail,
                        stat_fail_cause_dup_case_1,
                        stat_fail_cause_dup_case_2,
                    ]
                    print_progress(
                        time_start,
                        total_processed,
                        last_previous_uuid,
                        statistics
                    )
                if not done:
                    done = total_processed >= total_limit
        # end: while not done

    except Exception as e:
        info = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(f"""\nERROR: {message_exception_summary(e)}\n\n{info}""")

    finally:
        print_progress(time_start, total_processed, last_previous_uuid)
        full_time_end = datetime.datetime.now()
        full_time_elapsed = full_time_end - full_time_start
        LOGGER.warning(
            f"Full processing time: {full_time_elapsed}"
            f" for granularity: {granularity}, segment_start: {segment_start}, segment_count: {segment_count}\n"
            f"DONE.\n"
        )


def migrate_concept_maps_source_concept(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "concept_maps.source_concept"
    """
    return  # stub


def migrate_concept_maps_concept_relationship(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "concept_maps.concept_relationshi"
    """
    return  # stub


def migrate_value_sets_expansion_member(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "value_sets.expansion_member"
    """
    return  # stub


def perform_migration():
    """
    Command line endpoint for running migration locally. Does not support command line inputs at this time.
    """
    # todo: For this command line endpoint, add a line for each table to be migrated.
    migrate_database_table("custom_terminologies.code", 1, 0, segment_count=1)


def cleanup_database_table(
    table_name: str=None,
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    Post-migration cleanup:

    Early versions of this function will populate "keep" or "discard" action values in _poc and _poc_issue without
    triggering subsequent action. Later versions will rely on existing, populated action values and act on them.

    Populating the action column:

    Most population of the action column will use queries that analyze _poc and _poc_issue rows for known problems with
    duplication. Of the 2x2x2 logic cases, 7 cases can be derived by query, and these will populate the action column.
    For the human review case, the results of spreadsheets marking "keep" values for rows of _poc and _poc_issue will be
    imported and read to populate the action column appropriately.

    code table keep/discard - re-use to clean up source_concept:

    custom_terminologies.code migration reduces volume by migrating (from code to code_poc and code_poc_issue)
    only those rows that are in use in certain concept maps and in the most recent version of each terminology.

    For concept_maps.source_concept we migrate and reduce volume by using its foreign keys to custom_terminologies.code
    and concept_maps.concept_map_version to only process rows corresponding to the custom_terminologies.code rows that
    we are migrating. The uuid from the custom_terminologies.code row matches only 1 old_uuid in either code_poc or
    code_poc_issue. That row's code_id value matches at least 1, possibly 1+ code_id values across code_poc and
    code_poc_issue. Only 1 of those rows has the value "keep" in the action column. concept_maps.source_concept rows
    with custom_terminology_uuid values that match ANY of the old_uuid values for this same code_id are migrated. Then:
    concept_maps.source_concept rows that match on "discard" rows are modified to point to the "keep" row instead.
    Any concept_maps.source_concept row that does not match as above can be discarded; this is a large number.

    Same as above for value_sets.expansion_member.

    Catching up with new table rows in the v4 table:

    Prior to final cut-over, any time we re-run migration to pick up recently-added values, we simply re-run our
    analysis queries for duplicates, and re-run our cleanup function for the table (this function). This works because
    re-running migration does not disrupt any existing _poc and _poc_issue rows or their action columns.
    It simply adds more rows, if found and as found.

    What the cleanup action does:

    Applies the action indicated by the action column value in the _poc and _poc_issue tables, such as "keep" or
    "discard". (There may be other values, hence the action column is not a boolean.) A common action is deduplication
    of rows by code_id or mapping_id. The single survivor row in _data may come from  either _poc or _poc_issue,
    according to the action column value in _poc or _poc_issue. For "keep", migrates the corresponding row into the
    _data table. For "discard", ensures the row is not in _data and if it is, deletes it from _data.

    _poc and _poc_issue rows are not modified, either by cleanup, or by migrating recent rows to _poc and _poc_issue.
    """
    return  # stub


if __name__=="__main__":
    perform_migration()
