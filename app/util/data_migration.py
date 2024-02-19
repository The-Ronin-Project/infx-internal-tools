import datetime
import logging
import random
import time
import uuid
import traceback
import sys
import json
from math import floor

# from decouple import config
from sqlalchemy import text, MetaData, Table, Column, String, Row, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB, JSON

from app.database import get_db
from app.enum.concept_maps_for_content import ConceptMapsForContent
from app.enum.concept_maps_for_systems import ConceptMapsForSystems
from app.helpers.format_helper import IssuePrefix, \
    prepare_additional_data_for_storage, filter_unsafe_depends_on_value, prepare_code_and_display_for_storage_migration, \
    prepare_depends_on_value_for_storage, prepare_binding_and_value_for_jsonb_insert_migration, \
    prepare_depends_on_attributes_for_code_id_migration
from app.helpers.id_helper import generate_code_id, generate_mapping_id_with_source_code_values
from app.helpers.message_helper import message_exception_summary

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



def get_next_hex_char(hex_char):
    hex_int = int(hex_char, 16)
    hex_int = (hex_int + 1) % 16
    next_hex_char = hex(hex_int)[2:]
    return next_hex_char


def get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks_needed: int = 1) -> list:
    """
    Returns list of lists of UUIDs, split evenly into blocks according to requested number_of_blocks_needed.
    Included are app.enum.concept_maps_for_content.ConceptMapsForContent and ConceptMapsForSystems. Also see:

    Example of calling get_v5_concept_map_uuids_in_n_blocks_for_parallel_process() to control parallel processing:
    Main caller: if __name__=="__main__": in app.util.concept_map_v4_code_deduplication_hash.py
    Called function (calls this): app.util.concept_map_v4_code_deduplication_hash.identify_v4_concept_map_duplicates()

    Example of a function that accepts a concept map UUID and grabs just the "pending" and "active" versions to process:
    app.util.concept_map_v4_code_deduplication_hash.identify_v4_concept_map_duplicates_active_and_pending()

    For ConceptMapsForSystems we might need to migrate more versions in more statuses, to support existing unit tests.
    """
    if number_of_blocks_needed < 1 or number_of_blocks_needed > 32:
        return []
    full_list = []
    for concept_map_uuid in ConceptMapsForContent:
        full_list.append(concept_map_uuid.value)
    for concept_map_uuid in ConceptMapsForSystems:
        full_list.append(concept_map_uuid.value)
    sorted_list = sorted(full_list)
    list_length = len(sorted_list)
    block_length: int = floor(list_length / number_of_blocks_needed)

    # collect result_list
    result_list = []
    for n in range(0, number_of_blocks_needed):   # range is (>= start, < end)
        start = n * block_length
        if n == number_of_blocks_needed - 1:
            entry = sorted_list[start:]
            result_list.append(entry)
        else:
            end = ((n + 1) * block_length)
            entry = sorted_list[start:end]   # slice is (>= start, < end)
            result_list.append(entry)
    return result_list

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
        f"\nSTART: {time_start} - {uuid_start} to {uuid_end} - page size: {QUERY_LIMIT} - report every: {REPORT_LIMIT} rows\n")


def print_progress(time_start, total_processed, last_previous_uuid):

    time_end = datetime.datetime.now()
    time_elapsed = time_end - time_start
    if REPORT_LIMIT > 0:
        row_average_time = time_elapsed / REPORT_LIMIT
    else:
        row_average_time = 0
    LOGGER.warning(
        f"Accumulated rows: {total_processed}"
        f" - Time/1K: ({1000 * row_average_time})"
        f" - /100K: ({100000 * row_average_time})"
        f" - Last old_uuid: {last_previous_uuid}"
    )


def convert_empty_to_null(input_string: str):
    """
    conversion helper for data migration: when an empty column was represented as "" (empty string) return None
    """
    if input_string == "" or input_string == "null":
        return None
    else:
        return input_string


def convert_null_to_empty(input_string: str):
    """
    conversion helper for data migration: when a null column needs to be represented as "" (empty string) return ""
    """
    if input_string == None or input_string == "null":
        return ""
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
            deduplication_hash,
            terminology_version_uuid,
            created_date,
            additional_data, 
    """
    insert_depends_on_start = f"""
        INSERT INTO custom_terminologies.code_poc_depends_on
    """
    insert_depends_on_columns = f"""
        (
            depends_on_value_jsonb,
            depends_on_value_schema,  
            depends_on_value_simple, 
            depends_on_property, 
            depends_on_system, 
            depends_on_display,
            uuid,
            sequence,
            code_uuid,
            old_uuid
        )
        VALUES
        (
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
            :deduplication_hash,
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
            :uuid,
            :sequence,
            :code_uuid,
            :old_uuid
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
            deduplication_hash,
            terminology_version_uuid,
            created_date,
            additional_data, 
    """
    insert_issue_depends_on_start = f"""
        INSERT INTO custom_terminologies.code_poc_issue_depends_on
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
            :deduplication_hash,
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
            :uuid,
            :sequence,
            :code_uuid,
            :old_uuid
        )
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
            try:
                time_start = datetime.datetime.now()
                conn = get_db()

                # Processing by sequential UUID could miss new codes added by user work. Therefore to complete migration
                # users must stop using the database and we can run a final pass to get data for the late-arriving UUIDs
                query = f"""
                        select uuid, display, code, terminology_version_uuid,
                        created_date, additional_data, 
                        depends_on_property, depends_on_system, depends_on_value, depends_on_display
                        from custom_terminologies.code 
                        where uuid >= :last_previous_uuid and migrate = true
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

                        # prepare_code_and_display_for_storage
                        (
                            code_schema,
                            code_simple,
                            code_jsonb,
                            code_string_for_code_id,
                            display_string_for_code_id
                        ) = prepare_code_and_display_for_storage_migration(row.code, row.display)

                        # filter_unsafe_depends_on_value
                        (
                            depends_on_value,
                            rejected_depends_on_value
                        ) = filter_unsafe_depends_on_value(row.depends_on_value)

                        # has_depends_on
                        has_depends_on = (rejected_depends_on_value is None and depends_on_value is not None)
                        (
                            depends_on_value_schema,
                            depends_on_value_simple,
                            depends_on_value_jsonb,
                            depends_on_value_string
                        ) = prepare_depends_on_value_for_storage(depends_on_value)
                        depends_on_value_for_code_id = prepare_depends_on_attributes_for_code_id_migration(
                            convert_null_to_empty(depends_on_value_string),
                            convert_null_to_empty(row.depends_on_property),
                            convert_null_to_empty(row.depends_on_system),
                            convert_null_to_empty(row.depends_on_display)
                        )

                        # prepare_additional_data_for_storage
                        additional_data = prepare_additional_data_for_storage(
                            row.additional_data,
                            rejected_depends_on_value
                        )

                        # code_id (and deduplication_hash)
                        code_id = generate_code_id(
                            code_string=code_string_for_code_id,
                            display_string=display_string_for_code_id,
                            depends_on_value_string=depends_on_value_for_code_id,
                        )
                        deduplication_hash = code_id

                        # uuid
                        code_uuid = uuid.uuid4()

                        # jsonb columns get special handling
                        (
                            insert_code_jsonb,
                            insert_none_jsonb
                        ) = prepare_binding_and_value_for_jsonb_insert_migration(
                            insert_column_name="code_jsonb",
                            normalized_json_string=code_jsonb
                        )

                        # code_insert_query vs. code_insert_issue_query - send issues to the issue table
                        code_insert_query = (
                            insert_start
                            + insert_old_uuid
                            + insert_code_jsonb  # 'sql_escaped'::jsonb
                            + insert_code_id
                            + insert_end
                        )
                        code_insert_issue_query = (
                            insert_issue_start
                            + insert_issue_old_uuid
                            + insert_code_jsonb   # 'sql_escaped'::jsonb
                            + insert_issue_code_id
                            + insert_issue_end
                        )

                        # insert values
                        insert_values = {
                            "uuid": code_uuid,
                            "display": row.display,
                            "code_schema": code_schema,
                            "code_simple": code_simple,
                            "code_id": code_id,
                            "deduplication_hash": deduplication_hash,
                            "terminology_version_uuid": row.terminology_version_uuid,
                            "created_date": row.created_date,
                            "additional_data": additional_data,
                            "old_uuid": row.uuid,
                        }
                        if insert_none_jsonb is not None:
                            insert_values.update(insert_none_jsonb)

                        # issue_type
                        has_code_issue = False
                        if code_schema is not None and (
                            IssuePrefix.COLUMN_VALUE_FORMAT.value in code_schema
                        ):
                            has_code_issue = True
                            insert_values["code_schema"] = None
                            issue_type = code_schema + " (code)"
                        has_depends_on_issue = False
                        if depends_on_value_schema is not None and (
                            IssuePrefix.COLUMN_VALUE_FORMAT.value in depends_on_value_schema
                        ):
                            has_depends_on_issue = True
                            if has_code_issue:
                                issue_type += f", {depends_on_value_schema} (depends_on)"
                            else:
                                insert_values["code_schema"] = None
                                issue_type = depends_on_value_schema + " (depends_on)"
                        if has_code_issue is True or has_depends_on_issue is True:
                            query = code_insert_issue_query
                            insert_values.update({
                                "issue_type": issue_type
                            })
                        else:
                            query = code_insert_query

                        # run the query: either code_insert_query or code_insert_issue_query
                        try:
                            result = conn.execute(
                                text(query),
                                insert_values
                            )
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            error_summary = message_exception_summary(e)
                            if old_uuid_duplicate in error_summary or issue_old_uuid_duplicate in error_summary:
                                skip = True
                            elif code_duplicate in error_summary:
                                duplicate_code = True
                                has_code_issue = True
                                try:
                                    issue_type = insert_values.get("issue_type")
                                    if issue_type is not None:
                                        insert_values["issue_type"] = ", ".join([issue_type, code_duplicate])
                                    else:
                                        insert_values.update({
                                            "issue_type": f"{DUPLICATE_CODE_PREFIX}{code_id}",
                                        })
                                    result = conn.execute(
                                        text(code_insert_issue_query),
                                        insert_values
                                    )
                                    conn.commit()
                                except Exception as ex:
                                    conn.rollback()
                                    error_summary = message_exception_summary(e)
                                    if issue_old_uuid_duplicate in error_summary or code_duplicate in error_summary:
                                        skip = True
                                    else:
                                        raise ex
                            else:
                                raise e

                        # In real data, the only lists were unsafe: ALL migrated depends_on lists have exactly 1 member
                        # created by converting the single value to a 1-member list. That's why this block need not loop
                        if has_depends_on:

                            # uuid
                            depends_on_uuid = uuid.uuid4()

                            # jsonb
                            (
                                insert_depends_on_value_jsonb,
                                insert_none_jsonb
                            ) = prepare_binding_and_value_for_jsonb_insert_migration(
                                insert_column_name="depends_on_value_jsonb",
                                normalized_json_string=depends_on_value_jsonb
                            )

                            # depends_on_insert_query vs. depends_on_insert_issue_query - FK to normal vs. issues table
                            depends_on_insert_query = (
                                insert_depends_on_start
                                + insert_depends_on_columns
                                + insert_depends_on_value_jsonb  # 'sql_escaped'::jsonb
                                + insert_depends_on_binding
                            )
                            depends_on_insert_issue_query = (
                                insert_issue_depends_on_start
                                + insert_depends_on_columns
                                + insert_depends_on_value_jsonb  # 'sql_escaped'::jsonb
                                + insert_depends_on_binding
                            )

                            # insert values
                            insert_values = {
                                "depends_on_value_jsonb": depends_on_value_jsonb,
                                "depends_on_value_schema": depends_on_value_schema,
                                "depends_on_value_simple": depends_on_value_simple,
                                "depends_on_property": convert_empty_to_null(row.depends_on_property),
                                "depends_on_system": convert_empty_to_null(row.depends_on_system),
                                "depends_on_display": convert_empty_to_null(row.depends_on_display),
                                "uuid": depends_on_uuid,
                                "sequence": 1,
                                "code_uuid": code_uuid,
                                "old_uuid": row.uuid,
                            }
                            if insert_none_jsonb is not None:
                                insert_values.update(insert_none_jsonb)

                            # issue_type
                            if has_code_issue is True or has_depends_on_issue is True:
                                query = depends_on_insert_issue_query
                            else:
                                query = depends_on_insert_query

                            # run the query: either depends_on_insert_query or depends_on_insert_issue_query
                            try:
                                result = conn.execute(
                                    text(query),
                                    insert_values
                                )
                                conn.commit()
                            except Exception as e:
                                conn.rollback()
                                # no need to check for already inserted because the code_id assured that earlier
                                raise e

                        # end: if has_depends_on
                    # end: for row in result

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
                    print_progress(
                        time_start,
                        total_processed,
                        last_previous_uuid
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
    BATCH_SIZE = 25000
    conn = get_db()

    # Set up SQLAlchemy definitions
    metadata = MetaData()
    source_concept_data = Table(
        "source_concept_data",
        metadata,
        Column("uuid", UUID, nullable=False, primary_key=True),
        Column("code_schema", String, nullable=False),
        Column("code_simple", String, nullable=True),
        Column(
            "code_jsonb",
            JSONB(none_as_null=True),
            nullable=True,
        ),
        Column("display", String, nullable=False),
        Column("system_uuid", UUID, nullable=False),
        Column("comments", String, nullable=True),
        Column("map_status", String, nullable=False),
        Column("concept_map_version_uuid", UUID, nullable=False),
        Column("assigned_mapper", UUID, nullable=True),
        Column("assigned_reviewer", UUID, nullable=True),
        Column("no_map", Boolean, nullable=True),
        Column("reason_for_no_map", String, nullable=True),
        Column("mapping_group", String, nullable=True),
        Column(
            "previous_version_context",
            JSON(none_as_null=True),
            nullable=True,
        ),
        Column("custom_terminology_code_uuid", UUID, nullable=True),
        Column("save_for_discussion", Boolean, nullable=True),
        schema="concept_maps",
    )

    # Migrate data that has EITHER no custom_terminology_uuid OR the custom_terminology_uuid
    # is already migrated to custom_terminologies.code_data
    source_concept_migrated = False
    while source_concept_migrated is False:
        source_concept_results = conn.execute(
            text(
                """
                select sc.*
                from concept_maps.source_concept sc
                join concept_maps.concept_map_version cmv
                on cmv.uuid = sc.concept_map_version_uuid
                where cmv.migrate=true
                and (custom_terminology_uuid is null
                or custom_terminology_uuid in (select uuid from custom_terminologies.code_data))
                and sc.uuid not in (select uuid from concept_maps.source_concept_data)
                limit :batch_size
                """
            ), {
                "batch_size": BATCH_SIZE
            }
        ).fetchall()

        if source_concept_results:
            data_to_migrate = []
            for row in source_concept_results:
                (
                    code_schema,
                    code_simple,
                    code_jsonb,
                    _code_string_for_code_id,
                    _display_string_for_code_id
                ) = prepare_code_and_display_for_storage_migration(row.code, row.display)

                data_to_migrate.append({
                    "uuid": row.uuid,
                    "code_schema": code_schema,
                    "code_simple": code_simple,
                    "code_jsonb": code_jsonb,
                    "display": row.display,
                    "system_uuid": row.system, # Note name change
                    "comments": row.comments,
                    "map_status": row.map_status,
                    "concept_map_version_uuid": row.concept_map_version_uuid,
                    "assigned_mapper": row.assigned_mapper,
                    "assigned_reviewer": row.assigned_reviewer,
                    "no_map": row.no_map,
                    "reason_for_no_map": row.reason_for_no_map,
                    "mapping_group": row.mapping_group,
                    "previous_version_context": row.previous_version_context,
                    "custom_terminology_code_uuid": row.custom_terminology_uuid, # Note name change
                    "save_for_discussion": row.save_for_discussion
                })

            if data_to_migrate:
                conn.execute(
                    source_concept_data.insert(),
                    data_to_migrate
                )
                conn.commit()
            logging.warning(f"Migrated standard terminology data {BATCH_SIZE}")
        else:
            source_concept_migrated = True


USER_TO_UUID_MAP = {
    "Stephen Weaver": "69920e84-4cd6-42de-b522-f8cd239ce051",
    "Kurt Weber": "b357db91-cfb3-49d8-8db0-377f1ad321d5",
    "Katelin Brown": "91587bb3-a010-4358-af86-212acf356c84",
    "Hao Sun": "acfb20b1-361e-4642-bd48-090a8ad93e06",
    "Rey Johnson": "951d32b4-06a1-4b86-842e-57b8bef9bcd8",
    "Jonathan Perlin": "4f858880-cf4c-45a3-b546-4fd5f468ed8a",
    "Katie Ulvestad": "24d248f0-f0f1-4482-bf04-9cc5b1516b23",
    "Kristine Lynch": "f775072e-57c9-47ba-910f-73dcb36bcd54",
    "Theresa Aguilar": "915493ef-6d3e-4388-892e-662bedbde652",
    "Azita Zeyghami": "63be1a69-519c-4de1-bdee-8b6df1b72fe3",
    "Amanda Steffon": "c40efdc5-ea6f-4b28-8474-b72b7ace53d0",
    "Addison Stuart": "854a187d-dcf9-4aed-a247-47478ae7781e",
    "Elise Gatsby": "dd4c4e75-b046-446e-986c-2c8ed4607465",
    "Automap": "8990714d-8eeb-4acf-a5b7-abf92007a53a",
    "Automapping": "8990714d-8eeb-4acf-a5b7-abf92007a53a",
    "Rita Baroni": "f5562414-ec96-4939-98f7-debfb452afde",
    "Jon Barton": "ccaf4963-9613-4846-b53e-64b55a1dcd18",
    "Swapna Abhyankar": "88dddd13-bac3-44b6-ba14-20e5358ff6bc",
    "Susan Korgen": "6cb6ec1a-49b6-4dca-ad57-c45bc3176283",
    "NLP": "8abf5747-121b-48e0-9edf-dbd820d397fb",
    "Dan Angelis": "ba48e55b-cb9f-4943-b794-d50aa7b03368",
}


def user_to_uuid(user_name):
    if user_name is None:
        return None
    return USER_TO_UUID_MAP[user_name]


CODEABLE_CONCEPT_SCHEMA = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept"
CODE_SCHEMA = "code"


def migrate_concept_maps_concept_relationship(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "concept_maps.concept_relationshi"
    """
    BATCH_SIZE = 25000
    conn = get_db()

    # Set up SQLAlchemy definitions
    metadata = MetaData()
    concept_relationship_data = Table(
        "concept_relationship_data",
        metadata,
        Column("uuid", UUID, nullable=False, primary_key=True),
        Column("mapping_id", String, nullable=False),
        Column("deduplication_hash", String, nullable=False),
        Column("source_concept_uuid", UUID, nullable=False),
        Column("target_concept_code", String, nullable=False),
        Column("target_concept_display", String, nullable=False),
        Column("target_concept_terminology_version_uuid", UUID, nullable=False),
        Column("mapping_coments", String, nullable=True),
        Column("mapped_by", UUID, nullable=False),
        Column("mapped_date_time", DateTime, nullable=False),
        Column("relationship_code_uuid", UUID, nullable=False),
        Column("review_status", String, nullable=True),
        Column("reviewed_by", UUID, nullable=True),
        Column("reviewed_date_time", DateTime, nullable=True),
        Column("map_program_date_time", DateTime, nullable=True),
        Column("map_program_version", String, nullable=True),
        Column("map_program_prediction_id", String, nullable=True),
        Column("map_program_confidence_score", String, nullable=True),
        Column("deleted_by", UUID, nullable=True),
        Column("deleted_date_time", DateTime, nullable=True),
        schema="concept_maps",
    )

    # Migrate data without custom_terminology_uuid
    concept_relationship_migrated = False
    while concept_relationship_migrated is False:
        concept_relationship_results = conn.execute(
            text(
                """
                select cr.*, sc.no_map,
                sc.code_schema as sc_code_schema, sc.code_simple as sc_code_simple,
                sc.code_jsonb as sc_code_jsonb, sc.display as sc_display,
                cdo.depends_on_property, cdo.depends_on_system, cdo.depends_on_display,
                cdo.depends_on_value_schema, cdo.depends_on_value_simple, cdo.depends_on_value_jsonb
                from concept_maps.concept_relationship cr
                join concept_maps.source_concept_data sc
                on cr.source_concept_uuid = sc.uuid
                join concept_maps.concept_map_version cmv
                on cmv.uuid = sc.concept_map_version_uuid
                -- need to join all the way back to code_data so we can make the mapping_id
                left join custom_terminologies.code_data code_data
                on sc.custom_terminology_code_uuid=code_data.uuid
                -- need depends on (if available) for mapping_id
                left join custom_terminologies.code_depends_on cdo
                on code_data.uuid = cdo.code_uuid
                where cmv.migrate=true
                and cr.uuid not in (select uuid from concept_maps.concept_relationship_data)
                limit :batch_size
                """
            ), {
                "batch_size": BATCH_SIZE
            }
        ).fetchall()

        if concept_relationship_results:
            data_to_migrate = []
            for row in concept_relationship_results:

                # Calculate new mapping_id
                if row.sc_code_schema == CODEABLE_CONCEPT_SCHEMA:
                    source_code_string = row.sc_code_jsonb
                elif row.sc_code_schema == CODE_SCHEMA:
                    source_code_string = row.sc_code_simple
                else:
                    raise Exception("Unable to calculate source_code_string")

                depends_on_value = None
                if row.depends_on_value_schema is not None:
                    if row.depends_on_value_simple:
                        depends_on_value = row.depends_on_value_simple
                    else:
                        depends_on_value = json.dumps(row.depends_on_value_jsonb)
                    if not depends_on_value:
                        raise Exception("Unable to calculate depends on value")

                new_mapping_id = generate_mapping_id_with_source_code_values(
                    source_code_string=source_code_string,
                    display_string=row.sc_display,
                    relationship_code=str(row.relationship_code_uuid) if row.relationship_code_uuid else "",
                    target_concept_code=str(row.target_concept_display),
                    target_concept_display=str(row.target_concept_display),
                    target_concept_system=str(row.target_concept_system_version_uuid),
                    depends_on_value_string=depends_on_value,
                    depends_on_property=row.depends_on_property,
                    depends_on_system=row.depends_on_system,
                    depends_on_display=row.depends_on_display,
                )
                new_deduplication_hash = new_mapping_id

                # Concatenate mapping_comments and review_comment
                new_mapping_comments = row.mapping_comments
                if row.review_comment:
                    if new_mapping_comments is None:
                        new_mapping_comments = ""
                    new_mapping_comments += row.review_comment

                new_mapped_by = user_to_uuid(row.author)
                if new_mapped_by is None:
                    new_mapped_by = '70b5405d-b2ab-481b-85a5-d5b305164851'  # Unknown user to populate required column
                new_reviewed_by = user_to_uuid(row.reviewed_by)
                new_deleted_by = user_to_uuid(row.deleted_by)

                # Handle no maps
                new_target_concept_terminology_version_uuid = row.target_concept_system_version_uuid
                if new_target_concept_terminology_version_uuid is None:
                    if row.target_concept_code == 'No map' and row.target_concept_display == 'No matching concept':
                        new_target_concept_terminology_version_uuid = '93ec9286-17cf-4837-a4dc-218ce3015de6'

                # Previously only had time, not datetime
                # So we will migrate these rows with current datetime
                new_deleted_date_time = None
                if row.deleted_timestamp:
                    new_deleted_date_time = datetime.datetime.now()

                data_to_migrate.append({
                    "uuid": row.uuid,
                    "mapping_id": new_mapping_id,
                    "deduplication_hash": new_deduplication_hash,
                    "source_concept_uuid": row.source_concept_uuid,
                    "target_concept_code": row.target_concept_code,
                    "target_concept_display": row.target_concept_display,
                    "target_concept_terminology_version_uuid": new_target_concept_terminology_version_uuid,
                    "mapping_comments": new_mapping_comments,
                    "mapped_date_time": row.created_date,
                    "mapped_by": new_mapped_by,
                    "relationship_code_uuid": row.relationship_code_uuid,
                    "review_status": row.review_status,
                    "reviewed_by": new_reviewed_by,
                    "reviewed_date_time": row.reviewed_date,
                    "map_program_date_time": row.model_run_time,
                    "map_program_version": row.model_version,
                    "map_program_prediction_id": None,
                    "map_program_confidence_score": row.model_output_score,
                    "deleted_date_time": new_deleted_date_time,
                    "deleted_by": new_deleted_by
                })

            if data_to_migrate:
                conn.execute(
                    concept_relationship_data.insert(),
                    data_to_migrate
                )
                conn.commit()
            logging.warning(f"Migrated concept relationship data {BATCH_SIZE}")
        else:
            concept_relationship_migrated = True
    print("Complete")


def migrate_value_sets_expansion_member(
    granularity: int=1,
    segment_start: int=None,
    segment_count: int=None
):
    """
    migrate_database_table() helper function for when the original table_name is "value_sets.expansion_member"

    Parameters not implemented at this time; likely not needed due to speed of migration when run in OCI

    Strategy:
    - We'll first migrate rows without a custom_terminology_uuid (to avoid duplicate concerns)
    - Then we'll migrate rows with a custom_terminology_uuid AND have been migrated to code_data
    """
    BATCH_SIZE = 25000
    conn = get_db()

    # Set up SQLAlchemy definitions
    metadata = MetaData()
    expansion_member_data = Table(
        "expansion_member_data",
        metadata,
        Column("uuid", UUID, nullable=False, primary_key=True),
        Column("expansion_uuid", UUID, nullable=False),
        Column("code_schema", String, nullable=False),
        Column("code_simple", String, nullable=False),
        Column(
            "code_jsonb",
            JSONB(none_as_null=True),
            nullable=True,
        ),
        Column("display", String, nullable=False),
        Column("system", String, nullable=False),
        Column("version", String, nullable=False),
        Column("custom_terminology_uuid", UUID, nullable=True),
        Column("fhir_terminology_uuid", UUID, nullable=True),
        schema="value_sets",
    )

    # Migrate data without custom_terminology_uuid
    standard_terminology_migrated = False
    while standard_terminology_migrated is False:
        standard_terminology_data = conn.execute(
            text(
                """
                select vem.* from value_sets.expansion_member vem
                join value_sets.expansion vse
                on vse.uuid=vem.expansion_uuid
                join value_sets.value_set_version vsv
                on vsv.uuid=vse.vs_version_uuid
                where vem.custom_terminology_uuid is null
                and vem.uuid not in 
                (select uuid from value_sets.expansion_member_data)
                and vsv.migrate=true
                limit :batch_size
                """
            ), {
                "batch_size": BATCH_SIZE
            }
        ).fetchall()

        if standard_terminology_data:
            data_to_migrate = []
            for row in standard_terminology_data:
                (
                    code_schema,
                    code_simple,
                    code_jsonb,
                    _code_string_for_code_id,
                    _display_string_for_code_id
                ) = prepare_code_and_display_for_storage_migration(row.code, row.display)

                if code_schema is not None and row.system != "http://unitsofmeasure.org" and (
                        IssuePrefix.COLUMN_VALUE_FORMAT.value in code_schema
                ):
                    logging.warning(f"{code_schema}, {row.code}, {row.display}, {row.system}, {row.version}")

                # UCUM has codes that look like spark, but aren't
                if row.system == "http://unitsofmeasure.org":
                    code_schema = "code"
                    code_simple = row.code
                    code_jsonb = None

                if code_jsonb is not None:
                    code_jsonb = json.loads(code_jsonb)

                data_to_migrate.append({
                    "uuid": row.uuid,
                    "expansion_uuid": row.expansion_uuid,
                    "code_schema": code_schema,
                    "code_simple": code_simple,
                    "code_jsonb": code_jsonb,
                    "display": row.display,
                    "system": row.system,
                    "version": row.version,
                    "custom_terminology_uuid": row.custom_terminology_uuid,
                    "fhir_terminology_uuid": None,
                })

            if data_to_migrate:
                conn.execute(
                    expansion_member_data.insert(),
                    data_to_migrate
                )
                conn.commit()
            logging.warning(f"Migrated standard terminology data {BATCH_SIZE}")
        else:
            standard_terminology_migrated = True

    # Migrate data with a custom_terminology_uuid AND already migrated to code_data
    custom_terminology_verified_migrated = False
    while custom_terminology_verified_migrated is False:
        custom_terminology_verified = conn.execute(
            # Join used to select only data marked for migration
            # Other where clauses to select only data from custom terminologies
            # where we can verify it was migrated to code_data correctly
            text(
                """
                select vem.* from value_sets.expansion_member vem
                join value_sets.expansion vse
                on vse.uuid=vem.expansion_uuid
                join value_sets.value_set_version vsv
                on vsv.uuid=vse.vs_version_uuid
                where vem.custom_terminology_uuid is not null
                and vsv.migrate = true
                and vem.uuid not in 
                (select uuid from value_sets.expansion_member_data)
                and vem.custom_terminology_uuid in 
                (select uuid from custom_terminologies.code_data)
                limit :batch_size
                """
            ), {
                "batch_size": BATCH_SIZE
            }
        ).fetchall()

        if custom_terminology_verified:
            data_to_migrate = []
            for row in custom_terminology_verified:
                (
                    code_schema,
                    code_simple,
                    code_jsonb,
                    _code_string_for_code_id,
                    _display_string_for_code_id
                ) = prepare_code_and_display_for_storage_migration(row.code, row.display)

                if type(code_jsonb) == str:
                    try:
                        code_jsonb_dict = json.loads(code_jsonb)
                    except json.decoder.JSONDecodeError as e:
                        # This except entire section is to handle a single one-off error
                        # In a single code, deserializing the one field does not work
                        # But, in that instance, deserializing the other field does work
                        # Rey inspected the database and verified this row manually
                        print("You should only see this once in the migration")
                        print('row.uuid', row.uuid)
                        print('row.code', row.code)
                        print('row.display', row.display)
                        print('code_jsonb', code_jsonb)
                        code_jsonb_dict = json.loads(_code_string_for_code_id)
                        print('code_jsonb_dict', code_jsonb_dict)
                elif type(code_jsonb) == dict:
                    code_jsonb_dict = code_jsonb
                elif type(code_jsonb) == type(None):
                    code_jsonb_dict = None
                else:
                    raise Exception(f"code_jsonb must be str, dict, or None")

                # if code_schema is not None and (
                #         IssuePrefix.COLUMN_VALUE_FORMAT.value in code_schema
                # ):
                #     logging.warning(f"{code_schema}, {row.code}, {row.display}, {row.system}, {row.version}")

                data_to_migrate.append({
                    "uuid": row.uuid,
                    "expansion_uuid": row.expansion_uuid,
                    "code_schema": code_schema,
                    "code_simple": code_simple,
                    "code_jsonb": code_jsonb_dict,
                    "display": row.display,
                    "system": row.system,
                    "version": row.version,
                    "custom_terminology_uuid": row.custom_terminology_uuid,
                    "fhir_terminology_uuid": None,
                })
            if data_to_migrate:
                conn.execute(
                    expansion_member_data.insert(),
                    data_to_migrate
                )
                conn.commit()
            logging.warning(f"Migrated verified batch of {BATCH_SIZE}")
        else:
            custom_terminology_verified_migrated = True
    logging.warning("Complete")



# def perform_migration():
#     """
#     Command line endpoint for running migration locally. Does not support command line inputs at this time.
#     """
#     # todo: For this command line endpoint, add a line for each table to be migrated.
#     migrate_database_table("custom_terminologies.code", 1, 4, segment_count=1)


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
    # perform_migration()
    migrate_concept_maps_concept_relationship()
