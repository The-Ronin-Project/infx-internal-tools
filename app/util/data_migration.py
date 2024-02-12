import datetime
import logging
import random
import time
import uuid
import traceback
import sys

# from decouple import config
from sqlalchemy import text

from app.helpers.format_helper import prepare_dynamic_value_for_sql_issue, IssuePrefix, \
    prepare_additional_data_for_sql
from app.helpers.id_helper import hash_for_code_id
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
