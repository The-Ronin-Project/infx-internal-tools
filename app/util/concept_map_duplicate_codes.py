import datetime
import json
import sys
import traceback
from unittest import skip
import logging

import pytest
import uuid
from sqlalchemy import text

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.database import get_db
from app.helpers.format_helper import prepare_dynamic_value_for_sql_issue
from app.helpers.message_helper import message_exception_summary

LOGGER = logging.getLogger()


def load_condition_duplicates():
    """
    It's not clear we will need variants of this function in future. Recommend keeping it in the repo while we have v4.
    Creates an output table custom_terminologies.test_code_condition_duplicates with values you can use to
    de-duplicate entries in the concept_map.concept_relationship table by uuid, as follows:

    0. The goal count displays at top of this LOGGER output. After the output table has all those rows in it, you can
    1. do a select * on the output table in pgAdmin and be sure to sort by normalized_code_value (one of the columns)
    2. download from pgAdmin as CSV so you can see ALL the rows, not just the 1000 you can see in the pgAdmin viewer
    3. look for adjacent identical values above/below each other in sorted "normalized_code_value" column
    4. find corresponding "concept_map_concept_relationship_uuid" values for those adjacent identical values
    5. for context for decisions, other values are in the row including original code and concept_relationship target
    6. for each unique "normalized_code_value" value, choose only 1 "concept_map_concept_relationship_uuid" to keep
    7. Note: sometimes the original code value is spark format but normalized_code_value column looks like JSON:
             Interops cannot see Spark entries as duplicates so those should be ignored for this purpose.
    8. With items 6 and 7 both in mind, choose 1 uuid in concept_map.concept_relationship to keep. Discard the other.

    Note: Before you can use this function, you need to edit it to
    set the destination table name, concept map UUID and concept map version UUID in count_query and select_query.
    It would improve the function if these were arguments, so that manual editing was not necessary.
    However, it's not clear we will need variants of this function. Recommend keeping it in the repo while we have v4.
    """
    # If DB loses connection mid-way, query the table for the highest UUID captured so far and set first_uuid to that.
    first_uuid = uuid.UUID("00000000-0000-0000-0000-000000000000")

    # initialize
    page_size = 500
    report_page_size = 500
    start_uuid = first_uuid
    last_previous_uuid = start_uuid
    done = False
    total_processed = 0
    LOGGER.warning(f"START: {datetime.datetime.now()} ")

    # Set the destination table name, concept map UUID and concept map version UUID in count_query and select_query.
    count_query = """
        select count(ctc.code) as goal from
        concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        where cm.uuid = 'c504f599-6bf6-4865-8220-bb199e3d1809'
        and cmv.uuid = '955e518a-8030-4fe5-9e61-e0a6e6fda1b3'
        """
    select_query = """
        select cm.title, cm.uuid as cm_uuid, cmv.version as cm_version, 
        ctc.code, ctc.display, ctc.uuid as custom_terminologies_code_uuid, 
        cr.uuid as concept_map_concept_relationship_uuid,
        cr.target_concept_code, cr.target_concept_display, cr.target_concept_system
        from concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        where cm.uuid = 'c504f599-6bf6-4865-8220-bb199e3d1809'
        and cmv.uuid = '955e518a-8030-4fe5-9e61-e0a6e6fda1b3'
        and ctc.uuid > :start_uuid
        and ctc.uuid not in (
            select custom_terminologies_code_uuid from custom_terminologies.test_code_condition_duplicates
        )
        order by ctc.uuid
        limit :page_size
        """
    from app.database import get_db

    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    conn = get_db()
    try:
        count1 = conn.execute(
            text(count_query)
        ).first()
        LOGGER.warning(f"Rows in v4 table: {count1}")
        count2 = conn.execute(
            text("select count(*) from custom_terminologies.test_code_condition_duplicates")
        ).first()
        LOGGER.warning(f"Output rows completed before this run: {count2}")
    except Exception as e:
        conn.rollback()
        raise e

    while not done:
        try:
            result = conn.execute(
                text(select_query),
                {
                    "start_uuid": last_previous_uuid,
                    "page_size": page_size
                }
            ).fetchall()

            # process the results from this batch
            count = len(result)
            if count == 0:
                done = True
            else:
                for row in result:
                    last_previous_uuid = row.custom_terminologies_code_uuid
                    total_processed += 1
                    if total_processed % report_page_size == 0:
                        LOGGER.warning(f"Rows so far this run: {total_processed}")

                    # use parts of a work-in-progress migration function to get a normalized code_string value
                    (
                        code_schema,
                        code_simple,
                        code_jsonb,
                        code_string,
                        rejected
                    ) = prepare_dynamic_value_for_sql_issue(row.code, row.display)

                    insert_query = """
                    insert into custom_terminologies.test_code_condition_duplicates
                    (
                        custom_terminologies_code_uuid,
                        code,
                        normalized_code_value,
                        display,
                        cm_title,
                        cm_uuid,
                        cm_version,
                        concept_map_concept_relationship_uuid,
                        target_concept_code, 
                        target_concept_display, 
                        target_concept_system
                    )
                    values 
                    (
                        :custom_terminologies_code_uuid,
                        :code,
                        :normalized_code_value,
                        :display,
                        :cm_title,
                        :cm_uuid,
                        :cm_version,
                        :concept_map_concept_relationship_uuid,
                        :target_concept_code, 
                        :target_concept_display, 
                        :target_concept_system
                    )
                    """

                    try:
                        conn.execute(
                            text(insert_query),
                            {
                                "custom_terminologies_code_uuid": row.custom_terminologies_code_uuid,
                                "normalized_code_value": code_string,
                                "code": row.code,
                                "display": row.display,
                                "cm_title": row.title,
                                "cm_uuid": row.cm_uuid,
                                "cm_version": row.cm_version,
                                "concept_map_concept_relationship_uuid": row.concept_map_concept_relationship_uuid,
                                "target_concept_code": row.target_concept_code,
                                "target_concept_display": row.target_concept_display,
                                "target_concept_system": row.target_concept_system,
                            }
                        )
                        conn.commit()

                    except Exception as e:
                        conn.rollback()
                        error_summary = message_exception_summary(e)
                        # already processed this row? if so, just skip it
                        if "test_code_condition_duplicates_pkey" not in error_summary:
                            raise e

                    # for row in result

        except Exception as e:
            conn.rollback()
            info = "".join(traceback.format_exception(*sys.exc_info()))
            LOGGER.warning(f"""\nERROR: {message_exception_summary(e)}\n\n{info}""")
            LOGGER.warning("HALTED due to ERROR.")
            done = True

        # while not done

    LOGGER.warning("DONE.")


if __name__=="__main__":
    load_condition_duplicates()

