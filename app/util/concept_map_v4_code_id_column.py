import datetime
import json
import sys
import traceback
from unittest import skip
import logging

import pytest
import uuid
from sqlalchemy import text

from app.database import get_db
from app.helpers.format_helper import prepare_dynamic_value_for_sql_issue
from app.helpers.message_helper import message_exception_summary
from app.helpers.id_helper import generate_code_id

LOGGER = logging.getLogger()


def identify_duplicate_code_id_values_in_v4(
        concept_map_uuid: str = None,
        concept_map_version_uuid: str = None,
        output_table_name: str = None,
        output_pkey_distinct_constraint_name: str = None,
):
    """
    Populates a new normalized_code_id column in custom_terminologies.code with values that Systems can leverage to:

    STOP THIS CYCLE:
    1. Mapping Request Service has no way to screen incoming codes for what will eventually turn out to be duplicates
    2. Mapping Request Service sends all mapping work requests to the Content team
    3. Content works all requests, in good faith,
    4. some (many over time) become duplicate mappings, possibly mapped to the same target, possibly not
    5 there is no "diff" possible for v4 ConceptMaps (will be in v5) so no one sees this happened until ingestion fails

    EASY TO FIX:
    only after the new normalized_code_id column is populated in custom_terminologies.code, impossible without it

    APPROACH:
    1. Add normalized_code_id column to code table. Populate the column using this function.
    2. The Mapping Request Service calculates the normalized_code_id on each incoming code/display pair and if that
      normalized_code_id is already present in custom_terminologies.code AND ALREADY MAPPED in the relevant concept map
      for that resource_type, element, tenant, value set, terminology, and code, no work request for Content is created.
      (Only the normalized_code_id gives us the data to check this rapidly from within Mapping Request Service.
    3. A "to do" comment in mapping_request_service.py has been added where this would need to be placed (see open PR)
    4. Once the duplication from data ingestion is stopped, de-duplication of existing concept maps could occur.
       concept_map_duplicate_codes.py can output a review spreadsheet for Content (see open PR) to enable these steps:
    5. A "diff" for ConceptMap artifacts is possible when you order the serialization of rows by normalized_code_id.
       The rationale for the order will not be human-visible but "diffs" will be TRIVIAL TO SEE once rows are ordered
       in the before and after. We need not build a Retool page immediately: any text "diff" program can operate on JSON
       files as text. Note: we would want to re-output ConceptMap artifacts once before de-dup, and once after, to show
       what is removed by de-dup of mappings. From there, we are only interested in changes from needed work by Content.

    REVIEW of concept_map_duplicate_codes.py spreadsheet:
    Use with Content review to select which duplicates in concept_map.concept_relationship table are best to keep:
    1. Systems: all rows where normalized_code_id is unique across the code table - these rows can be auto-marked KEEP
    2. Systems: where normalized_code_id has duplicates - those with IDENTICAL TARGETS ONLY - 1 can be auto-marked KEEP
    3. Systems: use the concept_map_duplicate_codes.py function to use this normalized_code_id
    3. select * ... order by normalized_code_id, download the output as a CSV file, open in Excel, save as XLSX format
    4. Content: in Excel,marks the 1 KEEP decision for each normalized_code_id group in that empty "fix_action" column
    5. Content: save Excel changes, return XLSX file to Systems, Systems will run the following script(s):
    6. Load Systems decisions in to the "fix_action" column appropriately. Anything not marked KEEP will be discarded.
    7. Systems: where a normalized_code_id with duplicates has a row marked to KEEP - delete all other rows in group
    8. Systems: where a normalized_code_id with duplicates has no row marked to KEEP - see Content, or keep 1 at random
    """
    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    if (
        concept_map_uuid is None or
        concept_map_version_uuid is None or
        output_pkey_distinct_constraint_name is None or
        output_table_name is None
    ):
        LOGGER.warning("NO INPUT.")
        return

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
        where cm.uuid = :concept_map_uuid
        and cmv.uuid = :concept_map_version_uuid
        """
    select_query = f"""
        select cm.title, cm.uuid as cm_uuid, cmv.version as cm_version, 
        ctc.code, ctc.display, ctc.uuid as custom_terminologies_code_uuid, 
        cr.uuid as concept_map_concept_relationship_uuid,
        cr.target_concept_code, cr.target_concept_display, cr.target_concept_system
        from concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        where cm.uuid = :concept_map_uuid
        and cmv.uuid = :concept_map_version_uuid
        and ctc.uuid >= :start_uuid
        and ctc.code_id is not null
        order by ctc.uuid
        limit :page_size
        """
    from app.database import get_db
    conn = get_db()
    try:
        count1 = conn.execute(
            text(count_query),
            {
                "concept_map_uuid": concept_map_uuid,
                "concept_map_version_uuid": concept_map_version_uuid,
            }
        ).first()
        LOGGER.warning(f"Rows in v4 table: {count1}")
        count2 = conn.execute(
            text(f"select count(*) from {output_table_name}")
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
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
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

                    # leverage v5 migration functions to get a normalized code_string and code_id values
                    (
                        code_schema,
                        code_simple,
                        code_jsonb,
                        code_string,
                        rejected
                    ) = prepare_dynamic_value_for_sql_issue(row.code, row.display)
                    normalized_code_id = generate_code_id(  # Not supporting depends on data at the moment
                        code_string=code_string,
                        display=json.loads(code_string).get("text", ""),
                    )

                    insert_query = f"""
                    insert into custom_terminologies.code
                    (
                        normalized_code_id
                    )
                    values 
                    (
                        :normalized_code_id
                    )
                    where uuid = :code_uuid
                    """

                    try:
                        conn.execute(
                            text(insert_query),
                            {
                                "normalized_code_id": normalized_code_id,
                                "code_uuid": row.custom_terminologies_code_uuid
                            }
                        )
                        conn.commit()

                    except Exception as e:
                        conn.rollback()
                        error_summary = message_exception_summary(e)
                        # already processed this row? if so, just skip it
                        if output_pkey_distinct_constraint_name not in error_summary:
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


def identify_v4_concept_map_duplicates():
    """
    Command line endpoint for identifying v4 duplicate check locally. Does not support command line inputs at this time.
    """
    # todo: For this command line endpoint, add a line for each table to be checked.
    identify_duplicates_for_v4_concept_map(
        "c504f599-6bf6-4865-8220-bb199e3d1809",
        "955e518a-8030-4fe5-9e61-e0a6e6fda1b3",
        "custom_terminologies.test_code_condition_duplicates_w_id",
        "test_code_condition_duplicates_w_id_pkey"
    )


if __name__=="__main__":
    identify_v4_concept_map_duplicates()

