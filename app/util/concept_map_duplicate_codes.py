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
from app.helpers.format_helper import prepare_code_and_display_for_storage_migration, filter_unsafe_depends_on_value, \
    prepare_depends_on_value_for_storage, prepare_depends_on_attributes_for_code_id_migration
from app.helpers.message_helper import message_exception_summary
from app.helpers.id_helper import generate_code_id, generate_mapping_id_with_source_code_id
from app.util.data_migration import convert_empty_to_null

LOGGER = logging.getLogger()


def get_count_query() -> str:
    return """
        select cm.title as concept_map_title, count(ctc.code) as goal from
        concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        where cm.uuid = :concept_map_uuid
        and cmv.uuid = :concept_map_version_uuid
        group by cm.title
        """

def load_duplicate_for_v4_concept_map(
        concept_map_uuid: str = None,
        concept_map_version_uuid: str = None,
        output_table_name: str = None,
        output_pkey_distinct_constraint_name: str = None,
):
    """
    The duplicates in v4 data are:
    - code: the same code object in more than one row in the (only) code table: a "false difference" duplicate
    - mapping: the same source code object appears in more than one row in the same concept map table; the source code
               objects in these cases may be "false difference" duplicates or literally the same value. The variations:
        - "simple duplicate": the same source code object mapped to the same target in the same concept map
        - "Content should review": the same source code object mapped to different targets in the same concept map
        - NOT an issue: the same source code object appears in more than one concept map in our data: expected
    - NOT an issue: any of the above, in a concept map that Informatics has identified as NOT using. See next paragraph.

    For a list of UUIDs for v4 concept maps that Informatics IS using, see 2 enum lists:
    app.util.data_migration.ConceptMapsForContent and app.util.data_migration.ConceptMapsForSystems
    Query the current clinical-content database for "active" and "pending" versions of each.

    You may run this from the Postman call named "Concept Map v4 Duplicate Check" - set the 4 inputs in the payload.
    For examples of inputs see the command line function load_v4_concept_map_duplicates() or current Postman payload.
    Creates output table with values you can use as follows:

    "keep_non_duplicate" - this is a unique mapping, keep it in the map.

    "keep_1_discard_others" - mapping source and target are the same. The query for deleting all but 1 looks like:
    ```
    delete from custom_terminologies.code
    where uuid in  (
        WITH DuplicateValues AS (
            select code_deduplication_hash,
            custom_terminologies_code_uuid,
            ROW_NUMBER() OVER (PARTITION BY normalized_code_id ORDER BY custom_terminologies_code_uuid) as rn
            from {output_table_name}
        ), DuplicatesToDelete AS (
            SELECT t.*
            FROM {output_table_name} t
            INNER JOIN DuplicateValues d
            ON t.code_deduplication_hash = d.code_deduplication_hash
            AND t.custom_terminologies_code_uuid = d.custom_terminologies_code_uuid
            WHERE d.rn > 1
        )
        SELECT o.custom_terminologies_code_uuid
        from {output_table_name} o
        join DuplicatesToDelete dtd
        on o.code = dtd.code
        and o.display = dtd.display
        where o.cm_uuid = :concept_map_uuid
        and o.cm_version_uuid = :concept_map_version_uuid
    )
    ```
    "content_review" - consult with Content to select which mapping is best to keep and remove the others from
    concept_maps.concept_relationship (no query provided here). Steps to use the table output by this function are:
    1. select * ... for the specific concept map and concept map version, order by deduplication_hash
    2. download the output as a CSV file, open in Excel, save as XLSX format
    3. Content: in Excel,marks the 1 KEEP decision for each deduplication_hash group in that empty "fix_action" column
    4. Content: save Excel changes, return XLSX file to Systems, Systems will run the following script(s):
    5. Load Systems decisions in to the "fix_action" column appropriately. Anything not marked KEEP will be discarded.
    6. Systems: where a deduplication_hash with duplicates has a row marked to KEEP - delete all other rows in group
    7. Systems: where a deduplication_hash with duplicates has no row marked to KEEP - see Content, or keep 1 at random

    Note: SQL for sample output table: sql_schemas/custom_terminologies/v4_concept_map_duplicates_auto_resolve.sql
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
    UUID_START = uuid.UUID("00000000-0000-0000-0000-000000000000")
    first_uuid = UUID_START

    # initialize
    page_size = 500
    report_page_size = 500
    start_uuid = first_uuid
    last_previous_uuid = start_uuid
    done = False
    total_processed = 0
    concept_map_title = ""
    LOGGER.warning(f"\nSTART: {datetime.datetime.now()} \n")

    # Set the destination table name in the query text - it could be hard-coded, but this facilitates separate tests
    count_query = get_count_query()
    select_query = f"""
        select cm.title, cm.uuid as cm_uuid, cmv.version as cm_version, cmv.uuid as cm_version_uuid, 
        ctc.code, ctc.display, ctc.uuid as custom_terminologies_code_uuid, 
        cr.uuid as concept_map_concept_relationship_uuid, rc.code as relationship_code,
        cr.target_concept_code, cr.target_concept_display, cr.target_concept_system
        from concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        join concept_maps.relationship_codes rc on cr.relationship_code_uuid = rc.uuid
        where cm.uuid = :concept_map_uuid
        and cmv.uuid = :concept_map_version_uuid
        and ctc.uuid >= :start_uuid
        and ctc.uuid not in (
            select custom_terminologies_code_uuid from {output_table_name}
        )
        order by ctc.uuid
        limit :page_size
        """
    insert_query = f"""
        insert into {output_table_name}
        (
            custom_terminologies_code_uuid,
            normalized_code_value,
            normalized_display_value,
            code_deduplication_hash,
            mapping_deduplication_hash,
            code,
            display,
            cm_title,
            cm_uuid,
            cm_version,
            cm_version_uuid,
            concept_map_concept_relationship_uuid,
            relationship_code,
            target_concept_code, 
            target_concept_display, 
            target_concept_system
        )
        values 
        (
            :custom_terminologies_code_uuid,
            :normalized_code_value,
            :normalized_display_value,
            :code_deduplication_hash,
            :mapping_deduplication_hash,
            :code,
            :display,
            :cm_title,
            :cm_uuid,
            :cm_version,
            :cm_version_uuid,
            :concept_map_concept_relationship_uuid,
            :relationship_code,
            :target_concept_code, 
            :target_concept_display, 
            :target_concept_system
        )
    """

    conn = get_db()
    try:
        try:
            count_goal = conn.execute(
                text(count_query),
                {
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
                }
            ).first()
            if count_goal is None:
                concept_map_title = f"Test ONLY ConceptMap with UUID: {concept_map_uuid}"
                LOGGER.warning(f"'{concept_map_title}'")
            else:
                concept_map_title = count_goal.concept_map_title
                LOGGER.warning(f"'{concept_map_title}' row count: {count_goal.goal}")
            count_completed = conn.execute(
                text(f"""
                    select count(*) as completed from {output_table_name} 
                    where cm_uuid = :concept_map_uuid
                    and cm_version_uuid = :concept_map_version_uuid
                    """
                ),
                {
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
                }
            ).first()
            LOGGER.warning(f"Output rows completed before this run: {count_completed.completed}")
            if count_goal is not None and count_completed is not None and count_goal.goal == count_completed.completed:
                done = True
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
                        if concept_map_title == "":
                            concept_map_title = row.title
                        last_previous_uuid = row.custom_terminologies_code_uuid
                        total_processed += 1
                        if total_processed % report_page_size == 0:
                            LOGGER.warning(f"Rows so far this run: {total_processed}")

                        # leverage v5 migration functions to get code, display, depends_on, and deduplication values
                        (
                            code_schema,
                            code_simple,
                            code_jsonb,
                            code_string,
                            display_string
                        ) = prepare_code_and_display_for_storage_migration(row.code, row.display)
                        (
                            depends_on_value,
                            rejected_depends_on_value
                        ) = filter_unsafe_depends_on_value(row.depends_on_value)
                        has_depends_on = (rejected_depends_on_value is None and depends_on_value is not None)
                        (
                            depends_on_value_schema,
                            depends_on_value_simple,
                            depends_on_value_jsonb,
                            depends_on_value_string,
                            depends_on_property_string
                        ) = prepare_depends_on_value_for_storage(depends_on_value, row.depends_on_property)
                        depends_on_value_for_code_id = prepare_depends_on_attributes_for_code_id_migration(
                            depends_on_value_string,
                            depends_on_property_string,
                            row.depends_on_system,
                            row.depends_on_display
                        )
                        code_deduplication_hash = generate_code_id(
                            code_string=code_string,
                            display_string=display_string,
                            depends_on_value_string=depends_on_value_for_code_id,
                        )
                        mapping_deduplication_hash = generate_mapping_id_with_source_code_id(
                            source_code_id=code_deduplication_hash,
                            relationship_code=row.relationship_code,
                            target_concept_code=row.target_concept_code,
                            target_concept_display=row.target_concept_display,
                            target_concept_system=row.target_concept_system,
                        )

                        try:
                            conn.execute(
                                text(insert_query),
                                {
                                    "custom_terminologies_code_uuid": row.custom_terminologies_code_uuid,
                                    "normalized_code_value": code_string,
                                    "normalized_display_value": display_string,
                                    "code_deduplication_hash": code_deduplication_hash,
                                    "code": row.code,
                                    "display": row.display,
                                    "cm_title": row.title,
                                    "cm_uuid": row.cm_uuid,
                                    "cm_version": row.cm_version,
                                    "cm_version_uuid": row.cm_version_uuid,
                                    "concept_map_concept_relationship_uuid": row.concept_map_concept_relationship_uuid,
                                    "relationship_code": row.relationship_code,
                                    "target_concept_code": row.target_concept_code,
                                    "target_concept_display": row.target_concept_display,
                                    "target_concept_system": row.target_concept_system,
                                    "mapping_deduplication_hash": mapping_deduplication_hash,
                                }
                            )
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            error_summary = message_exception_summary(e)
                            # already processed this row? if so, just skip it
                            if output_pkey_distinct_constraint_name not in error_summary:
                                raise e
            except Exception as e:
                conn.rollback()
                done = True
                raise e
            # while not done / try
    except Exception as e:
        conn.rollback()
        info = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(f"""\nERROR: {message_exception_summary(e)}\n\n{info}""")
        LOGGER.warning("HALTED due to ERROR.")
    # main / try

    LOGGER.warning(f"Rows so far this run: {total_processed}")
    LOGGER.warning(f"'{concept_map_title}' completed loading")
    LOGGER.warning("\nDONE.\n")


def mark_duplicate_for_v4_concept_map(
        concept_map_uuid: str = None,
        concept_map_version_uuid: str = None,
        output_table_name: str = None,
):
    # Get some introductory info that is helpful for the log
    count_query = get_count_query()

    # The source code is used only once in this map
    mark_code_single_query = f"""
        WITH DuplicateValues AS (
            select distinct code_deduplication_hash, count(code_deduplication_hash) as count
            from {output_table_name}
            group by code_deduplication_hash
            order by count desc
        ), DuplicatesToMark AS (
            SELECT t.*
            FROM {output_table_name} t
            INNER JOIN DuplicateValues d
            ON t.code_deduplication_hash = d.code_deduplication_hash
            WHERE d.count = 1
        )
        SELECT o.custom_terminologies_code_uuid
        from {output_table_name} o
        join DuplicatesToMark dtm
        on o.code = dtm.code
        and o.display = dtm.display
        where o.cm_uuid = :concept_map_uuid
        and o.cm_version_uuid = :concept_map_version_uuid
        """

    # The source code is used in more than one mapping, and the targets are different: needs Content review
    mark_mapping_dup_query = f"""
        WITH DuplicateValues AS (
            select distinct mapping_deduplication_hash, count(mapping_deduplication_hash) as count
            from {output_table_name}
            group by code_deduplication_hash
            order by count desc
        ), DuplicatesToMark AS (
            SELECT t.*
            FROM {output_table_name} t
            INNER JOIN DuplicateValues d
            ON t.mapping_deduplication_hash = d.mapping_deduplication_hash
            WHERE d.count > 1
        )
        SELECT o.custom_terminologies_code_uuid
        from {output_table_name} o
        join DuplicatesToMark dtm
        on o.code = dtm.code
        and o.display = dtm.display
        where o.cm_uuid = :concept_map_uuid
        and o.cm_version_uuid = :concept_map_version_uuid
        """

    # The source code is used in more than one mapping, and targets are same: can auto-resolve by deleting all but 1
    mark_code_dup_mapping_diff_query = f"""
        WITH DuplicateValues AS (
            select distinct code_deduplication_hash, count(code_deduplication_hash) as count
            from {output_table_name}
            group by code_deduplication_hash
            order by count desc
        ), DuplicatesToMark AS (
            SELECT t.*
            FROM {output_table_name} t
            INNER JOIN DuplicateValues d
            ON t.code_deduplication_hash = d.code_deduplication_hash
            and t.mapping_deduplication_hash <> d.mapping_deduplication_hash
            WHERE d.count > 1
        )
        SELECT o.custom_terminologies_code_uuid
        from {output_table_name} o
        join DuplicatesToMark dtm
        on o.code = dtm.code
        and o.display = dtm.display
        where o.cm_uuid = :concept_map_uuid
        and o.cm_version_uuid = :concept_map_version_uuid
        """
    update_fix_code_single = f"""
        update {output_table_name}  
        set fix_action = :fix_action  
        where custom_terminologies_code_uuid in ({mark_code_single_query})
        """
    update_fix_mapping_dup = f"""
        update {output_table_name}  
        set fix_action = :fix_action  
        where custom_terminologies_code_uuid in ({mark_mapping_dup_query})
     """
    update_fix_code_dup_mapping_diff_query = f"""
        update {output_table_name}  
        set fix_action = :fix_action  
        where custom_terminologies_code_uuid in ({mark_code_dup_mapping_diff_query})
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
        output_table_name is None
    ):
        LOGGER.warning("NO INPUT.")
        return

    LOGGER.warning(f"START: {datetime.datetime.now()} ")

    conn = get_db()
    try:
        count_goal = conn.execute(
            text(count_query),
            {
                "concept_map_uuid": concept_map_uuid,
                "concept_map_version_uuid": concept_map_version_uuid,
            }
        ).first()
        if count_goal is None:
            concept_map_title = f"Test ONLY ConceptMap with UUID: {concept_map_uuid}"
            LOGGER.warning(f"'{concept_map_title}'")
        else:
            concept_map_title = count_goal.concept_map_title
            LOGGER.warning(f"'{concept_map_title}' row count: {count_goal.goal}")

        # MARK rows with fix_action where Systems can auto-resolve, or mark as needing Content review.
        # Of the possible logic cases, we simplify by calling the main function only on concept map UUIDs in
        # app.util.data_migration.ConceptMapsForContent. This guarantees codes and mappings are worth keeping.
        LOGGER.warning(f"Marking to drive auto-resolve functions")

        try:
            LOGGER.warning(f"Marking non-duplicate codes to keep")
            LOGGER.warning(update_fix_code_single)
            conn.execute(
                text(update_fix_code_single),
                {
                    "fix_action": "keep_non_duplicate",
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
                }
            )
            conn.commit()

            LOGGER.warning(f"Marking duplicate codes (that have non-duplicate mapping targets) for Content review")
            LOGGER.warning(update_fix_code_dup_mapping_diff_query)
            conn.execute(
                text(update_fix_code_dup_mapping_diff_query),
                {
                    "fix_action": "content_review",
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
                }
            )
            conn.commit()

            LOGGER.warning(f"Marking duplicate mappings to auto-resolve (run query to keep only 1, discard others)")
            LOGGER.warning(update_fix_mapping_dup)
            conn.execute(
                text(update_fix_mapping_dup),
                {
                    "fix_action": "keep_1_discard_others",
                    "concept_map_uuid": concept_map_uuid,
                    "concept_map_version_uuid": concept_map_version_uuid,
                }
            )
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise e

    except Exception as e:
        conn.rollback()
        info = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(f"""\nERROR: {message_exception_summary(e)}\n\n{info}""")
        LOGGER.warning("HALTED due to ERROR.")
    # main / try

    LOGGER.warning("\nDONE.\n")


def load_v4_concept_map_duplicates():
    """
    Command line endpoint for running v4 duplicate check locally. Does not support command line inputs at this time.
    """
    # todo: For this command line endpoint, add a line for each table to be checked.
    # MDA Document Reference active
    load_duplicate_for_v4_concept_map(
        "e7734e09-da3b-45f6-a845-24583d6709fb",
        "f48b4ad4-1945-47d1-ab2b-2b9b6823c522",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
        "pkey_test_v4_concept_map_duplicates_auto_resolve"
    )
    # PSJ Condition active
    load_duplicate_for_v4_concept_map(
        "8f648ad7-1dfb-46e1-872f-598ece845624",
        "4cf3cd09-2992-4124-9736-3d5751ea1ec8",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
        "pkey_test_v4_concept_map_duplicates_auto_resolve"
    )
    # PSJ Condition pending
    # todo: a "pending" map is not doing will with the count_query at the start of the loop
    # load_duplicate_for_v4_concept_map(
    #     "8f648ad7-1dfb-46e1-872f-598ece845624",
    #     "c592e970-d6c6-4ec6-a24a-f9c8867c46b4",
    #     "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    #     "pkey_test_v4_concept_map_duplicates_auto_resolve"
    # )
    # MDA Condition to SNOMED CT active
    load_duplicate_for_v4_concept_map(
        "c504f599-6bf6-4865-8220-bb199e3d1809",
        "955e518a-8030-4fe5-9e61-e0a6e6fda1b3",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
        "pkey_test_v4_concept_map_duplicates_auto_resolve"
    )
    # PSJ Doc Reference active
    load_duplicate_for_v4_concept_map(
        "2c353c65-e4d7-4932-b518-7bc42d98772d",
        "5ee300c0-426f-4515-b522-7af54c55b913",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
        "pkey_test_v4_concept_map_duplicates_auto_resolve"
    )
    # PSJ Doc Reference pending
    # todo: a "pending" map is not doing will with the count_query at the start of the loop
    # load_duplicate_for_v4_concept_map(
    #     "2c353c65-e4d7-4932-b518-7bc42d98772d",
    #     "083f3282-3391-4186-9c07-2c45be1b3ee7",
    #     "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    #     "pkey_test_v4_concept_map_duplicates_auto_resolve"
    # )
    # Test ONLY: Condition active
    load_duplicate_for_v4_concept_map(
        "ae61ee9b-3f55-4d3c-96e7-8c7194b53767",
        "7bff7e50-7d95-46f6-8268-d18a5257327b",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
        "pkey_test_v4_concept_map_duplicates_auto_resolve"
    )


def mark_v4_concept_map_duplicates():
    """
    Command line endpoint for marking v4 duplicate check locally. Does not support command line inputs at this time.
    """
    # todo: For this command line endpoint, add a line for each table to be checked.
    # MDA Document Reference active
    mark_duplicate_for_v4_concept_map(
        "e7734e09-da3b-45f6-a845-24583d6709fb",
        "f48b4ad4-1945-47d1-ab2b-2b9b6823c522",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    )
    # PSJ Condition active
    mark_duplicate_for_v4_concept_map(
        "8f648ad7-1dfb-46e1-872f-598ece845624",
        "4cf3cd09-2992-4124-9736-3d5751ea1ec8",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    )
    # PSJ Condition pending
    # todo: a "pending" map is not doing will with the count_query at the start of the loop
    # mark_duplicate_for_v4_concept_map(
    #     "8f648ad7-1dfb-46e1-872f-598ece845624",
    #     "4cf3cd09-2992-4124-9736-3d5751ea1ec8",
    #     "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    # )
    # MDA Condition to SNOMED CT active
    mark_duplicate_for_v4_concept_map(
        "c504f599-6bf6-4865-8220-bb199e3d1809",
        "955e518a-8030-4fe5-9e61-e0a6e6fda1b3",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    )
    # PSJ Doc Reference active
    mark_duplicate_for_v4_concept_map(
        "2c353c65-e4d7-4932-b518-7bc42d98772d",
        "5ee300c0-426f-4515-b522-7af54c55b913",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    )
    # # PSJ Doc Reference pending
    # todo: a "pending" map is not doing will with the count_query at the start of the loop
    # mark_duplicate_for_v4_concept_map(
    #     "2c353c65-e4d7-4932-b518-7bc42d98772d",
    #     "083f3282-3391-4186-9c07-2c45be1b3ee7",
    #     "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    # )
    # Test ONLY: Condition active
    mark_duplicate_for_v4_concept_map(
        "ae61ee9b-3f55-4d3c-96e7-8c7194b53767",
        "7bff7e50-7d95-46f6-8268-d18a5257327b",
        "custom_terminologies.test_v4_concept_map_duplicates_auto_resolve",
    )


if __name__=="__main__":
    load_v4_concept_map_duplicates()
    # mark_v4_concept_map_duplicates()

