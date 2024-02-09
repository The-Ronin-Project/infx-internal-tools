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


def load_duplicate_for_v4_concept_map(
        concept_map_uuid: str = None,
        concept_map_version_uuid: str = None,
        output_table_name: str = None,
        output_pkey_distinct_constraint_name: str = None,
):
    """
    Recommend keeping it in the repo as long as there are consuming teams using v4.
    For a list of UUIDs for v4 concept maps that Informatics is keeping, see 2 enum lists:
    app.util.data_migration.ConceptMapsForContent and app.util.data_migration.ConceptMapsForSystems

    You may run this from the Postman call named "Concept Map v4 Duplicate Check" - set the 4 inputs in the payload.
    For examples of inputs see the command line function load_v4_concept_map_duplicates() or current Postman payload.

    Creates an output table with a schema like custom_terminologies.test_code_condition_duplicates with values you can:

    Use (with management approval for urgency) to apply technical resolution to duplicates without reference to
    mapping targets, which may be different and of different quality. The query for performing this looks like:
    ```
    delete from custom_terminologies.code
    where uuid in
    (WITH DuplicateValues AS (
        SELECT
            normalized_code_id,
            custom_terminologies_code_uuid, -- Assuming this is a unique identifier
            ROW_NUMBER() OVER (PARTITION BY normalized_code_id ORDER BY custom_terminologies_code_uuid) as rn
        FROM custom_terminologies.test_code_condition_duplicates_w_id
    ), DuplicatesToDelete AS (
        SELECT t.*
        FROM custom_terminologies.test_code_condition_duplicates_w_id t
        INNER JOIN DuplicateValues d
        ON t.normalized_code_id = d.normalized_code_id
        AND t.custom_terminologies_code_uuid = d.custom_terminologies_code_uuid -- Joining on the unique identifier
        WHERE d.rn > 1)
    SELECT ctc.uuid from custom_terminologies.code ctc
    join DuplicatesToDelete dtd
    on ctc.code=dtd.code
    and ctc.display=dtd.display
    and ctc.terminology_version_uuid='45022739-00fb-4fbc-a2f2-82614cde6f68');
    ```

    Use with a Content review to select which duplicates in concept_map.concept_relationship table are "best" to keep:

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

    Note: An SQL expression for an example output table, custom_terminologies.test_code_condition_duplicates, is below:
    ```
    -- Table: custom_terminologies.test_code_condition_duplicates

    -- DROP TABLE IF EXISTS custom_terminologies.test_code_condition_duplicates;

    CREATE TABLE IF NOT EXISTS custom_terminologies.test_code_condition_duplicates
    (
        custom_terminologies_code_uuid uuid NOT NULL,
        normalized_code_value character varying COLLATE pg_catalog."default",
        display character varying COLLATE pg_catalog."default",
        depends_on_property character varying COLLATE pg_catalog."default",
        depends_on_value character varying COLLATE pg_catalog."default",
        cm_title character varying COLLATE pg_catalog."default",
        cm_uuid uuid,
        cm_version character varying COLLATE pg_catalog."default",
        code character varying COLLATE pg_catalog."default",
        target_concept_code character varying COLLATE pg_catalog."default",
        target_concept_display character varying COLLATE pg_catalog."default",
        target_concept_system character varying COLLATE pg_catalog."default",
        concept_map_concept_relationship_uuid uuid,
        CONSTRAINT test_code_condition_duplicates_pkey PRIMARY KEY (custom_terminologies_code_uuid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS custom_terminologies.test_code_condition_duplicates
        OWNER to roninadmin;
    ```
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
        and ctc.uuid not in (
            select custom_terminologies_code_uuid from {output_table_name}
        )
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

                    # use parts of a work-in-progress migration function to get a normalized code_string value
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
                    insert into {output_table_name}
                    (
                        custom_terminologies_code_uuid,
                        code,
                        normalized_code_value,
                        normalized_code_id,
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
                        :normalized_code_id,
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
                                "normalized_code_id": normalized_code_id,
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


def load_v4_concept_map_duplicates():
    """
    Command line endpoint for running v4 duplicate check locally. Does not support command line inputs at this time.
    """
    # todo: For this command line endpoint, add a line for each table to be checked.
    load_duplicate_for_v4_concept_map(
        "c504f599-6bf6-4865-8220-bb199e3d1809",
        "955e518a-8030-4fe5-9e61-e0a6e6fda1b3",
        "custom_terminologies.test_code_condition_duplicates_w_id",
        "test_code_condition_duplicates_w_id_pkey"
    )


if __name__=="__main__":
    load_v4_concept_map_duplicates()

