import datetime
import json
import sys
import traceback
from unittest import skip
import logging

import pytest
import uuid
from sqlalchemy import text

import app.concept_maps.models
from app.database import get_db
from app.enum.concept_maps_for_content import ConceptMapsForContent
from app.enum.concept_maps_for_systems import ConceptMapsForSystems
from app.helpers.format_helper import prepare_code_and_display_for_storage_migration, filter_unsafe_depends_on_value, \
    prepare_depends_on_value_for_storage, prepare_depends_on_attributes_for_code_id_migration
from app.helpers.message_helper import message_exception_summary
from app.helpers.id_helper import generate_code_id
from app.util.data_migration import convert_empty_to_null, get_v5_concept_map_uuids_in_n_blocks_for_parallel_process

LOGGER = logging.getLogger()


def identify_duplicate_code_values_in_v4(
        concept_map_uuid: str = None,
        concept_map_version_uuid: str = None
):
    """
    Currently this function limits the work to code values used in a specific concept map and version.
    See identify_v4_concept_map_duplicates() which calls identify_v4_concept_map_duplicates_ctive_and_pending().
    todo: If both inputs are None, walk through the entire custom_terminologies.code table.

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

    identify_duplicate_code_values_in_v4() accomplishes a first step ("Step 1") towards resolving the above issues. It
    populates a new deduplication_hash column in custom_terminologies.code with values that Systems can leverage to:

    STOP THIS CYCLE:
    1. Mapping Request Service has no way to screen incoming codes for what will eventually turn out to be duplicates
    2. Mapping Request Service sends all mapping work requests to the Content team
    3. Content works all requests, in good faith
    4. Some (many over time) become duplicate mappings, possibly mapped to the same target, possibly not
    5. There is no "diff" possible for v4 ConceptMaps (will be in v5) so no one sees this happened until ingestion fails

    EASY TO FIX:
    only after the new deduplication_hash column is populated in custom_terminologies.code, impossible without it

    APPROACH:
    1. Add deduplication_hash column to the v4 custom_terminologies.code table. Populate the column using this function.
    2. The Mapping Request Service calculates the deduplication_hash on each incoming code/display pair and if that
      deduplication_hash is already present in custom_terminologies.code AND ALREADY MAPPED in the relevant concept map
      for that resource_type, element, tenant, value set, terminology, and code, no work request for Content is created.
      (Only the deduplication_hash gives us the data to check this rapidly from within Mapping Request Service).
    3. A "to do" comment in mapping_request_service.py has been added, in the spot where this call must be made.

    REVIEW of concept_map_duplicate_codes.py spreadsheet:
    Use with Content review to select which duplicates in concept_map.concept_relationship table are best to keep:
    1. Systems: all rows where deduplication_hash is unique across the code table - these rows can be auto-marked KEEP
    2. Systems: where deduplication_hash has duplicates - those with IDENTICAL TARGETS ONLY - 1 can be auto-marked KEEP
    3. Systems: run the concept_map_duplicate_codes.py function to make a table of duplicates info for Content
    4. select * ... order by deduplication_hash, download the output as a CSV file, open in Excel, save as XLSX format
    5. Content: in Excel,marks the 1 KEEP decision for each deduplication_hash group in that empty "fix_action" column
    6. Content: save Excel changes, return XLSX file to Systems, Systems will run the following script(s):
    7. Load Systems decisions in to the "fix_action" column appropriately. Anything not marked KEEP will be discarded.
    8. Systems: where a deduplication_hash with duplicates has a row marked to KEEP - delete all other rows in group
    9. Systems: where a deduplication_hash with duplicates has no row marked to KEEP - see Content, or keep 1 at random
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
        concept_map_version_uuid is None
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

    # Set the concept map UUID and concept map version UUID in query text.
    select_query = f"""
        select cm.title, cm.uuid as cm_uuid, cmv.version as cm_version, 
        ctc.code, ctc.display, ctc.uuid as custom_terminologies_code_uuid, 
        ctc.depends_on_value, ctc.depends_on_property, ctc.depends_on_system, ctc.depends_on_display,
        cr.uuid as concept_map_concept_relationship_uuid,
        cr.target_concept_code, cr.target_concept_display, cr.target_concept_system
        from concept_maps.concept_map cm
        join concept_maps.concept_map_version cmv on cmv.concept_map_uuid = cm.uuid
        join concept_maps.source_concept sc on sc.concept_map_version_uuid = cmv.uuid
        join concept_maps.concept_relationship cr on cr.source_concept_uuid = sc.uuid
        join custom_terminologies.code ctc on sc.custom_terminology_uuid = ctc.uuid
        where cm.uuid = :concept_map_uuid
        and cmv.uuid = :concept_map_version_uuid
        and ctc.uuid > :start_uuid
        and ctc.deduplication_hash is null
        order by ctc.uuid
        limit :page_size
        """
    update_query = f"""
        update custom_terminologies.code
        set deduplication_hash = :deduplication_hash
        where uuid = :custom_terminologies_code_uuid
        """

    from app.database import get_db
    conn = get_db()
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
                    if total_processed % report_page_size == 0:
                        LOGGER.warning(f"Rows so far this run: {total_processed}")
                    total_processed += 1

                    # leverage v5 migration functions to get deduplication_hash values
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
                        depends_on_value_string
                    ) = prepare_depends_on_value_for_storage(depends_on_value)
                    depends_on_value_for_code_id = prepare_depends_on_attributes_for_code_id_migration(
                        depends_on_value_string,
                        row.depends_on_property,
                        row.depends_on_system,
                        row.depends_on_display
                    )
                    deduplication_hash = generate_code_id(
                        code_string=code_string,
                        display_string=display_string,
                        depends_on_value_string=depends_on_value_for_code_id,
                    )

                    try:
                        conn.execute(
                            text(update_query),
                            {
                                "deduplication_hash": deduplication_hash,
                                "custom_terminologies_code_uuid": row.custom_terminologies_code_uuid
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
            done = True

    # while not done
    LOGGER.warning(f"Rows so far this run: {total_processed}")


def identify_v4_concept_map_duplicates(
    number_of_blocks_requested: int=4,
    block_to_process_in_this_run: int=0,
):
    """
    @param number_of_blocks_requested - how to split for parallel processing, for example request 4 blocks, 8, etc.
    @param block_to_process_in_this_run - a 0-based index - which block - must be < number_of_blocks_requested
    """
    # INFO log level leads to I/O overload due to httpx logging per issue, for 1000s of issues. At an arbitrary point in
    # processing, the error task overloads and experiences a TCP timeout, causing some number of errors to not be loaded
    LOGGER.setLevel("WARNING")

    # Create a console handler and add it to the logger if it doesn't have any handlers
    if not LOGGER.hasHandlers():
        ch = logging.StreamHandler()
        LOGGER.addHandler(ch)

    # process all the items of interest
    message = (
        f"START: {datetime.datetime.now()}\n"
        f"Concept map UUIDs in group '{block_to_process_in_this_run}' "
        f"which is 1 of {number_of_blocks_requested} groups to process:\n"
    )
    try:
        uuid_lists = get_v5_concept_map_uuids_in_n_blocks_for_parallel_process(number_of_blocks_requested)
        list_to_process = uuid_lists[block_to_process_in_this_run]
        for concept_map_uuid in list_to_process:
            message += f"  {concept_map_uuid}\n"
        LOGGER.warning(message + f"\nProcessing the {len(list_to_process)} UUIDs now.")
        for concept_map_uuid in list_to_process:
            message = identify_v4_concept_map_duplicates_active_and_pending(concept_map_uuid)
            LOGGER.warning(message)
        LOGGER.warning("DONE.")
    except Exception as e:
        info = "".join(traceback.format_exception(*sys.exc_info()))
        LOGGER.warning(f"""\nERROR: {message_exception_summary(e)}\n\n{info}""")
        LOGGER.warning("HALTED due to ERROR.")


def identify_v4_concept_map_duplicates_active_and_pending(concept_map_uuid) -> str:
    """
    Process the most recent "pending" and/or "active" version (if any)
    @return message - information to use in logging
    """
    message = f"Processing concept map UUID: {concept_map_uuid}"

    concept_map = app.concept_maps.models.ConceptMap(concept_map_uuid)
    if concept_map is None:
        return message + "\nConcept map not found."
    else:
        message += f"\nTitle: {concept_map.title}"

    # pending
    concept_map_version = concept_map.get_most_recent_version(
        active_only=False,
        load_mappings=False,
        pending_only=True
    )
    if concept_map_version is None:
        message += "\nConcept map 'pending' version not found."
    else:
        message += f"\nConcept map 'pending' version {concept_map_version.version} UUID: {concept_map_version.uuid}"
        identify_duplicate_code_values_in_v4(concept_map_uuid, concept_map_version.uuid)

    # active
    concept_map_version = concept_map.get_most_recent_version(
        active_only=True,
        load_mappings=False,
        pending_only=False
    )
    if concept_map_version is None:
        message += "\nConcept map 'active' version not found."
    else:
        message += f"\nConcept map 'active' version {concept_map_version.version} UUID: {concept_map_version.uuid}"
        identify_duplicate_code_values_in_v4(concept_map_uuid, concept_map_version.uuid)

    # done
    return message


if __name__=="__main__":
    # Template to do parallel runs on a laptop with 16 processors (15 blocks leaves 1 to do other stuff).
    # Edit the first number based on the number of CPUs on your laptop ; second number is 0-based relative to that.
    identify_v4_concept_map_duplicates(
        number_of_blocks_requested=15,
        block_to_process_in_this_run=12,
    )

