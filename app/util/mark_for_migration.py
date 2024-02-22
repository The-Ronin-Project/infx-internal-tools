import logging
from sqlalchemy import text

import app.enum.concept_maps_for_content
import app.enum.concept_maps_for_systems
import app.enum.terminologies_for_systems
import app.enum.value_sets_for_systems
import app.enum.value_sets_for_content

import app.models.data_ingestion_registry
import app.value_sets.models

from app.database import get_db


def reset_mark_to_move():
    conn = get_db()
    # Reset concept maps
    conn.execute(
        text(
            """
        update concept_maps.concept_map_version
        set migrate=false
        """
        )
    )

    # Reset value sets
    conn.execute(
        text(
            """
        update value_sets.value_set_version
        set migrate=false
        """
        )
    )

    # Reset custom terminology codes
    conn.execute(
        text(
            """
        update custom_terminologies.code
        set migrate=false
        """
        )
    )


def mark_concept_maps_for_migration(data_normalization_registry: app.models.data_ingestion_registry.DataNormalizationRegistry):
    # todo: add items from data normalization registry
    conn = get_db()

    # Iterate through concept maps designated for move
    concept_maps_for_systems = app.enum.concept_maps_for_systems.ConceptMapsForSystems
    concept_maps_for_content = app.enum.concept_maps_for_content.ConceptMapsForContent

    systems_uuids = [x.value for x in concept_maps_for_systems]
    content_uuids = [x.value for x in concept_maps_for_content]
    content_uuids = list(set(content_uuids))
    systems_uuids = list(set(systems_uuids))

    # For systems UUIDs, mark all versions to migrate
    for concept_map_uuid in systems_uuids:
        conn.execute(
            text(
                """
                update concept_maps.concept_map_version
                set migrate=true
                where concept_map_uuid=:concept_map_uuid
                """
            ),
            {"concept_map_uuid": concept_map_uuid},
        )

    # For Content UUIDs, mark active and pending versions; Also, perform some data integrity checks
    for concept_map_uuid in content_uuids:
        # Verify there is only one active version (if any)
        all_versions = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version
                where concept_map_uuid=:concept_map_uuid
                """
            ),
            {"concept_map_uuid": concept_map_uuid},
        ).fetchall()
        active_versions = [
            version for version in all_versions if version.status == "active"
        ]
        if len(active_versions) > 1:
            logging.warning(
                f"Concept Map has more than one active version. CM UUID: {concept_map_uuid}"
            )

        # Mark active version for move
        active_version = None
        active_version_uuid = None
        if active_versions:
            active_version = active_versions[0]
            active_version_uuid = active_version.uuid
            conn.execute(
                text(
                    """
                update concept_maps.concept_map_version
                set migrate=true
                where uuid=:version_uuid
                """
                ),
                {"version_uuid": active_version_uuid},
            )

        # Verify there are no pending versions older than the active version
        if active_version:
            pending_versions_older_than_active = conn.execute(
                text(
                    """
                    select * from concept_maps.concept_map_version
                    where concept_map_uuid=:concept_map_uuid
                    and status='pending'
                    and version < :active_version_version
                    """
                ),
                {
                    "concept_map_uuid": concept_map_uuid,
                    "active_version_version": active_version.version,
                },
            ).fetchall()
            if pending_versions_older_than_active:
                logging.warning(
                    f"Pending version older than active detected in CM UUID: {concept_map_uuid}"
                )

        # Mark the pending versions more recent than the active (or all pending versions if no active) for move
        if active_version:
            conn.execute(
                text(
                    """
                    update concept_maps.concept_map_version
                    set migrate=true
                    where concept_map_uuid=:concept_map_uuid
                    and status='pending'
                    and version > :active_version_version
                    """
                ),
                {
                    "concept_map_uuid": concept_map_uuid,
                    "active_version_version": active_version.version,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    update concept_maps.concept_map_version
                    set migrate=true
                    where concept_map_uuid=:concept_map_uuid
                    and status='pending'
                    """
                ),
                {"concept_map_uuid": concept_map_uuid},
            )

    # Mark everything registered in the data normalization registry
    concept_map_normalization_entries = [entry for entry in data_normalization_registry.entries if
                                         entry.registry_entry_type == 'concept_map']
    for entry in concept_map_normalization_entries:
        concept_map = entry.concept_map
        active_version = concept_map.most_recent_active_version
        version_uuid = active_version.uuid
        conn.execute(
            text(
                """
                update concept_maps.concept_map_version
                set migrate=true
                where uuid=:version_uuid
                """
            ), {
                "version_uuid": version_uuid
            }
        )


def mark_value_sets_for_migration(data_normalization_registry: app.models.data_ingestion_registry.DataNormalizationRegistry):
    conn = get_db()

    # Mark everything registered in the data normalization registry
    value_set_normalization_entries = [entry for entry in data_normalization_registry.entries if entry.registry_entry_type=='value_set']
    for entry in value_set_normalization_entries:
        value_set = entry.value_set
        active_version = value_set.load_most_recent_active_version(
                value_set.uuid
            )
        version_uuid = active_version.uuid
        conn.execute(
            text(
                """
                update value_sets.value_set_version
                set migrate=true
                where uuid=:version_uuid
                """
            ), {
                "version_uuid": version_uuid
            }
        )

    # Mark everything registered in an active flexible registry
    registry_value_sets = conn.execute(
        text(
            """
            select distinct value_set_uuid from flexible_registry.group_member
            """
        )
    ).fetchall()
    registry_value_set_uuids = [entry.value_set_uuid for entry in registry_value_sets]
    for value_set_uuid in registry_value_set_uuids:
        value_set_version = app.value_sets.models.ValueSet.load_most_recent_active_version(value_set_uuid)
        conn.execute(
            text(
                """
                update value_sets.value_set_version
                set migrate=true
                where uuid=:version_uuid
                """
            ), {
                "version_uuid": value_set_version.uuid
            }
        )

    # Mark everything needed for a marked concept map
    conn.execute(
        text(
            """
            update value_sets.value_set_version
            set migrate=true
            where uuid in 
            (select source_value_set_version_uuid from concept_maps.concept_map_version
            where migrate=true)
            """
        )
    )

    conn.execute(
        text(
            """
            update value_sets.value_set_version
            set migrate=true
            where uuid in 
            (select target_value_set_version_uuid from concept_maps.concept_map_version
            where migrate=true)
            """
        )
    )

    # Mark everything specifically added to the enum
    value_sets_for_content = app.enum.value_sets_for_content.ValueSetsForContent
    content_uuids = [x.value for x in value_sets_for_content]
    content_uuids = list(set(content_uuids))

    for value_set_uuid in content_uuids:
        conn.execute(
            text(
                """
                update value_sets.value_set_version
                set migrate=true
                where value_set_uuid=:value_set_uuid
                and status in ('pending', 'active')
                """
            ), {
                "value_set_uuid": value_set_uuid
            }
        )

    # todo: implement specific selection for systems if necessary


def mark_custom_terminology_codes_for_migration():
    conn = get_db()

    # Mark everything specifically added to the enum
    # todo: implement if necessary

    # Mark everything needed for a marked value set
    conn.execute(
        text(
            """
            update custom_terminologies.code
            set migrate=true
            where uuid in 
            (select distinct expansion_member.custom_terminology_uuid from value_sets.value_set_version vsv
            join value_sets.expansion 
            on vsv.uuid = expansion.vs_version_uuid
            join value_sets.expansion_member
            on expansion_member.expansion_uuid = expansion.uuid
            where vsv.migrate=true)
            """
        )
    )  # Individually mark codes for migration based on membership in value set via custom_terminology_code field

    conn.execute(
        text(
            """
            update custom_terminologies.code
            set migrate=true
            where terminology_version_uuid in 
            (select distinct tv.uuid from value_sets.value_set_version vsv
            join value_sets.expansion 
            on vsv.uuid = expansion.vs_version_uuid
            join value_sets.expansion_member
            on expansion_member.expansion_uuid = expansion.uuid
            join public.terminology_versions tv
            on tv.fhir_uri = expansion_member.system
            and tv.version = expansion_member.version
            where vsv.migrate=true)
            """
        )
    )  # Individually mark codes for migration based on membership in value set via system, version lookup

    # We don't want to move partial terminologies
    # So, this will identify any terminology version with data marked for migration
    # and then mark the entire terminology version for migration
    conn.execute(
        text(
            """
            UPDATE custom_terminologies.code
            SET migrate = true
            WHERE terminology_version_uuid IN (
                SELECT terminology_version_uuid
                FROM custom_terminologies.code
                WHERE migrate = true
                GROUP BY terminology_version_uuid
            )
            """
        )
    )

    # Finally, if a more recent version of a terminology exists than the one marked for migration, mark it too
    # For example, if MDA Observations v10 was marked, we'd want to move v11 and v12 if they exist

    conn.execute(
        text(
            """
            -- Step 1: Identify Codes to Migrate with their Versions and Terminology
            WITH CodesToMigrate AS (
                SELECT
                    ct.terminology_version_uuid,
                    tv.terminology,
                    CAST(tv.version AS FLOAT) AS version_marked
                FROM custom_terminologies.code ct
                JOIN public.terminology_versions tv ON ct.terminology_version_uuid = tv.uuid
                WHERE ct.migrate = true
                and tv.is_standard = false and tv.fhir_terminology=false
                and tv.version != 'N/A'
            ),
            
            -- Step 2: Find All More Recent Versions for Each Terminology
            MoreRecentVersions AS (
                SELECT
                    tv.uuid,
                    tv.terminology,
                    CAST(tv.version AS FLOAT) AS version
                FROM public.terminology_versions tv
                JOIN CodesToMigrate ctm ON tv.terminology = ctm.terminology
                WHERE CAST(tv.version AS FLOAT) > ctm.version_marked
                and tv.is_standard = false and tv.fhir_terminology=false
                and tv.version != 'N/A'
            )
            
            -- Step 3: Update Records for All More Recent Versions
            UPDATE custom_terminologies.code
            SET migrate = true
            WHERE terminology_version_uuid IN (SELECT uuid FROM MoreRecentVersions)
              AND migrate = false
            """
        )
    )


def generate_report_of_content_to_move():
    conn = get_db()

    # Drop and re-create the report table
    conn.execute(
        text(
            """
            DROP TABLE IF EXISTS custom_terminologies.migration_report
            """
        )
    )

    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS custom_terminologies.migration_report
            (
                terminology_version_uuid uuid NOT NULL,
                terminology_name character varying COLLATE pg_catalog."default",
                terminology_version character varying COLLATE pg_catalog."default",
                value_sets character varying COLLATE pg_catalog."default",
                normalization_registry_vs character varying COLLATE pg_catalog."default",
                normalization_registry_cm character varying COLLATE pg_catalog."default",
                flexible_registries character varying COLLATE pg_catalog."default",
                concept_maps character varying COLLATE pg_catalog."default",
                migrate bool,
                CONSTRAINT migration_report_pkey PRIMARY KEY (terminology_version_uuid)
            )
            
            TABLESPACE pg_default
            """
        )
    )

    conn.execute(
        text(
            """
            ALTER TABLE IF EXISTS custom_terminologies.migration_report
                OWNER to roninadmin
            """
        )
    )

    # Copy in all custom terminologies
    conn.execute(
        text(
            """
            insert into custom_terminologies.migration_report
            (terminology_name, terminology_version, terminology_version_uuid)
            select terminology, version, uuid from public.terminology_versions
            where uuid in 
            (select distinct(terminology_version_uuid)
            from custom_terminologies.code)
            """
        )
    )

    # Select those custom terminologies so we can iterate through them
    migration_report_empty = conn.execute(
        text(
            """
            select * from custom_terminologies.migration_report
            """
        )
    ).fetchall()

    for row in migration_report_empty:
        terminology_version_uuid = row.terminology_version_uuid

        # Mark whether those terminologies are moving or not
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set migrate = 
                (select distinct(migrate) from custom_terminologies.code
                where terminology_version_uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )

        # Mark impacted active and pending value sets
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set value_sets = 
                (select array_agg(distinct(concat(vs.title, ' version ', vsv.version))) from value_sets.expansion_member em
                join value_sets.expansion
                on expansion.uuid = em.expansion_uuid
                join value_sets.value_set_version vsv
                on vsv.uuid = expansion.vs_version_uuid
                and vsv.status in ('active', 'pending')
                join value_sets.value_set vs
                on vs.uuid = vsv.value_set_uuid
                join terminology_versions tv
                on tv.fhir_uri=em.system
                and tv.version=em.version
                where tv.uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )

        # Mark impacted data normalization registry entries for value sets
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set normalization_registry_vs = 
                (select array_agg(distinct registry.data_element) 
                from value_sets.expansion_member em
                join value_sets.expansion
                on expansion.uuid = em.expansion_uuid
                join value_sets.value_set_version vsv
                on vsv.uuid = expansion.vs_version_uuid
                and vsv.status in ('active', 'pending')
                join value_sets.value_set vs
                on vs.uuid = vsv.value_set_uuid
                join data_ingestion.registry
                on registry.value_set_uuid=vs.uuid
                join terminology_versions tv
                on tv.fhir_uri=em.system
                and tv.version=em.version
                where tv.uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )

        # Mark impacted data normalization registry entries for concept maps
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set normalization_registry_cm = 
                (select array_agg(distinct concat(cm.title, ' version ', cmv.version))
                from data_ingestion.registry dnr
                join concept_maps.concept_map cm
                on cm.uuid=dnr.concept_map_uuid
                join concept_maps.concept_map_version cmv
                on cmv.concept_map_uuid=cm.uuid
                and cmv.status in ('active', 'pending')
                join value_sets.value_set_version vsv
                on vsv.uuid=cmv.source_value_set_version_uuid
                or vsv.uuid=cmv.target_value_set_version_uuid
                join value_sets.expansion
                on expansion.vs_version_uuid=vsv.uuid
                join value_sets.expansion_member vsem
                on vsem.expansion_uuid=expansion.uuid
                join terminology_versions tv
                on tv.fhir_uri=vsem.system
                and tv.version=vsem.version
                and tv.uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )

        # Mark impacted flexible registries
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set flexible_registries = 
                (select array_agg(distinct registry.title) 
                from flexible_registry.group_member frgm
                join flexible_registry.group frg
                on frgm.group_uuid=frg.uuid
                join flexible_registry.registry
                on registry.uuid=frg.registry_uuid
                join value_sets.value_set vs
                on vs.uuid=frgm.value_set_uuid
                join value_sets.value_set_version vsv
                on vsv.value_set_uuid=vs.uuid
                join value_sets.expansion vse
                on vse.vs_version_uuid=vsv.uuid
                join value_sets.expansion_member em
                on em.expansion_uuid=vse.uuid
                join terminology_versions tv
                on em.system=tv.fhir_uri
                and em.version=tv.version
                where tv.uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )

        # Mark impacted concept maps
        conn.execute(
            text(
                """
                update custom_terminologies.migration_report
                set concept_maps = 
                (select array_agg(distinct concat(cm.title, ' version ', cmv.version))
                from concept_maps.concept_map cm
                join concept_maps.concept_map_version cmv
                on cmv.concept_map_uuid=cm.uuid
                and cmv.status in ('active', 'pending')
                join value_sets.value_set_version vsv
                on vsv.uuid=cmv.source_value_set_version_uuid
                or vsv.uuid=cmv.target_value_set_version_uuid
                join value_sets.expansion
                on expansion.vs_version_uuid=vsv.uuid
                join value_sets.expansion_member vsem
                on vsem.expansion_uuid=expansion.uuid
                join terminology_versions tv
                on tv.fhir_uri=vsem.system
                and tv.version=vsem.version
                and tv.uuid=:terminology_version_uuid)
                where terminology_version_uuid=:terminology_version_uuid
                """
            ), {
                "terminology_version_uuid": terminology_version_uuid
            }
        )


def get_latest_code_uuid_for_code(input_code_uuid):
    conn = get_db()

    select_query = text(
        """
        WITH TerminologyInfo AS (
            SELECT
                cv.terminology,
                cv.uuid AS terminology_version_uuid,
                c.code,
                c.display,
                c.depends_on_property,
                c.depends_on_system,
                c.depends_on_value,
                c.depends_on_display
            FROM custom_terminologies.code c
            JOIN public.terminology_versions cv ON c.terminology_version_uuid = cv.uuid
            WHERE c.uuid = :input_code_uuid -- Replace 'input_code_uuid' with the actual uuid
        ),
        LatestVersion AS (
            SELECT
                tv.terminology,
                MAX(tv.version::float) AS latest_version
            FROM public.terminology_versions tv
            JOIN TerminologyInfo ti ON tv.terminology = ti.terminology
        --     WHERE (tv.effective_start IS NULL OR tv.effective_start <= CURRENT_DATE)
        --       AND (tv.effective_end IS NULL OR tv.effective_end >= CURRENT_DATE)
            GROUP BY tv.terminology
        ),
        LatestTerminology AS (
            SELECT
                tv.uuid AS latest_terminology_version_uuid,
                lv.terminology,
                lv.latest_version
            FROM LatestVersion lv
            JOIN public.terminology_versions tv ON lv.terminology = tv.terminology
                AND lv.latest_version = tv.version::float
        )
        SELECT
            c.uuid AS new_code_uuid,
            c.code,
            c.display,
            c.depends_on_property,
            c.depends_on_system,
            c.depends_on_value,
            c.depends_on_display
        FROM custom_terminologies.code c
        JOIN LatestTerminology lt ON c.terminology_version_uuid = lt.latest_terminology_version_uuid
        JOIN TerminologyInfo ti ON c.code = ti.code
            AND c.display = ti.display
            AND c.depends_on_property = ti.depends_on_property
            AND c.depends_on_system = ti.depends_on_system
            AND c.depends_on_value = ti.depends_on_value
            AND c.depends_on_display = ti.depends_on_display
    """)

    result = conn.execute(
        select_query,
        {"input_code_uuid": input_code_uuid}
    ).one()
    return result.new_code_uuid


def unmark_duplicates():
    conn = get_db()

    # Select from table
    results = conn.execute(
        text(
            """
            select * from custom_terminologies.code_duplicate_action
            """
        )
    ).fetchall()

    # Iterate through each row and ummark the duplicate row
    for row in results:
        uuid_to_unmark = None
        if row.keep_orig:
            uuid_to_unmark = row.dup_uuid
        elif row.keep_dup:
            uuid_to_unmark = row.orig_uuid
        if not uuid_to_unmark:
            raise Exception(f"No row to unmark identified for row: {row.orig_uuid}, {row.dup_uuid}, {row.keep_orig}, {row.keep_dup}")

        latest_uuid = get_latest_code_uuid_for_code(uuid_to_unmark)
        if not latest_uuid:
            raise Exception(f"Translation failed for code uuid: {uuid_to_unmark}")

        conn.execute(
            text(
                """
                update custom_terminologies.code
                set migrate=false
                where uuid=:code_uuid
                """
            ), {
                "code_uuid": latest_uuid
            }
        )


def mark_content_for_migration():
    conn = get_db()

    data_normalization_registry = app.models.data_ingestion_registry.DataNormalizationRegistry()
    data_normalization_registry.load_entries()

    reset_mark_to_move()

    mark_concept_maps_for_migration(data_normalization_registry)
    mark_value_sets_for_migration(data_normalization_registry)
    mark_custom_terminology_codes_for_migration()

    generate_report_of_content_to_move()

    # Finally, identify specific duplicates which should NOT be moved and unmark them for moving
    unmark_duplicates()

    conn.commit()
    conn.close()


if __name__ == "__main__":
    mark_content_for_migration()
