import datetime
import uuid
from sqlalchemy import text

from app.database import get_db
from app.models.concept_maps import ConceptMap, ConceptMapVersion, Mapping
from app.models.codes import Code


class ConceptMapVersionCreator:
    def __init__(self):
        self.previous_concept_map_version = None
        self.new_concept_map_version = None
        self.conn = None

    def register_new_concept_map_version(
        self,
        new_version_description,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
    ):
        # Determine what the new version number should be
        new_version_num = self.previous_concept_map_version.version + 1
        self.conn.execute(
            text(
                """
                update 
                """
            )
        )
        # Register the new version in concept_maps.concept_map_version

    def new_version_from_previous(
        self,
        previous_version_uuid,
        new_version_description,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
    ):
        self.previous_concept_map_version = ConceptMapVersion(previous_version_uuid)

        # Open a persistent connection and begin a transaction
        self.conn = get_db()
        self.conn.execute(text("begin"))

        # Data integrity checks
        # todo: make sure the previous version is the latest version; we don't want to version anything but the latest ever

        # Register the new concept map version
        self.register_new_concept_map_version()

        self.populate_source_concepts()

        # Iterate through the new sources, compare w/ previous, make decisions:
        for source in new_sources:
            if ...:
                self.process_equivalent_mapping()
            elif ...:
                self.process_narrower_broader_mapping()
            elif ...:
                self.process_inactive_target_mappings()
            elif ...:
                self.process_no_map()
            # What if it was still pending, neither mapped nor no-mapped? (Copy over comments, hold for discussion, etc.)
            else:
                raise Exception("Don't know what to do w/ this...")

    def new_version_from_previous_deprecated(
        previous_version_uuid,
        new_version_description,
        new_version_num,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
    ):
        """
        Creates a new version of the concept map based on a previous version.

        Args:
            previous_version_uuid (str): The UUID of the previous version of the concept map.
            new_version_description (str): The description of the new version.
            new_version_num (int): The version number of the new version.
            new_source_value_set_version_uuid (str): The UUID of the version of the source value set to use for the new version.
            new_target_value_set_version_uuid (str): The UUID of the version of the target value set to use for the new version.
        """
        conn = get_db()
        new_version_uuid = uuid.uuid4()

        # # Lookup concept_map_uuid
        # concept_map_uuid = (
        #     conn.execute(
        #         text(
        #             """
        #         select * from concept_maps.concept_map_version
        #         where uuid=:previous_version_uuid
        #         """
        #         ),
        #         {"previous_version_uuid": previous_version_uuid},
        #     )
        #     .first()
        #     .concept_map_uuid
        # )

        # Add entry to concept_maps.concept_map_version
        conn.execute(
            text(
                """
                insert into concept_maps.concept_map_version
                (uuid, concept_map_uuid, description, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
                values
                (:new_version_uuid, :concept_map_uuid, :description, 'pending', now(), :version_num, :source_value_set_version_uuid, :target_value_set_version_uuid)
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                "concept_map_uuid": concept_map_uuid,
                "description": new_version_description,
                "version_num": new_version_num,
                "source_value_set_version_uuid": new_source_value_set_version_uuid,
                "target_value_set_version_uuid": new_target_value_set_version_uuid,
            },
        )
        # Populate concept_maps.source_concept
        conn.execute(
            text(
                """
                insert into concept_maps.source_concept
                (uuid, code, display, system, map_status, concept_map_version_uuid)
                select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid from value_sets.expansion_member
                join value_sets.expansion
                on expansion.uuid=expansion_member.expansion_uuid
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where vs_version_uuid=:new_source_value_set_version_uuid
                """
            ),
            {
                "new_source_value_set_version_uuid": new_source_value_set_version_uuid,
                "concept_map_version_uuid": new_version_uuid,
            },
        )

        # Load new target value set
        target_value_set_expansion = conn.execute(
            text(
                """
                select expansion_member.*, tv.uuid as terminology_uuid from value_sets.expansion_member
                join value_sets.expansion
                on expansion.uuid=expansion_member.expansion_uuid
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where vs_version_uuid=:vs_version_uuid
                """
            ),
            {"vs_version_uuid": new_target_value_set_version_uuid},
        )
        target_value_set_lookup = {
            (x.code, x.display, x.system): x for x in target_value_set_expansion
        }

        # Iterate through source_concepts in new version
        previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        exact_previous_mappings = previous_concept_map_version.mappings
        code_display_system_previous_mappings = {
            (key.code, key.display, key.system): value
            for key, value in exact_previous_mappings.items()
        }

        new_source_concepts = conn.execute(
            text(
                """
                select tv.fhir_uri, tv.version terminology_version, source_concept.uuid as source_concept_uuid, * from concept_maps.source_concept
                join public.terminology_versions tv
                on tv.uuid = cast(source_concept.system as uuid)
                where concept_map_version_uuid = :new_version_uuid
                """
            ),
            {"new_version_uuid": new_version_uuid},
        )
        for item in new_source_concepts:
            source_code = Code.load_concept_map_source_concept(item.source_concept_uuid)

            if (
                item.code,
                item.display,
                item.fhir_uri,
            ) in code_display_system_previous_mappings:
                mappings = code_display_system_previous_mappings[
                    (item.code, item.display, item.fhir_uri)
                ]

                for mapping in mappings:
                    target_code = mapping.target

                    # See if the target from the old mapping is in the new target value set or not
                    if (
                        target_code.code,
                        target_code.display,
                        target_code.system,
                    ) in target_value_set_lookup:
                        # A match was found, copy the mapping over
                        target_info = target_value_set_lookup.get(
                            (target_code.code, target_code.display, target_code.system)
                        )

                        target_code = Code(
                            code=target_info.code,
                            display=target_info.display,
                            system=None,
                            version=None,
                            terminology_version=target_info.terminology_uuid,
                        )

                        new_mapping = Mapping(
                            source=source_code,
                            relationship=mapping.relationship,
                            target=target_code,
                            mapping_comments=mapping.mapping_comments,
                            author=mapping.author,
                            review_status=mapping.review_status,
                        )
                        new_mapping.save()

        # Index the targets
        ConceptMap.index_targets(new_version_uuid, new_target_value_set_version_uuid)
