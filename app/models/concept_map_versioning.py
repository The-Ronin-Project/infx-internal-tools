import datetime
import uuid
from sqlalchemy import text

from app.database import get_db
from app.models.concept_maps import ConceptMap, ConceptMapVersion, Mapping, SourceConcept, MappingRelationship
from app.models.codes import Code
from app.models.terminologies import Terminology, load_terminology_version_with_cache


class ConceptMapVersionCreator:
    def __init__(self):
        self.previous_concept_map_version = None
        self.new_concept_map_version = None
        self.new_version_uuid = None
        self.conn = None

        self.new_source_value_set_version_uuid = None
        self.new_target_value_set_version_uuid = None

    def register_new_concept_map_version(
        self,
        new_version_description
    ):
        # Determine what the new version number should be
        new_version_num = self.previous_concept_map_version.version + 1
        # Register the new version in concept_maps.concept_map_version
        concept_map_uuid = self.previous_concept_map_version.concept_map.uuid
        self.conn.execute(
            text(
                """
                insert into concept_maps.concept_map_version
                (uuid, concept_map_uuid, description, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
                values
                (:new_version_uuid, :concept_map_uuid, :description, 'pending', now(), :version_num, :source_value_set_version_uuid, :target_value_set_version_uuid)
                """
            ),
            {
                "new_version_uuid": self.new_version_uuid,
                "concept_map_uuid": concept_map_uuid,
                "description": new_version_description,
                "version_num": new_version_num,
                "source_value_set_version_uuid": self.new_source_value_set_version_uuid,
                "target_value_set_version_uuid": self.new_target_value_set_version_uuid,
            },
        )

    def populate_source_concepts(self):
        # Populate all the sources with a status  of pending
        self.conn.execute(
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
                "new_source_value_set_version_uuid": self.new_source_value_set_version_uuid,
                "concept_map_version_uuid": self.new_version_uuid,
            },
        )

        # Retrieve and return new sources
        new_sources = self.conn.execute(
            text(
                """
                select * from concept_maps.source_concept
                where concept_map_version_uuid=:concept_map_version_uuid
                """
            ), {
                'concept_map_version_uuid': self.new_version_uuid
            }
        ).fetchall()
        return new_sources

    def load_all_sources_and_mappings(self, concept_map_version_uuid):
        all_data = self.conn.execute(
            text(
                """
                SELECT
                    sc.uuid AS source_concept_uuid,
                    sc.code AS source_concept_code,
                    sc.display AS source_concept_display,
                    sc.system AS source_concept_system,
                    sc.comments AS source_concept_comments,
                    sc.additional_context AS source_concept_additional_context,
                    sc.map_status AS source_concept_map_status,
                    sc.concept_map_version_uuid AS source_concept_map_version_uuid,
                    sc.assigned_mapper AS source_concept_assigned_mapper,
                    sc.assigned_reviewer AS source_concept_assigned_reviewer,
                    sc.no_map AS source_concept_no_map,
                    sc.reason_for_no_map AS source_concept_reason_for_no_map,
                    sc.mapping_group AS source_concept_mapping_group,
                    cr.uuid AS concept_relationship_uuid,
                    cr.target_concept_code,
                    cr.target_concept_display,
                    cr.target_concept_system,
                    cr.mapping_comments,
                    cr.review_status,
                    cr.created_date AS concept_relationship_created_date,
                    cr.reviewed_date AS concept_relationship_reviewed_date,
                    cr.author AS concept_relationship_author,
                    cr.relationship_code_uuid AS concept_relationship_relationship_code_uuid,
                    cr.target_concept_system_version_uuid,
                    cr.review_comment AS concept_relationship_review_comment,
                    cr.reviewed_by AS concept_relationship_reviewed_by
                FROM
                    concept_maps.source_concept sc
                LEFT JOIN
                    concept_maps.concept_relationship cr
                ON
                    sc.uuid = cr.source_concept_uuid
                WHERE
                    sc.concept_map_version_uuid = :concept_map_version_uuid
                """
            ), {
                'concept_map_version_uuid': concept_map_version_uuid
            }
        )

        # Create a dict that lets you lookup a source and get both it's source_concept data and mappings
        # For example:
        # response[
        #     (code, display, system) = {
        #                                 "source_concept": SourceConcept(),
        #                                 "mappings": [Mapping(), Mapping()]
        #                             }
        # ]

        response = {}

        for row in all_data:
            # In the database, source_concept.system is a terminology_version_uuid, but we want the FHIR URL
            source_system_terminology = load_terminology_version_with_cache(row.source_concept_system)

            source_concept = SourceConcept(
                uuid=row.source_concept_uuid,
                code=row.source_concept_code,
                display=row.source_concept_display,
                system=source_system_terminology,
                comments=row.source_concept_comments,
                additional_context=row.source_concept_additional_context,
                map_status=row.source_concept_map_status,
                assigned_mapper=row.source_concept_assigned_mapper,
                assigned_reviewer=row.source_concept_assigned_reviewer,
                no_map=row.source_concept_no_map,
                reason_for_no_map=row.source_concept_reason_for_no_map,
                mapping_group=row.source_concept_mapping_group
            )

            mapping = None

            # Check if the row has a mapping
            if row.target_concept_code is not None:
                mapping_relationship = MappingRelationship.load_by_code(row.concept_relationship_relationship_code_uuid)

                target_system = load_terminology_version_with_cache(row.target_concept_system_version_uuid)

                target_code = Code(
                        code=row.target_concept_code,
                        display=row.target_concept_display,
                        system=row.target_concept_system,
                        version=target_system.version
                    )

                mapping = Mapping(
                    uuid=row.concept_relationship_uuid,
                    source=source_concept.code_object,
                    relationship=mapping_relationship,
                    target=target_code,
                    mapping_comments=row.mapping_comments,
                    author=row.concept_relationship_author,
                    review_status=row.review_status,
                    created_date=row.concept_relationship_created_date,
                    reviewed_date=row.concept_relationship_reviewed_date,
                    review_comment=row.concept_relationship_review_comment,
                    reviewed_by=row.concept_relationship_reviewed_by,
                )

            lookup_key = (source_concept.code, source_concept.display, source_concept.system)

            if lookup_key not in response:
                response[lookup_key] = {
                    "source_concept": source_concept,
                    "mappings": []
                }

                if mapping is not None:
                    response[lookup_key]["mappings"].append(mapping)

            else:
                # Multiple rows for the same source means multiple mappings, so we need to append that
                response[lookup_key]["mappings"].append(mapping)

        return response

    def new_version_from_previous(
        self,
        previous_version_uuid,
        new_version_description,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
    ):
        # Set up our variables we need to work with
        self.previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        self.new_source_value_set_version_uuid = new_source_value_set_version_uuid
        self.new_target_value_set_version_uuid = new_target_value_set_version_uuid

        # Open a persistent connection and begin a transaction
        self.conn = get_db()
        self.conn.execute(text("begin"))

        # Data integrity checks
        # todo: make sure the previous version is the latest version; we don't want to version anything but the latest ever

        # todo: do we want to re-expand the value_sets being passed in? Or put some data integrity checks to ensure the expansion is already done?

        # Register the new concept map version
        self.new_version_uuid = uuid.uuid4()
        self.register_new_concept_map_version(
            new_version_description=new_version_description
        )

        # Populate the concept_maps.source_concept table with the latest expansion of the new target value set version
        new_source_concepts = self.populate_source_concepts()

        # Iterate through the new sources, compare w/ previous, make decisions:
        previous_sources_and_mappings = self.load_all_sources_and_mappings(self.previous_concept_map_version.uuid)


        for item in new_source_concepts:
            lookup_key = (item.code, item.display, item.system)

            if lookup_key not in previous_sources_and_mappings:
                # Handle the case of a new source w/ no related previous one
                pass

            previous_source_concept = item.get('source_concept')
            previous_mappings = item.get('mappings')

            # todo: handle updating source metadata
            # What if it was still pending, neither mapped nor no-mapped? (Copy over comments, hold for discussion, etc.)

            if not previous_mappings:
                # Handle no-maps
                self.process_no_map()

            else:
                for mapping in previous_mappings:

                    #todo: check if target is still active
                    target_is_active = True
                    if target_is_active is False:
                        self.process_inactive_target_mapping(mapping)
                    else:
                        if mapping.relationship.display == 'Equivalent':
                            self.process_equivalent_mapping(mapping)
                        else:
                            self.process_non_equivalent_mapping(mapping)

            """
            Somewhere in here, we need to handle updating:
            - comments
            - additional context (should this get copied forward?)
            - map status
            - assigned mapper
            - assigned reviewer
            - no map
            - reason for no map
            - mapping group
            """

    def process_inactive_target_mapping(self, mapping):
        # todo: Rey to implement
        pass

    def process_equivalent_mapping(self, mapping):
        # todo: Theresa to implement

        # use the Mapping class, MappingRelationship class, SourceConcept class, and Code class
        source_concept = SourceConcept()

        source_code = source_concept.code_object
        relationship = MappingRelationship()
        target_code = Code()

        new_mapping = Mapping(
            # todo: implement here
        )
        new_mapping.save()
        pass

    def process_non_equivalent_mapping(self, mapping):
        # todo: Jon to implement
        # write additional context (on the source 
        # revert mapping status to ready for review

        pass

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
        # conn.execute(
        #     text(
        #         """
        #         insert into concept_maps.concept_map_version
        #         (uuid, concept_map_uuid, description, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
        #         values
        #         (:new_version_uuid, :concept_map_uuid, :description, 'pending', now(), :version_num, :source_value_set_version_uuid, :target_value_set_version_uuid)
        #         """
        #     ),
        #     {
        #         "new_version_uuid": new_version_uuid,
        #         "concept_map_uuid": concept_map_uuid,
        #         "description": new_version_description,
        #         "version_num": new_version_num,
        #         "source_value_set_version_uuid": new_source_value_set_version_uuid,
        #         "target_value_set_version_uuid": new_target_value_set_version_uuid,
        #     },
        # )
        # Populate concept_maps.source_concept
        # conn.execute(
        #     text(
        #         """
        #         insert into concept_maps.source_concept
        #         (uuid, code, display, system, map_status, concept_map_version_uuid)
        #         select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid from value_sets.expansion_member
        #         join value_sets.expansion
        #         on expansion.uuid=expansion_member.expansion_uuid
        #         join public.terminology_versions tv
        #         on tv.fhir_uri=expansion_member.system
        #         and tv.version=expansion_member.version
        #         where vs_version_uuid=:new_source_value_set_version_uuid
        #         """
        #     ),
        #     {
        #         "new_source_value_set_version_uuid": new_source_value_set_version_uuid,
        #         "concept_map_version_uuid": new_version_uuid,
        #     },
        # )

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
