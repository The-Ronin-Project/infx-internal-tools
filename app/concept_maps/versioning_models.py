import datetime
import json
import uuid
from sqlalchemy import text

from app.database import get_db
from app.concept_maps.models import (
    ConceptMap,
    ConceptMapVersion,
    Mapping,
    SourceConcept,
    MappingRelationship,
)
from app.models.codes import Code
from app.terminologies.models import load_terminology_version_with_cache


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        This method is used to handle custom object types during the JSON encoding process.
        It defines how to convert datetime and UUID objects into JSON-compatible data types.

        If the object type is datetime.datetime or uuid.UUID, this method will convert it to
        a string representation. For other object types, the default JSON encoding method
        is called.

        Args:
            obj: An object of any type.

        Returns:
            A JSON-compatible representation of the object.
        """
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super(CustomJSONEncoder, self).default(obj)


class ConceptMapVersionCreator:
    def __init__(self):
        self.previous_concept_map_version = None
        self.new_concept_map_version = None
        self.new_version_uuid = None
        self.conn = None

        self.new_source_value_set_version_uuid = None
        self.new_target_value_set_version_uuid = None

    def register_new_concept_map_version(self, new_version_description):
        """
        Registers the new ConceptMapVersion in the database.

        This method inserts a new record into the concept_maps.concept_map_version table with the
        given values for the new ConceptMapVersion. The new_version_uuid, concept_map_uuid, and other
        fields are set using the provided data.

        Args:
            new_version_description (str): The description of the new ConceptMapVersion.
        """
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
        """
        Populates the concept_maps.source_concept table with the latest expansion of the new target value set version.

        This method inserts new records into the concept_maps.source_concept table by selecting the
        latest expansion_members from the value_sets.expansion_member table. The new source concepts
        are created with a status of 'pending'.

        Returns:
            list: A list of new SourceConcept instances.
        """
        # Populate all the sources with a status  of pending
        self.conn.execute(
            text(
                """
                insert into concept_maps.source_concept
                (uuid, code, display, system, map_status, concept_map_version_uuid, custom_terminology_uuid)
                select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid, custom_terminology_uuid from value_sets.expansion_member
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where expansion_uuid in (
                    select uuid from value_sets.expansion
                    where vs_version_uuid=:new_source_value_set_version_uuid
                    order by timestamp desc
                    limit 1
                )
                """
            ),
            {
                "new_source_value_set_version_uuid": self.new_source_value_set_version_uuid,
                "concept_map_version_uuid": self.new_version_uuid,
            },
        )

        sources_and_mappings = self.load_all_sources_and_mappings(self.new_version_uuid)

        new_sources = []
        for lookup_key, source_and_mapping in sources_and_mappings.items():
            new_sources.append(source_and_mapping.get("source_concept"))

        return new_sources

    def load_all_sources_and_mappings(self, concept_map_version_uuid):
        """
        Loads all source concepts and their mappings for a given ConceptMapVersion.

        This method queries the database to get all source concepts and their associated mappings
        for the given ConceptMapVersion. It iterates through the query results and organizes the
        data into a dictionary where the key is a tuple (code, display, system) and the value is
        a dictionary containing the source concept and a list of its mappings.

        Args:
            concept_map_version_uuid (uuid.UUID): The UUID of the ConceptMapVersion.

        Returns:
            dict: A dictionary containing source concepts and their mappings.
        """
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
            ),
            {"concept_map_version_uuid": concept_map_version_uuid},
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
            source_system_terminology = load_terminology_version_with_cache(
                row.source_concept_system
            )

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
                mapping_group=row.source_concept_mapping_group,
                concept_map_version_uuid=row.source_concept_map_version_uuid
            )

            mapping = None

            # Check if the row has a mapping
            if row.target_concept_code is not None:
                mapping_relationship = MappingRelationship.load_by_uuid(
                    row.concept_relationship_relationship_code_uuid
                )

                target_system = load_terminology_version_with_cache(
                    row.target_concept_system_version_uuid
                )

                target_code = Code(
                    code=row.target_concept_code,
                    display=row.target_concept_display,
                    system=None,
                    version=None,
                    terminology_version_uuid=row.target_concept_system_version_uuid,
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

            lookup_key = (
                source_concept.code,
                source_concept.display,
                source_concept.system.fhir_uri,
            )

            if lookup_key not in response:
                response[lookup_key] = {
                    "source_concept": source_concept,
                    "mappings": [],
                }

                if mapping is not None:
                    response[lookup_key]["mappings"].append(mapping)

            else:
                # Multiple rows for the same source means multiple mappings, so we need to append that
                response[lookup_key]["mappings"].append(mapping)

        return response

    def load_all_targets(self):
        """
        Loads all target concepts from the new target value set version.

        This method queries the database to get all target concepts from the latest expansion
        of the new target value set version. The results are organized into a dictionary where
        the key is a tuple (code, display, system) and the value is a Code instance.

        Returns:
            dict: A dictionary containing target concepts.
        """
        target_value_set_expansion = self.conn.execute(
            text(
                """
                select expansion_member.*, tv.uuid as terminology_uuid from value_sets.expansion_member
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where expansion_uuid in (
                    select uuid from value_sets.expansion
                    where vs_version_uuid=:vs_version_uuid
                    order by timestamp desc
                    limit 1
                )
                """
            ),
            {"vs_version_uuid": self.new_target_value_set_version_uuid},
        )
        target_value_set_lookup = {
            (x.code, x.display, x.system): Code(
                code=x.code, display=x.display, system=x.system, version=x.version
            )
            for x in target_value_set_expansion
        }
        return target_value_set_lookup

    def new_version_from_previous(
        self,
        previous_version_uuid: uuid.UUID,
        new_version_description: str,
        new_source_value_set_version_uuid: uuid.UUID,
        new_target_value_set_version_uuid: uuid.UUID,
        require_review_for_non_equivalent_relationships: bool,
        require_review_no_maps_not_in_target: bool,
    ):
        """
        Creates a new ConceptMapVersion based on the previous version.

        This method performs the following steps to create the new ConceptMapVersion:
        1. Set up variables and open a persistent database connection.
        2. Register the new ConceptMapVersion in the database.
        3. Populate the source concepts with the latest expansion of the new target value set version.
        4. Iterate through the new source concepts and compare them with the previous version.
        5. Process no-maps and mappings with inactive targets, and copy mappings exactly or require review.
        6. Save the new ConceptMapVersion.

        Args:
            previous_version_uuid (uuid.UUID): The UUID of the previous ConceptMapVersion.
            new_version_description (str): The description of the new ConceptMapVersion.
            new_source_value_set_version_uuid (uuid.UUID): The UUID of the new source value set version.
            new_target_value_set_version_uuid (uuid.UUID): The UUID of the new target value set version.
            require_review_for_non_equivalent_relationships (bool): Whether to require review for non-equivalent relationships.
            require_review_no_maps_not_in_target (bool): Whether to require review for no-maps not in the target.
        """
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
        previous_sources_and_mappings = self.load_all_sources_and_mappings(
            self.previous_concept_map_version.uuid
        )

        # Load and index the new targets
        new_targets_lookup = self.load_all_targets()
        ConceptMap.index_targets(
            self.new_version_uuid, new_target_value_set_version_uuid
        )
        previous_contexts_list = []

        for new_source_concept in new_source_concepts:
            source_lookup_key = (
                new_source_concept.code,
                new_source_concept.display,
                new_source_concept.system.fhir_uri,
            )

            if source_lookup_key not in previous_sources_and_mappings:
                # Handle the case of a new source w/ no related previous one
                pass

            else:
                previous_source_concept = previous_sources_and_mappings[
                    source_lookup_key
                ].get("source_concept")
                previous_mappings = previous_sources_and_mappings[
                    source_lookup_key
                ].get("mappings")

                # Some parts of source concept should always carry forward, regardless
                new_source_concept.update(
                    conn=self.conn,
                    comments=previous_source_concept.comments,
                    additional_context=previous_source_concept.additional_context,
                    map_status=previous_source_concept.map_status,
                    assigned_mapper=previous_source_concept.assigned_mapper,
                    assigned_reviewer=previous_source_concept.assigned_mapper,
                    no_map=previous_source_concept.no_map,
                    reason_for_no_map=previous_source_concept.reason_for_no_map,
                    mapping_group=previous_source_concept.mapping_group,
                )

                if not previous_mappings:
                    # Handle no-maps
                    result = self.process_no_map(
                        previous_source_concept,
                        new_source_concept,
                        require_review_no_maps_not_in_target,
                        previous_contexts_list,
                    )
                    if result is not None:
                        previous_contexts_list.append(result)

                else:
                    previous_mapping_context = []
                    for previous_mapping in previous_mappings:
                        target_lookup_key = (
                            previous_mapping.target.code,
                            previous_mapping.target.display,
                            previous_mapping.target.system,
                        )

                        if target_lookup_key not in new_targets_lookup:
                            # Append previous context to list in case multiple mappings which need to save it
                            previous_context_for_row = self.process_inactive_target_mapping(
                                new_source_concept=new_source_concept,
                                previous_mapping=previous_mapping,
                            )
                            previous_mapping_context.append(previous_context_for_row)
                        else:
                            new_target_concept = new_targets_lookup[target_lookup_key]

                            if previous_mapping.relationship.display == "Equivalent":
                                self.copy_mapping_exact(
                                    new_source_concept=new_source_concept,
                                    new_target_code=new_target_concept,
                                    previous_mapping=previous_mapping,
                                )
                            else:
                                if require_review_for_non_equivalent_relationships:
                                    self.copy_mapping_require_review(
                                        new_source_concept=new_source_concept,
                                        new_target_concept=new_target_concept,
                                        previous_mapping=previous_mapping,
                                    )
                                else:
                                    self.copy_mapping_exact(
                                        new_source_concept=new_source_concept,
                                        new_target_code=new_target_concept,
                                        previous_mapping=previous_mapping,
                                    )
                    if previous_mapping_context:
                        # If previous context needs to be written to the source, do it after the loop so we have it all
                        new_source_concept.update(
                            conn=self.conn,
                            previous_version_context=json.dumps(
                                previous_mapping_context, cls=CustomJSONEncoder
                            )
                        )

        # self.conn.execute(text("rollback"))

    def process_no_map(
        self,
        previous_source_concept: SourceConcept,
        new_source_concept: SourceConcept,
        require_review_no_maps_not_in_target: bool,
        previous_contexts_list: list,
    ):
        """
        Processes a source concept with explicit no maps from the previous version.

        This method checks if a review is required for no-maps not in the target.
        If a review is required, it sets the map_status of the new source concept to 'pending'
        and adds the previous context to the previous_contexts_list. If not, it takes no action.

        Args:
            previous_source_concept (SourceConcept): The previous source concept.
            new_source_concept (SourceConcept): The new source concept.
            require_review_no_maps_not_in_target (bool): Whether to require review for no-maps not in the target.
            previous_contexts_list (list): A list of previous contexts.

        Returns:
            dict: A dictionary containing the previous context if a review is required, otherwise None.
        """
        if (
            require_review_no_maps_not_in_target
            and previous_source_concept.reason_for_no_map == "Not in target code system"
        ):
            # Set map_status back to 'no map' so the user reviews whether a no-map is still appropriate
            previous_context = {
                "reason": "previous no-map",
                "no_map_reason": previous_source_concept.reason_for_no_map,
                "comments": previous_source_concept.comments,
                "assigned_mapper": previous_source_concept.assigned_mapper,
                "assigned_reviewer": previous_source_concept.assigned_reviewer,
                "map_status": previous_source_concept.map_status,
            }
            new_source_concept.update(
                conn=self.conn,
                previous_version_context=json.dumps(
                    previous_contexts_list, cls=CustomJSONEncoder
                ),
                map_status="no map",
            )
            return previous_context
        return None
        # else:
        #     # Copy over everything w/ no changes
        #     new_source_concept.update(
        #         conn=self.conn,
        #         no_map=previous_source_concept.no_map,
        #         comments=previous_source_concept.comments,
        #         additional_context=previous_source_concept.additional_context,
        #         map_status=previous_source_concept.map_status,
        #         assigned_mapper=previous_source_concept.assigned_mapper,
        #         assigned_reviewer=previous_source_concept.assigned_reviewer,
        #         reason_for_no_map=previous_source_concept.reason_for_no_map,
        #         mapping_group=previous_source_concept.mapping_group,
        #     )

    def process_inactive_target_mapping(
        self, new_source_concept: SourceConcept, previous_mapping: Mapping
    ):
        """
        Processes a mapping with an inactive target.

        This method saves the previous mapping information to the previous_mapping_context
        and updates the new source concept with the previous mapping context and a map_status
        of 'pending'.

        Args:
            new_source_concept (SourceConcept): The new source concept.
            previous_mapping (Mapping): The previous mapping.
        """

        # Save previous mapping info to previous mapping context
        previous_mapping_context = {
            "reason": "previous target no longer in target value set",
            "source_code": previous_mapping.source.code,
            "source_display": previous_mapping.source.display,
            "relationship": previous_mapping.relationship.display,
            "target_code": previous_mapping.target.code,
            "target_display": previous_mapping.target.display,
            "mapping_comments": previous_mapping.mapping_comments,
            "author": previous_mapping.author,
            "review_status": previous_mapping.review_status,
            "created_date": previous_mapping.created_date,
            "reviewed_date": previous_mapping.created_date,
            "review_comment": previous_mapping.review_comment,
            "reviewed_by": previous_mapping.reviewed_by,
        }
        new_source_concept.update(
            conn=self.conn,
            map_status="pending",
        )
        return previous_mapping_context

    def copy_mapping_exact(
        self,
        new_source_concept: SourceConcept,
        new_target_code: Code,
        previous_mapping: Mapping,
    ):
        """
        Copies a mapping exactly from the previous version to the new version.

        This method creates a new Mapping instance with the same values as the previous mapping,
        and saves the new mapping to the database.

        Args:
            new_source_concept (SourceConcept): The new source concept.
            new_target_code (Code): The new target code.
            previous_mapping (Mapping): The previous mapping.
        """
        source_code = new_source_concept.code_object

        relationship = previous_mapping.relationship

        target_code = new_target_code

        new_mapping = Mapping(
            source=source_code,
            relationship=relationship,
            target=target_code,
            mapping_comments=previous_mapping.mapping_comments,
            author=previous_mapping.author,
            review_status=previous_mapping.review_status,
            created_date=previous_mapping.created_date,
            reviewed_date=previous_mapping.reviewed_date,
            review_comment=previous_mapping.review_comment,
            reviewed_by=previous_mapping.reviewed_by,
        )

        new_mapping.save()

    def copy_mapping_require_review(
        self,
        new_source_concept: SourceConcept,
        new_target_concept: Code,
        previous_mapping: Mapping,
    ):
        """
        Copies a mapping from the previous version to the new version and sets its review status to "ready for review".

        This method creates a new Mapping instance with the same values as the previous mapping,
        except for the review_status which is set to "ready for review", and saves the new mapping
        to the database.

        Args:
            new_source_concept (SourceConcept): The new source concept.
            new_target_concept (Code): The new target code.
            previous_mapping (Mapping): The previous mapping.
        """
        source_code = new_source_concept.code_object

        relationship = previous_mapping.relationship

        target_code = new_target_concept

        # Explicitly over-write the review status to set it back to needing review
        new_mapping = Mapping(
            source=source_code,
            relationship=relationship,
            target=target_code,
            mapping_comments=previous_mapping.mapping_comments,
            author=previous_mapping.author,
            review_status="ready for review",
            created_date=previous_mapping.created_date,
            review_comment=previous_mapping.review_comment,  # todo: how to handle this
        )

        new_mapping.save()