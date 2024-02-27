import datetime
import json
import uuid
from typing import Dict, List
import logging

from sqlalchemy import text

from app.database import get_db
# import app.concept_maps.models
import app.value_sets.models
from app.errors import BadRequestWithCode, NotFoundException
from app.models.codes import Code
from app.terminologies.models import load_terminology_version_with_cache
from app.concept_maps.models import ConceptMap, ConceptMapVersion, SourceConcept, Mapping

LOGGER = logging.getLogger()

# Only to be used for local development; do not commit with this on
# LOGGER.setLevel("DEBUG")


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

        # These are sources that are present for the first time in the new version
        self.novel_sources = []

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
        Populates the concept_maps.source_concept_data table with the latest expansion of the new target value set version.

        This method inserts new records into the concept_maps.source_concept_data table by selecting the
        latest expansion_members from the value_sets.expansion_member_data table. The new source concepts
        are created with a status of 'pending'.

        Returns:
            list: A list of new SourceConcept instances.
        """
        # Populate all the sources with a status of pending
        # Todo: Columns need to be renamed for expansion_member_data table
        self.conn.execute(
            text(
                """
                insert into concept_maps.source_concept_data
                (uuid, code_schema, code_simple, code_jsonb, display, system_uuid, map_status, concept_map_version_uuid, custom_terminology_code_uuid)
                select uuid_generate_v4(), code_schema, code_simple, code_jsonb, display, tv.uuid, 'pending', :concept_map_version_uuid, custom_terminology_uuid from value_sets.expansion_member_data
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member_data.system
                and tv.version=expansion_member_data.version
                where expansion_uuid in (
                    select uuid from value_sets.expansion_member_data
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

        mapped_no_map_lookup, all_mappings_lookup = self.load_all_sources_and_mappings(
            self.new_version_uuid
        )

        # Extract new source concepts from both dictionaries
        new_sources = []
        for (
            lookup_key,
            source_and_mapping,
        ) in all_mappings_lookup.items():
            new_sources.append(source_and_mapping.get("source_concept"))

        for (
            lookup_key,
            source_and_mapping,
        ) in mapped_no_map_lookup.items():
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
        NO_MAP_RELATIONSHIP_CODE_UUID = uuid.UUID("dca7c556-82d9-4433-8971-0b7edb9c9661")  # Narrower Than
        NO_MAP_TARGET_CODE = "No map"
        NO_MAP_TARGET_DISPLAY = "No matching concept"

        # todo: in the future, we would like to identify if something is no map by comparing the target code
        # todo: to this no map code. However, we're not sure all the no map data is consistent, so for now
        # todo: we will simply check code and display
        # no_map_code = app.models.codes.Code(
        #     system=None,
        #     version=None,
        #     code="No map",
        #     display="No matching concept",
        #     terminology_version_uuid="None", 93ec9286-17cf-4837-a4dc-218ce3015de6
        #     from_custom_terminology=True,
        #     custom_terminology_code_id=e566fc0a3fe4cd1aaee97c2685c12c37,
        #     custom_terminology_code_uuid=3c233069-c5db-48ec-8d5a-6b87b24d8797,
        #     stored_custom_terminology_deduplication_hash=e566fc0a3fe4cd1aaee97c2685c12c37
        # ) todo: or call load by uuid to load this

        mapped_no_map: Dict[SourceConcept, List[Mapping]] = dict()
        real_mappings: Dict[SourceConcept, List[Mapping]] = dict()  # This excludes no maps

        # 1. Load the data
        all_mappings = app.concept_maps.models.ConceptMapVersion.load_mappings(
            concept_map_version_uuid=concept_map_version_uuid
        )

        # 2. For each row, flag if it is a no map
        for source_concept, mappings in all_mappings.items():
            for mapping in mappings:
                if mapping.target.code.code == NO_MAP_TARGET_CODE and \
                        mapping.target.code.display == NO_MAP_TARGET_DISPLAY and \
                        mapping.relationship.uuid == NO_MAP_RELATIONSHIP_CODE_UUID:
                    is_mapped_no_map = True
                else:
                    is_mapped_no_map = False

                if is_mapped_no_map:
                    if source_concept not in mapped_no_map:
                        mapped_no_map[source_concept] = [mapping]
                    else:
                        mapped_no_map[source_concept].append(mapping)

                else:  # Not a mapped not map, add to regular dict
                    if source_concept not in real_mappings:
                        real_mappings[source_concept] = [mapping]
                    else:
                        real_mappings[source_concept].append(mapping)

        return mapped_no_map, real_mappings

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
                select expansion_member.*, tv.uuid as terminology_uuid from value_sets.expansion_member_data
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member_data.system
                and tv.version=expansion_member_data.version
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

    def validate_inputs_for_new_version_from_previous(
            self,
            previous_version_uuid: uuid.UUID,
            new_source_value_set_version_uuid: uuid.UUID,
            new_target_value_set_version_uuid: uuid.UUID,
    ):
        # Data integrity checks
        # Make sure the previous version is the latest version; we don't want to version anything but the latest ever
        input_previous_concept_map_version = ConceptMapVersion(previous_version_uuid)

        if not input_previous_concept_map_version.is_latest_version():
            raise BadRequestWithCode(
                "ConceptMap.create_new_from_previous.previous_version_uuid",
                f"Input concept map version with UUID {previous_version_uuid} is not the most recent version",
            )

        # Validate the input new_source_value_set_version_uuid refers to the active version
        source_value_set_version = app.value_sets.models.ValueSetVersion.load(new_source_value_set_version_uuid)
        active_source_value_set_version = (
            app.value_sets.models.ValueSet.load_most_recent_active_version(
                source_value_set_version.value_set.uuid
            )
        )
        if active_source_value_set_version is None or (
                str(active_source_value_set_version.uuid)
                != str(new_source_value_set_version_uuid)
        ):
            raise BadRequestWithCode(
                "ConceptMap.create_new_from_previous.new_source_value_set_version_uuid",
                f"Input source value set with UUID {new_source_value_set_version_uuid} is not the most recent active",
            )

        # Validate the input new_target_value_set_version_uuid refers to the active version
        target_value_set_version = app.value_sets.models.ValueSetVersion.load(
            new_target_value_set_version_uuid
        )
        active_target_value_set_version = (
            app.value_sets.models.ValueSet.load_most_recent_active_version(
                target_value_set_version.value_set.uuid
            )
        )
        if active_target_value_set_version is None or (
                str(active_target_value_set_version.uuid)
                != str(new_target_value_set_version_uuid)
        ):
            raise BadRequestWithCode(
                "ConceptMap.create_new_from_previous.new_target_value_set_version_uuid",
                f"Input target value set with UUID {new_target_value_set_version_uuid} is not the most recent active",
            )

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
        1. Perform data integrity checks on the input parameters.
        2. Set up variables and open a persistent database connection.
        3. Register the new ConceptMapVersion in the database.
        4. Populate the source concepts with the latest expansion of the new source value set version.
        5. Iterate through the new source concepts and compare them with the previous version.
        6. Handle the cases of new sources, mapped no maps, mappings with inactive targets, and copy mappings exactly or require review.
        7. Save the new ConceptMapVersion.

        Args:
            previous_version_uuid (uuid.UUID): The UUID of the previous ConceptMapVersion.
            Callers must input the UUID of the most recent concept map version, regardless of version status.
            new_version_description (str): The description of the new ConceptMapVersion.
            new_source_value_set_version_uuid (uuid.UUID): The UUID of the new source value set version.
            Callers must input the UUID of the most recent active source value set version.
            new_target_value_set_version_uuid (uuid.UUID): The UUID of the new target value set version.
            Callers must input the UUID of the most recent active target value set version.
            require_review_for_non_equivalent_relationships (bool): Whether to require review for non-equivalent relationships.
            require_review_no_maps_not_in_target (bool): Whether to require review for no-maps not in the target.
        """
        self.validate_inputs_for_new_version_from_previous(
            previous_version_uuid=previous_version_uuid,
            new_source_value_set_version_uuid=new_source_value_set_version_uuid,
            new_target_value_set_version_uuid=new_target_value_set_version_uuid,
        )

        # todo: dated April 2023: do we want to re-expand the value_sets being passed in? Or put some data integrity checks to ensure the expansion is already done?

        # Mapped no map constants
        no_map_target_concept_code = "No map"
        no_map_target_concept_display = "No matching concept"
        no_map_system = "http://projectronin.io/fhir/CodeSystem/ronin/nomap"
        no_map_version = "1.0"

        # Create new version from previous
        try:
            # Set up our variables we need to work with
            self.previous_concept_map_version = (
                app.concept_maps.models.ConceptMapVersion(previous_version_uuid)
            )
            self.new_source_value_set_version_uuid = new_source_value_set_version_uuid
            self.new_target_value_set_version_uuid = new_target_value_set_version_uuid

            # Open a persistent connection and begin a transaction
            self.conn = get_db()
            self.conn.execute(text("begin"))

            # Register the new concept map version
            self.new_version_uuid = uuid.uuid4()
            self.register_new_concept_map_version(
                new_version_description=new_version_description
            )

            # Populate concept_maps.source_concept_data table with the latest expansion of the new source value set version
            # Note: populate_source_concepts() calls load_all_sources_and_mappings() with the self.new_version_uuid.
            # This first time call to load_all_sources_and_mappings will have none in all the concept relationship columns.
            new_source_concepts = self.populate_source_concepts()

            # Iterate through the new sources, compare w/ previous, make decisions:
            # This second call to load_all_sources_and_mappings should HAVE values in the concept relationship columns.
            (
                mapped_no_map_lookup,
                all_mappings_lookup,
            ) = self.load_all_sources_and_mappings(
                self.previous_concept_map_version.uuid
            )
            all_previous_sources_and_mappings = {
                **mapped_no_map_lookup,
                **all_mappings_lookup,
            }

            # Load and index the new targets
            new_targets_lookup = self.load_all_targets()
            app.concept_maps.models.ConceptMap.index_targets(
                self.new_version_uuid, new_target_value_set_version_uuid
            )
            previous_contexts_list = []

            for new_source_concept in new_source_concepts:
                # For each new_source_concept, create a source_lookup_key tuple using the source concept's properties
                # This is the lookup key for new source concepts, so it can compare them against the lookup_key within
                # the load_all_sources_and_mappings method
                source_lookup_key = (
                    new_source_concept.code_simple,
                    new_source_concept.code_jsonb,
                    new_source_concept.code_schema,
                    new_source_concept.display,
                    new_source_concept.system.fhir_uri,
                    new_source_concept.depends_on_display if new_source_concept.depends_on else None,
                    new_source_concept.depends_on_property if new_source_concept.depends_on else None,
                    new_source_concept.depends_on_system if new_source_concept.depends_on else None,
                    new_source_concept.depends_on_value if new_source_concept.depends_on else None,
                )

                if source_lookup_key not in all_previous_sources_and_mappings:
                    # If the source_lookup_key is not found in previous_sources_and_mappings
                    # (i.e. the source concept is new and not present in the previous version),
                    # add the new_source_concept to the novel_sources list.
                    self.novel_sources.append(new_source_concept)

                else:
                    # If the source_lookup_key is found in previous_sources_and_mappings
                    # Retrieve the previous_source_concept and its previous_mappings from previous_sources_and_mappings using the source_lookup_key
                    previous_source_concept = all_previous_sources_and_mappings[
                        source_lookup_key
                    ].get("source_concept")
                    previous_mappings = all_previous_sources_and_mappings[
                        source_lookup_key
                    ].get("mappings")

                    # Some parts of source concept should always carry forward, regardless
                    new_source_concept.update(
                        comments=previous_source_concept.comments,
                        additional_context=previous_source_concept.additional_context,
                        map_status=previous_source_concept.map_status,
                        assigned_mapper=previous_source_concept.assigned_mapper,
                        assigned_reviewer=previous_source_concept.assigned_reviewer,
                        no_map=previous_source_concept.no_map,
                        reason_for_no_map=previous_source_concept.reason_for_no_map,
                        mapping_group=previous_source_concept.mapping_group,
                    )

                    if source_lookup_key in mapped_no_map_lookup:
                        # If the source_lookup_key is found in mapped_no_map_lookup, handle the mapped_no_maps case:
                        # a. Retrieve the previous_mapping_data from mapped_no_map_lookup using the source_lookup_key
                        previous_mapping_data = mapped_no_map_lookup[source_lookup_key]
                        # b. get the previous_mapping
                        mapping = previous_mapping_data["mappings"][0]
                        previous_mapping = mapping
                        no_map_target_concept = app.models.codes.Code(
                            code=previous_mapping.target.code,
                            display=previous_mapping.target.display,
                            system=previous_mapping.target.system,
                            version=no_map_version
                        )

                        if require_review_for_non_equivalent_relationships:
                            # c. check for require_review_for_non_equivalent_relationships is True
                            self.copy_mapping_require_review(
                                new_source_concept=new_source_concept,
                                new_target_concept=no_map_target_concept,
                                previous_mapping=previous_mapping,
                            )

                        if (
                            require_review_no_maps_not_in_target
                            and previous_source_concept.reason_for_no_map
                            == "Not in target code system"
                        ):
                            # d. check for require_review is True and reason "Not in target code system"
                            result = self.process_no_map(
                                previous_source_concept,
                                new_source_concept,
                                require_review_no_maps_not_in_target,
                                previous_contexts_list,
                            )
                            if result is not None:
                                previous_contexts_list.append(result)

                        if (
                            not require_review_for_non_equivalent_relationships
                            and not require_review_no_maps_not_in_target
                        ):
                            # e. Otherwise, copy the previous mapping exactly using the copy_mapping_exact method with the new_target_code set to "No map"
                            no_map_code = app.models.codes.Code(
                                code=no_map_target_concept_code,
                                display=no_map_target_concept_display,
                                system=no_map_system,
                                version=no_map_version,
                                saved_to_db=True
                            )

                            self.copy_mapping_exact(
                                new_source_concept=new_source_concept,
                                new_target_code=no_map_code,
                                previous_mapping=previous_mapping,
                            )

                    else:
                        # If none of the above conditions match, it means the source concept has regular mappings in the previous version:
                        # a. Initialize an empty list previous_mapping_context to store the previous context data for the source concept.
                        previous_mapping_context = []
                        for previous_mapping in previous_mappings:
                            #  b. Iterate through the previous_mappings:
                            # i. Create a target_lookup_key tuple using the target concept's properties (code, display, system) from the previous_mapping.
                            target_lookup_key = (
                                previous_mapping.target.code,
                                previous_mapping.target.display,
                                previous_mapping.target.system,
                            )

                            if target_lookup_key not in new_targets_lookup:
                                # ii. If the target_lookup_key is not found in new_targets_lookup, it means the target concept is inactive.
                                # Process the inactive target mapping using the process_inactive_target_mapping method
                                # and append the result to the previous_mapping_context.

                                previous_context_for_row = (
                                    self.process_inactive_target_mapping(
                                        new_source_concept=new_source_concept,
                                        previous_mapping=previous_mapping,
                                    )
                                )
                                previous_mapping_context.append(
                                    previous_context_for_row
                                )
                            else:
                                # iii. If the target_lookup_key is found in new_targets_lookup, retrieve the new_target_concept from the new_targets_lookup
                                # Then, based on the relationship display and the require_review_for_non_equivalent_relationships parameter,
                                # either copy the previous mapping exactly using copy_mapping_exact, or require review and use copy_mapping_require_review.
                                new_target_concept = new_targets_lookup[
                                    target_lookup_key
                                ]

                                if (
                                    previous_mapping.relationship.display
                                    == "Equivalent"
                                ):
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
                        # After iterating through the previous_mappings, if there's any data in the previous_mapping_context,
                        # update the new_source_concept with the previous version context by converting the previous_mapping_context to a JSON string.
                        if previous_mapping_context:
                            new_source_concept.update(
                                previous_version_context=json.dumps(
                                    previous_mapping_context, cls=CustomJSONEncoder
                                ),
                            )

        except BadRequestWithCode or NotFoundException as e:
            LOGGER.info(
                f"create_new_from_previous missing data in concept map UUID {input_previous_concept_map_version.concept_map.uuid} version UUID {input_previous_concept_map_version.uuid}"
            )
            self.conn.rollback()
            raise e

        except Exception as e:  # uncaught exceptions can be so costly here, that a 'bare except' is acceptable, despite PEP 8: E722

            LOGGER.info(
                f"create_new_from_previous unexpected error with concept map UUID {input_previous_concept_map_version.concept_map.uuid} version UUID {input_previous_concept_map_version.uuid}"
            )
            self.conn.rollback()
            raise e

        # Return the new concept map version UUID
        return self.new_version_uuid

    def create_no_map_mappings(self, new_concept_map_version):
        """
        Creates mappings for source concepts where no_map=True.
        """
        no_map_relationship_uuid = "dca7c556-82d9-4433-8971-0b7edb9c9661"
        no_map_target_concept_code = "No map"
        no_map_target_concept_display = "No matching concept"
        no_map_target_system_version_uuid = "93ec9286-17cf-4837-a4dc-218ce3015de6"

        no_maps = self.conn.execute(
            text(
                """
                SELECT uuid FROM concept_maps.source_concept_data  
                WHERE no_map=true AND concept_map_version_uuid=:new_concept_map_version;
                """
            ),
            {"new_concept_map_version": new_concept_map_version},
        )

        # Iterate through the source uuids that are no maps
        for row in no_maps:
            source_uuid = row[0]
            source_concept = app.concept_maps.models.SourceConcept.load(source_uuid)
            # Create a new mapping between the source concept and the Ronin_No map terminology
            mapping = app.concept_maps.models.Mapping(
                source=source_concept,
                relationship=app.concept_maps.models.MappingRelationship.load_by_uuid(
                    no_map_relationship_uuid
                ),
                target=Code(
                    code=no_map_target_concept_code,
                    display=no_map_target_concept_display,
                    system=None,
                    version=None,
                    terminology_version=load_terminology_version_with_cache(
                        no_map_target_system_version_uuid
                    ),
                ),
                mapping_comments="mapped no map",
                author=None,
                review_status="reviewed",
                created_date=None,
                reviewed_date=None,
                review_comment=None,
                reviewed_by=None,
            )

            mapping.save()

    def process_no_map(
        self,
        previous_source_concept: "app.concept_maps.models.SourceConcept",
        new_source_concept: "app.concept_maps.models.SourceConcept",
        require_review_no_maps_not_in_target: bool,
        previous_contexts_list: list,
    ):
        """
        Processes a source concept with explicit no maps from the previous version.

        This method checks if a review is required for no-maps not in the target.
        If a review is required, it sets the map_status of the new source concept to 'ready for review'
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
                previous_version_context=json.dumps(
                    previous_contexts_list, cls=CustomJSONEncoder
                ),
                map_status="ready for review",
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
        self,
        new_source_concept: "app.concept_maps.models.SourceConcept",
        previous_mapping: "app.concept_maps.models.Mapping",
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
            "author": previous_mapping.mapped_by,
            "review_status": previous_mapping.review_status,
            "created_date": previous_mapping.mapped_date_time,
            "reviewed_date": previous_mapping.mapped_date_time,
            "review_comment": previous_mapping.review_comments,
            "reviewed_by": previous_mapping.reviewed_by,
        }
        new_source_concept.update(
            map_status="pending",
        )
        return previous_mapping_context

    def copy_mapping_exact(
        self,
        new_source_concept: "app.concept_maps.models.SourceConcept",
        new_target_code: Code,
        previous_mapping: "app.concept_maps.models.Mapping",
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
        relationship = previous_mapping.relationship

        target_code = new_target_code

        new_mapping = app.concept_maps.models.Mapping(
            source=new_source_concept,
            relationship=relationship,
            target=target_code,
            mapping_comments=previous_mapping.mapping_comments,
            author=previous_mapping.mapped_by,
            review_status=previous_mapping.review_status,
            created_date=previous_mapping.mapped_date_time,
            reviewed_date=previous_mapping.reviewed_date_time,
            review_comment=previous_mapping.review_comments,
            reviewed_by=previous_mapping.reviewed_by,
        )

        new_mapping.save()

    def copy_mapping_require_review(
        self,
        new_source_concept: "app.concept_maps.models.SourceConcept",
        new_target_concept: Code,
        previous_mapping: "app.concept_maps.models.Mapping",
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
        new_mapping = app.concept_maps.models.Mapping(
            source=new_source_concept,
            relationship=relationship,
            target=target_code,
            mapping_comments=previous_mapping.mapping_comments,
            author=previous_mapping.mapped_by,
            review_status="ready for review",
            created_date=previous_mapping.mapped_date_time,
            review_comment=previous_mapping.review_comments,  # todo: how to handle this
        )

        new_mapping.save()
