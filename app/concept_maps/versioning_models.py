import datetime
import json
import uuid
from sqlalchemy import text

from app.database import get_db
import app.concept_maps.models
from app.errors import BadRequestWithCode, NotFoundException
from app.models.codes import Code
from app.terminologies.models import load_terminology_version_with_cache
import app.value_sets.models
import logging

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
        Populates the concept_maps.source_concept table with the latest expansion of the new target value set version.

        This method inserts new records into the concept_maps.source_concept table by selecting the
        latest expansion_members from the value_sets.expansion_member table. The new source concepts
        are created with a status of 'pending'.

        Returns:
            list: A list of new SourceConcept instances.
        """
        # Populate all the sources with a status of pending
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
        mapped_no_map_criteria = {
            "relationship_code_uuid": "dca7c556-82d9-4433-8971-0b7edb9c9661",
            "target_concept_code": "No map",
            "target_concept_display": "No matching concept",
        }

        mapped_no_map = {}
        all_mappings = {}

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
                        cr.target_concept_code AS target_concept_code,
                        cr.target_concept_display AS target_concept_display,
                        cr.target_concept_system AS target_concept_system,
                        cr.mapping_comments AS mapping_comments,
                        cr.review_status AS review_status,
                        cr.created_date AS concept_relationship_created_date,
                        cr.reviewed_date AS concept_relationship_reviewed_date,
                        cr.author AS concept_relationship_author,
                        cr.relationship_code_uuid AS concept_relationship_relationship_code_uuid,
                        cr.target_concept_system_version_uuid AS target_concept_system_version_uuid,
                        cr.review_comment AS concept_relationship_review_comment,
                        cr.reviewed_by AS concept_relationship_reviewed_by,
                        ctc.depends_on_property,
                        ctc.depends_on_system,
                        ctc.depends_on_value,
                        ctc.depends_on_display
                    FROM
                        concept_maps.concept_relationship cr
                    RIGHT JOIN
                        concept_maps.source_concept sc
                    ON
                        sc.uuid = cr.source_concept_uuid
                    LEFT JOIN
                        custom_terminologies.code ctc
                    ON
                        sc.custom_terminology_uuid = ctc.uuid
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
        #                                 "mappings": [Mapping(), Mapping()]
        #                             }
        # ]

        # Terminology Local cache
        # todo: study why ttl_cache, or other caching strategies, did not stop repeat loads from happening
        terminology = dict()

        for row in all_data:
            # In the database, source_concept.system is a terminology_version_uuid, but we want the FHIR URL
            # Get the system
            source_system = row.source_concept_system
            if source_system is None:
                raise BadRequestWithCode(
                    "ConceptMap.loadAllSourcesAndMappings.missingSystem",
                    f"Concept map version UUID: {concept_map_version_uuid.version}, source concept UUID: {row.source_concept_uuid} has no source system identified",
                )

            # Get the Terminology
            if source_system not in terminology.keys():
                terminology.update(
                    {source_system: load_terminology_version_with_cache(source_system)}
                )

            is_mapped_no_map = (
                (
                    str(row.concept_relationship_relationship_code_uuid)
                    == mapped_no_map_criteria["relationship_code_uuid"]
                )
                and (
                    row.target_concept_code
                    == mapped_no_map_criteria["target_concept_code"]
                )
                and (
                    row.target_concept_display
                    == mapped_no_map_criteria["target_concept_display"]
                )
            )

            # Create the SourceConcept
            source_concept = app.concept_maps.models.SourceConcept(
                uuid=row.source_concept_uuid,
                code=row.source_concept_code,
                display=row.source_concept_display,
                system=terminology.get(source_system),
                comments=row.source_concept_comments,
                additional_context=row.source_concept_additional_context,
                map_status=row.source_concept_map_status,
                assigned_mapper=row.source_concept_assigned_mapper,
                assigned_reviewer=row.source_concept_assigned_reviewer,
                no_map=row.source_concept_no_map,
                reason_for_no_map=row.source_concept_reason_for_no_map,
                mapping_group=row.source_concept_mapping_group,
                concept_map_version_uuid=row.source_concept_map_version_uuid,
                # Matching behavior relies on depends on data defaulting to '' instead of null
                depends_on_system=row.depends_on_system
                if row.depends_on_system is not None
                else "",
                depends_on_property=row.depends_on_property
                if row.depends_on_property is not None
                else "",
                depends_on_value=row.depends_on_value
                if row.depends_on_value is not None
                else "",
                depends_on_display=row.depends_on_display
                if row.depends_on_value is not None
                else "",
            )

            mapping = None

            # Check if the row has a mapping: when debugging, target values are in 1-based positions ~15, 16, 17, 24
            if row.target_concept_code is not None:
                mapping_relationship = (
                    app.concept_maps.models.MappingRelationship.load_by_uuid(
                        row.concept_relationship_relationship_code_uuid
                    )
                )

                # if target_concept_system_version_uuid (position ~24) is None here, that is fine: do not load when None
                if row.target_concept_system_version_uuid is None:
                    target_system = None
                else:
                    target_system = load_terminology_version_with_cache(
                        row.target_concept_system_version_uuid
                    )

                # make the target code
                target_code = Code(
                    code=row.target_concept_code,
                    display=row.target_concept_display,
                    system=None,
                    version=None,
                    terminology_version=target_system,
                )

                # make the mapping
                mapping = app.concept_maps.models.Mapping(
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
                source_concept.depends_on_display,
                source_concept.depends_on_property,
                source_concept.depends_on_system,
                source_concept.depends_on_value,
            )

            if is_mapped_no_map:
                if lookup_key not in mapped_no_map:
                    mapped_no_map[lookup_key] = {
                        "source_concept": source_concept,
                        "mappings": [],
                    }

                    if mapping is not None:
                        mapped_no_map[lookup_key]["mappings"].append(mapping)

                else:
                    mapped_no_map[lookup_key]["mappings"].append(mapping)

            else:
                if lookup_key not in all_mappings:
                    all_mappings[lookup_key] = {
                        "source_concept": source_concept,
                        "mappings": [],
                    }

                    if mapping is not None:
                        all_mappings[lookup_key]["mappings"].append(mapping)

                else:
                    all_mappings[lookup_key]["mappings"].append(mapping)

        return mapped_no_map, all_mappings

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

        # Data integrity checks
        # make sure the previous version is the latest version; we don't want to version anything but the latest ever
        input_previous_concept_map_version = app.concept_maps.models.ConceptMapVersion(
            previous_version_uuid
        )
        concept_map = app.concept_maps.models.ConceptMap(
            input_previous_concept_map_version.concept_map.uuid
        )
        concept_map_most_recent_version = concept_map.get_most_recent_version(
            active_only=False
        )

        if concept_map_most_recent_version is None or str(
            concept_map_most_recent_version.uuid
        ) != str(previous_version_uuid):
            raise BadRequestWithCode(
                "ConceptMap.create_new_from_previous.previous_version_uuid",
                f"Input concept map version with UUID {previous_version_uuid} is not the most recent version",
            )

        # Validate the input new_source_value_set_version_uuid
        source_value_set_version = app.value_sets.models.ValueSetVersion.load(
            new_source_value_set_version_uuid
        )
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

        # Validate the input new_target_value_set_version_uuid
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

            # Populate concept_maps.source_concept table with the latest expansion of the new source value set version
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
                source_lookup_key = (
                    new_source_concept.code,
                    new_source_concept.display,
                    new_source_concept.system.fhir_uri,
                    new_source_concept.depends_on_display,
                    new_source_concept.depends_on_property,
                    new_source_concept.depends_on_system,
                    new_source_concept.depends_on_value,
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
                        no_map_target_concept = app.concept_maps.models.Code(
                            code=previous_mapping.target.code,
                            display=previous_mapping.target.display,
                            system=previous_mapping.target.system,
                            version=no_map_version,
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
                            no_map_code = app.concept_maps.models.Code(
                                code=no_map_target_concept_code,
                                display=no_map_target_concept_display,
                                system=no_map_system,
                                version=no_map_version,
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
                f"create_new_from_previous missing data in concept map UUID {concept_map.uuid} version UUID {concept_map_most_recent_version.uuid}"
            )
            self.conn.rollback()
            raise e

        except Exception as e:  # uncaught exceptions can be so costly here, that a 'bare except' is acceptable, despite PEP 8: E722

            LOGGER.info(
                f"create_new_from_previous unexpected error with concept map UUID {concept_map.uuid} version UUID {concept_map_most_recent_version.uuid}"
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
                SELECT uuid FROM concept_maps.source_concept  
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
            "author": previous_mapping.author,
            "review_status": previous_mapping.review_status,
            "created_date": previous_mapping.created_date,
            "reviewed_date": previous_mapping.created_date,
            "review_comment": previous_mapping.review_comment,
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
            author=previous_mapping.author,
            review_status="ready for review",
            created_date=previous_mapping.created_date,
            review_comment=previous_mapping.review_comment,  # todo: how to handle this
        )

        new_mapping.save()
