import csv
import io
import string

from uuid import uuid4

from deprecated.classic import deprecated
from flask import Blueprint, jsonify, request, make_response

from werkzeug.exceptions import BadRequest

import app.tasks as tasks
from app.concept_maps.models import *
from app.concept_maps.versioning_models import *
from app.helpers.oci_helper import get_data_from_oci
import app.concept_maps.rxnorm_mapping_models

from app.errors import NotFoundException

concept_maps_blueprint = Blueprint("concept_maps", __name__)


@concept_maps_blueprint.route(
    "/SourceConcepts/<string:source_concept_uuid>", methods=["PATCH"]
)
def update_source_concept(source_concept_uuid):
    """
    Used to update the assignment of a mapping row from one person to another. This is an essential feature.
    Update the comments field of a source concept identified by the source_concept_uuid.
    Returns a status message.
    """
    comments = request.json.get("comments")
    assigned_mapper = request.json.get("assigned_mapper")
    source_concept = SourceConcept.load(source_concept_uuid)
    source_concept.update(comments=comments, assigned_mapper=assigned_mapper)
    return jsonify(source_concept.serialize())


@concept_maps_blueprint.route("/ConceptMaps/<string:version_uuid>", methods=["GET"])
def get_concept_map_version(version_uuid):
    """
    GET: Retrieve the data for a Concept Map version. The schema version will be the highest of:
    the current ConceptMap.database_schema_version (such as 3, as in /ConceptMap/v3/published)
    or the current ConceptMap.next_schema_version (such as 4, as in /ConceptMap/v4/published).

    Required. We maintain appropriate GET endpoints for each resource type.
    This will be used in Retool as we deprecate the old direct database queries.

    @param version_uuid: Concept Map version UUID
    """
    include_internal_info = bool(request.values.get("include_internal_info"))
    concept_map_version = ConceptMapVersion(version_uuid)
    concept_map_to_json = concept_map_version.serialize(
        include_internal_info=include_internal_info,
        schema_version=ConceptMap.next_schema_version,
    )
    return jsonify(concept_map_to_json)


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/actions/index", methods=["POST"]
)
def index_targets(version_uuid):
    """
    Index the targets of a ConceptMap version identified by the version_uuid.
    Returns a status message.

    Used via Postman for manually re-indexing a concept map when there's issues w/ the index.
    """
    target_value_set_version_uuid = request.json.get("target_value_set_version_uuid")
    ConceptMap.index_targets(
        version_uuid, target_value_set_version_uuid=target_value_set_version_uuid
    )
    return "OK"


@concept_maps_blueprint.route("/ConceptMaps/", methods=["GET", "POST"])
def create_initial_concept_map_and_version_one():
    """
    GET: Retrieve a draft version of a Concept Map using its version UUID.
    POST: Create an initial ConceptMap and its first version based on input from the request payload.

    The Concept Map schema version will be the highest of:
    the current ConceptMap.database_schema_version (such as 3)
    or the current ConceptMap.next_schema_version (such as 4).

    Required. We maintain appropriate GET endpoints for each resource type.
    This will be used in Retool as we deprecate the old direct database queries.
    """
    if request.method == "POST":
        name = request.json.get("name")
        title = request.json.get("title")
        publisher = request.json.get("publisher")
        author = request.json.get("author")
        cm_description = request.json.get("cm_description")
        experimental = request.json.get("experimental")
        use_case_uuid = request.json.get("use_case_uuid")
        source_value_set_uuid = request.json.get("source_value_set_uuid")
        target_value_set_uuid = request.json.get("target_value_set_uuid")
        cm_version_description = request.json.get("cm_version_description")
        source_value_set_version_uuid = request.json.get(
            "source_value_set_version_uuid"
        )
        target_value_set_version_uuid = request.json.get(
            "target_value_set_version_uuid"
        )

        new_cm = ConceptMap.initial_concept_map_creation(
            name=name,
            title=title,
            publisher=publisher,
            author=author,
            use_case_uuid=use_case_uuid,
            cm_description=cm_description,
            experimental=experimental,
            source_value_set_uuid=source_value_set_uuid,
            target_value_set_uuid=target_value_set_uuid,
            cm_version_description=cm_version_description,
            source_value_set_version_uuid=source_value_set_version_uuid,
            target_value_set_version_uuid=target_value_set_version_uuid,
        )
        return jsonify(new_cm.serialize())

    elif request.method == "GET":

        concept_map_uuid = request.values.get("concept_map_uuid")
        version = request.values.get("version")
        include_internal_info = request.values.get("include_internal_info")
        include_internal_info = bool(include_internal_info)

        if not concept_map_uuid or not version:
            return jsonify(
                {"error": "A concept_map_uuid and version must be supplied."}, 400
            )

        concept_map_version = ConceptMapVersion.load_by_concept_map_uuid_and_version(
            concept_map_uuid=concept_map_uuid, version=version
        )

        if not concept_map_version:
            return jsonify({"error": "Concept Map Version not found."}, 404)

        serialized_concept_map_version = concept_map_version.serialize(
            include_internal_info=include_internal_info,
            schema_version=ConceptMap.next_schema_version,
        )
        return jsonify(serialized_concept_map_version)


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/draft", methods=["GET"]
)
def get_concept_map_draft(version_uuid):
    """
    Retrieve a draft of a ConceptMap version identified by the version_uuid.
    Returns the draft as a CSV file attachment.
    """
    concept_map_version = ConceptMapVersion(version_uuid)
    csv_data, csv_field_names = concept_map_version.mapping_draft()

    # Create a CSV file-like object in memory
    si = io.StringIO()
    cw = csv.DictWriter(si, fieldnames=csv_field_names)
    cw.writeheader()
    cw.writerows(csv_data)

    # Return the CSV file as a response
    output = make_response(si.getvalue())
    filename = concept_map_version.concept_map.title.translate(
        str.maketrans("", "", string.whitespace)
    )
    output.headers[
        "Content-Disposition"
    ] = f"attachment; filename={filename}-{concept_map_version.version}draft.csv"
    output.headers["Content-type"] = "text/csv"
    return output


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/published", methods=["GET", "POST"]
)
def get_concept_map_version_published(version_uuid):
    """
    GET: Retrieve a published version of a Concept Map from OCI. The schema version will be the highest of:
    the current ConceptMap.database_schema_version (such as 3, as in /ConceptMap/v3/published)
    or the current ConceptMap.next_schema_version (such as 4, as in /ConceptMap/v4/published).

    POST: Create a new published version of the Concept Map and store it in OCI.
    If the current ConceptMap.database_schema_version (such as 3)
    is different from the current ConceptMap.next_schema_version (such as 4)
    then both formats are output to OCI in /ConceptMap/v3/published and /ConceptMap/v4/published folders.

    As an intended follow-up to publishing a concept map to OCI via this API, this function contacts the error service
    to see if any of the errors previously reported by the organization are resolved by new concepts added to the map.

    @param version_uuid: Concept Map version UUID
    @raise BadRequestWithCode if the schema_version is v4 or later and there are no mappings in the concept map.
    """
    concept_map_version = ConceptMapVersion(version_uuid)

    if request.method == "POST":
        oci_overwrite_allowed = request.values.get("overwrite_allowed").lower() == "true"
        try:
            concept_map_version.publish(resolve_errors=True, overwrite_allowed=oci_overwrite_allowed)
        except ValueError as value_error:
            return BadRequest(value_error.args[0])

        return "Published"

    if request.method == "GET":
        concept_map_from_object_store = get_data_from_oci(
            oci_root=ConceptMap.object_storage_folder_name,
            resource_schema_version=str(ConceptMap.next_schema_version),
            release_status="published",
            resource_id=concept_map_version.concept_map.uuid,
            resource_version=concept_map_version.version,
            content_type="json",
            return_content=True,
        )
        return jsonify(concept_map_from_object_store)


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:previous_version_uuid>/new_version_from_previous",
    methods=["POST"],
)
def create_new_concept_map_version_from_previous(previous_version_uuid):
    """
    @param previous_version_uuid: UUID of the most recent concept map version, regardless of version status.
    """
    new_version_description = request.json.get("new_version_description")
    # Callers must input the UUID of the most recent active source value set version.
    new_source_value_set_version_uuid = request.json.get(
        "new_source_value_set_version_uuid"
    )
    # Callers must input the UUID of the most recent active target value set version.
    new_target_value_set_version_uuid = request.json.get(
        "new_target_value_set_version_uuid"
    )
    require_review_for_non_equivalent_relationships = request.json.get(
        "require_review_for_non_equivalent_relationships"
    )
    require_review_no_maps_not_in_target = request.json.get(
        "require_review_no_maps_not_in_target"
    )

    version_creator = ConceptMapVersionCreator()
    version_creator.new_version_from_previous(
        previous_version_uuid,
        new_version_description,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
        require_review_for_non_equivalent_relationships,
        require_review_no_maps_not_in_target,
    )
    return "Created", 201


@deprecated("We have moved away from the ConceptMapSuggestions paradigm.")
@concept_maps_blueprint.route("/ConceptMapSuggestions/", methods=["POST"])
def mapping_suggestion():
    """
    Create a new Mapping Suggestion with the provided source concept UUID, code, display, terminology_version_uuid,
    suggestion source, and confidence.
    """
    if request.method == "POST":
        source_concept_uuid = request.json.get("source_concept_uuid")
        code = request.json.get("code")
        display = request.json.get("display")
        terminology_version_uuid = request.json.get("terminology_version_uuid")
        suggestion_source = request.json.get("suggestion_source")
        confidence = request.json.get("confidence")
        new_uuid = uuid4()

        terminology_version = Terminology.load(terminology_version_uuid)
        code = Code(
            system=terminology_version.fhir_uri,
            version=terminology_version.version,
            code=code,
            display=display,
            terminology_version=terminology_version,
        )

        new_suggestion = MappingSuggestion(
            uuid=new_uuid,
            source_concept_uuid=source_concept_uuid,
            code=code,
            suggestion_source=suggestion_source,
            confidence=confidence,
        )

        new_suggestion.save()

        return jsonify(new_suggestion.serialize())


@concept_maps_blueprint.route("/mappings/", methods=["POST"])
def create_mappings():
    """
    Create a new Concept Map with the provided source concept UUID(s), relationship code UUID, target concept code,
    target concept display, target concept terminology version UUID, mapping comments, author, and review status.
    """

    if request.method == "POST":
        source_concept_uuids = request.json.get("source_concept_uuids")
        if not isinstance(source_concept_uuids, list):
            source_concept_uuids = [source_concept_uuids]

        relationship_code_uuid = request.json.get("relationship_code_uuid")
        target_concept_code = request.json.get("target_concept_code")
        target_concept_display = request.json.get("target_concept_display")
        target_concept_terminology_version_uuid = request.json.get(
            "target_concept_terminology_version_uuid"
        )
        mapping_comments = request.json.get("mapping_comments")
        author = request.json.get("author")
        review_status = request.json.get("review_status")

        relationship = MappingRelationship.load(relationship_code_uuid)

        target_code = Code(
            code=target_concept_code,
            display=target_concept_display,
            system=None,
            version=None,
            terminology_version_uuid=target_concept_terminology_version_uuid,
        )

        new_mappings = []
        for source_concept_uuid in source_concept_uuids:
            make_author_assigned_mapper(source_concept_uuid, author)
            source_code = SourceConcept.load(source_concept_uuid)
            new_mapping = Mapping(
                source=source_code,
                relationship=relationship,
                target=target_code,
                mapping_comments=mapping_comments,
                author=author,
                review_status=review_status,
            )
            new_mapping.save()
            new_mappings.append(new_mapping.serialize())

        return jsonify(new_mappings)


@concept_maps_blueprint.route(
    "/ConceptMaps/update/mapping_relationships", methods=["PATCH"]
)
def update_mapping_relationship():
    data = request.get_json()

    # Check if required fields are provided in the update
    if "mapping_uuid" not in data or "new_relationship_code_uuid" not in data:
        return BadRequest(
            "mapping_uuid and new_relationship_code_uuid are required fields"
        )

        # Convert the new_relationship_code_uuid from string to UUID object
    try:
        new_relationship_code_uuid = UUID(data["new_relationship_code_uuid"])
    except ValueError:
        return BadRequest("Invalid UUID format for new_relationship_code_uuid")

        # Iterate over each mapping_uuid and update the relationship_code_uuid
    for mapping_uuid_str in data["mapping_uuid"]:
        try:
            mapping_uuid = UUID(mapping_uuid_str)
        except ValueError:
            return BadRequest("Invalid UUID format for mapping_uuid")

            # Update the relationship_code_uuid for the specified mapping_uuid in the database
        Mapping.update_relationship_code(mapping_uuid, new_relationship_code_uuid)

    return jsonify({"message": "Successfully updated mapping relationship(s)"})


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/simplifier", methods=["POST"]
)
def push_concept_map_version_to_simplifier(version_uuid):
    """
    This API is required for manually publishing to Simplifier.
    """
    uuid_obj = uuid.UUID(version_uuid)  # cast as uuid
    concept_map_version = ConceptMapVersion(uuid=uuid_obj)  # instantiate object
    # check for active status error if false
    concept_map_version.to_simplifier()

    return "Successfully pushed to simplifier", 200


@concept_maps_blueprint.route("/ConceptMaps/simplifier/back_fill", methods=["POST"])
def full_back_fill_to_simplifier():
    """
    This API is required for manually publishing to Simplifier.
    """
    tasks.back_fill_concept_maps_to_simplifier.delay()

    return "Full concept map back fill to Simplifier complete."


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/concepts_to_assign", methods=["GET"]
)
def concepts_for_mapper_assignment(version_uuid):
    dict_list = get_concepts_for_assignment(version_uuid)
    response = jsonify(dict_list)
    return response


@concept_maps_blueprint.route("/ConceptMaps/map_no_maps", methods=["POST"])
def new_concept_map_version_map_no_maps():
    previous_version_uuid = request.json.get("previous_version_uuid")
    new_version_description = request.json.get("new_version_description")
    new_source_value_set_version_uuid = request.json.get(
        "new_source_value_set_version_uuid"
    )
    new_target_value_set_version_uuid = request.json.get(
        "new_target_value_set_version_uuid"
    )
    require_review_for_non_equivalent_relationships = request.json.get(
        "require_review_for_non_equivalent_relationships"
    )
    require_review_no_maps_not_in_target = request.json.get(
        "require_review_no_maps_not_in_target"
    )

    # Process the concept map version and get the new concept map version UUID
    creator = ConceptMapVersionCreator()
    new_version_uuid = creator.new_version_from_previous(
        previous_version_uuid=previous_version_uuid,
        new_version_description=new_version_description,
        new_source_value_set_version_uuid=new_source_value_set_version_uuid,
        new_target_value_set_version_uuid=new_target_value_set_version_uuid,
        require_review_for_non_equivalent_relationships=require_review_for_non_equivalent_relationships,
        require_review_no_maps_not_in_target=require_review_no_maps_not_in_target,
    )

    # Call create_no_map_mappings on the new concept map version
    creator.create_no_map_mappings(new_version_uuid)

    return "Concept map version processed successfully."


@concept_maps_blueprint.route("/ConceptMaps/reference_data/RxNorm", methods=["POST"])
def get_rxnorm_data_for_source_code_endpoint():
    source_code = request.json.get("source_code")
    response = app.concept_maps.rxnorm_mapping_models.get_rxnorm_data_for_source_code(
        source_code
    )
    return jsonify(response)
