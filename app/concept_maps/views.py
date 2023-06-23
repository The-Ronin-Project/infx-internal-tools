from flask import Blueprint, jsonify, request, make_response
import io
import csv
import string
from uuid import uuid4
from werkzeug.exceptions import BadRequest

from app.helpers.oci_helper import (
    version_set_status_active,
    get_object_type_from_db,
    get_object_type_from_object_store,
    set_up_object_store,
    get_json_from_oci,
)
from app.helpers.simplifier_helper import (
    publish_to_simplifier,
)
from app.concept_maps.models import *
from app.concept_maps.versioning_models import *
from app.models.data_ingestion_registry import DataNormalizationRegistry

concept_maps_blueprint = Blueprint("concept_maps", __name__)


@concept_maps_blueprint.route(
    "/SourceConcepts/<string:source_concept_uuid>", methods=["PATCH"]
)
def update_source_concept(source_concept_uuid):
    """
    Update the comments field of a source concept identified by the source_concept_uuid.
    Returns a status message.
    """
    comments = request.json.get("comments")
    assigned_mapper = request.json.get("assigned_mapper")
    source_concept = SourceConcept.load(source_concept_uuid)
    source_concept.update(comments=comments, assigned_mapper=assigned_mapper)
    return jsonify(source_concept.serialize())


# # Concept Map Endpoints
# @app.route("/ConceptMaps/actions/new_version_from_previous", methods=["POST"])
# def new_cm_version_from_previous():
#     """
#     Create a new ConceptMap version based on a previous version.
#     Takes input from the request payload.
#     Returns a status message.
#     """
#     previous_version_uuid = request.json.get("previous_version_uuid")
#     new_version_description = request.json.get("new_version_description")
#     new_version_num = request.json.get("new_version_num")
#     new_source_value_set_version_uuid = request.json.get(
#         "new_source_value_set_version_uuid"
#     )
#     new_target_value_set_version_uuid = request.json.get(
#         "new_target_value_set_version_uuid"
#     )
#
#     new_version = ConceptMap.new_version_from_previous(
#         previous_version_uuid=previous_version_uuid,
#         new_version_description=new_version_description,
#         new_version_num=new_version_num,
#         new_source_value_set_version_uuid=new_source_value_set_version_uuid,
#         new_target_value_set_version_uuid=new_target_value_set_version_uuid,
#     )
#     return "OK"


@concept_maps_blueprint.route("/ConceptMaps/<string:version_uuid>", methods=["GET"])
def get_concept_map_version(version_uuid):
    """
    Retrieve a specific ConceptMap version identified by the version_uuid.
    Returns the ConceptMap version as JSON data.
    """
    include_internal_info = bool(request.values.get("include_internal_info"))
    concept_map_version = ConceptMapVersion(version_uuid)
    concept_map_to_json = concept_map_version.serialize(
        include_internal_info=include_internal_info
    )
    return jsonify(concept_map_to_json)


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/actions/index", methods=["POST"]
)
def index_targets(version_uuid):
    """
    Index the targets of a ConceptMap version identified by the version_uuid.
    Returns a status message.
    """
    target_value_set_version_uuid = request.json.get("target_value_set_version_uuid")
    ConceptMap.index_targets(
        version_uuid, target_value_set_version_uuid=target_value_set_version_uuid
    )
    return "OK"


@concept_maps_blueprint.route("/ConceptMaps/", methods=["GET", "POST"])
def create_initial_concept_map_and_version_one():
    """
    Create an initial ConceptMap and its first version based on input from the request payload.
    Returns the newly created ConceptMap as JSON data.
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
        return jsonify(ConceptMap.serialize(new_cm))
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
            include_internal_info=include_internal_info
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
    "/ConceptMaps/<string:version_uuid>/prerelease", methods=["GET", "POST"]
)
def get_concept_map_version_prerelease(version_uuid):
    """
    Retrieve or store a pre-release version of a Concept Map using its version UUID.
    If a POST request is made, a new pre-release version is created and stored.
    If a GET request is made, the pre-release version is retrieved.
    """
    object_type = "concept_map"
    if request.method == "POST":
        concept_map_version = ConceptMapVersion(
            version_uuid
        )  # using the version uuid to get the version metadata from the ConceptMapVersion class
        (
            concept_map_to_json,
            initial_path,
        ) = concept_map_version.prepare_for_oci()  # serialize the metadata
        concept_map_to_datastore = (
            set_up_object_store(  # use the serialized data with an oci_helper function
                concept_map_to_json, initial_path, folder="prerelease"
            )
        )
        return jsonify(
            concept_map_to_datastore
        )  # returns the serialized metadata posted to OCI
    if request.method == "GET":
        concept_map = get_object_type_from_db(  # concept_map will be a dictionary of overall concept_map uuid and version integer
            version_uuid, object_type
        )  # use the version uuid with an oci_helper function to check DB first
        if not concept_map:
            return {"message": "concept map uuid not found."}  # error if not in DB
        concept_map_from_object_store = get_object_type_from_object_store(  # uses the return from the above oci_helper function to call another  oci_helper function
            object_type, concept_map, folder="prerelease"
        )
        return jsonify(concept_map_from_object_store)  # returns the file from OCI


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:version_uuid>/published", methods=["GET", "POST"]
)
def get_concept_map_version_published(version_uuid):
    """
    Retrieve or store a published version of a Concept Map using its version UUID.
    If a POST request is made, a new published version is created and stored.
    If a GET request is made, the published version is retrieved.
    """
    object_type = "concept_map"
    if request.method == "POST":
        concept_map_uuid = ConceptMapVersion(version_uuid).concept_map.uuid
        concept_map_version = ConceptMapVersion(
            version_uuid
        )  # using the version uuid to get the version metadata from the ConceptMapVersion class
        (
            concept_map_to_json,
            initial_path,
        ) = concept_map_version.prepare_for_oci()  # serialize the metadata
        concept_map_to_json_copy = (
            concept_map_to_json.copy()
        )  # Simplifier requires status
        concept_map_to_datastore = (
            set_up_object_store(  # use the serialized data with an oci_helper function
                concept_map_to_json, initial_path, folder="published"
            )
        )
        concept_map_version.version_set_status_active()
        resource_type = "ConceptMap"  # param for Simplifier
        concept_map_to_json_copy["status"] = "active"
        publish_to_simplifier(resource_type, concept_map_uuid, concept_map_to_json_copy)
        # Publish new version of data normalization registry
        try:
            DataNormalizationRegistry.publish_data_normalization_registry()
        except:
            pass  # todo: find better error handling

        return jsonify(concept_map_to_datastore)

    if request.method == "GET":
        concept_map = get_object_type_from_db(version_uuid, object_type)
        if not concept_map:
            return {"message": "concept map uuid not found."}
        concept_map_from_object_store = get_object_type_from_object_store(
            object_type, concept_map, folder="published"
        )
        return jsonify(concept_map_from_object_store)


@concept_maps_blueprint.route(
    "/ConceptMaps/<string:previous_version_uuid>/new_version_from_previous",
    methods=["POST"],
)
def create_new_concept_map_version_from_previous(previous_version_uuid):
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
