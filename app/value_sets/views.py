from io import StringIO
from flask import Blueprint, request, jsonify, Response
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
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.value_sets.models import *

value_sets_blueprint = Blueprint("value_sets", import_name=__name__)


# Value Set Endpoints
# FHIR endpoint
@value_sets_blueprint.route("/ValueSet/<string:uuid>/$expand")
def expand_value_set(uuid):
    """Expands the specified ValueSet and returns it as a JSON object."""
    force_new = request.values.get("force_new") == "true"
    vs_version = ValueSetVersion.load(uuid)
    vs_version.expand(force_new=force_new)
    return jsonify(vs_version.serialize())


@value_sets_blueprint.route("/ValueSets/", methods=["GET", "POST"])
def get_all_value_sets_metadata():
    """
    Handles both GET and POST requests for ValueSets metadata.
    GET: Returns a list of metadata for all ValueSets.
    POST: Creates a new ValueSet and returns its metadata.
    """
    if request.method == "GET":
        active_only = False if request.values.get("active_only") == "false" else True
        return jsonify(ValueSet.load_all_value_set_metadata(active_only))
    if request.method == "POST":
        name = request.json.get("name")
        title = request.json.get("title")
        publisher = request.json.get("publisher")
        contact = request.json.get("contact")
        value_set_description = request.json.get("value_set_description")
        immutable = request.json.get("immutable")
        experimental = request.json.get("experimental")
        purpose = request.json.get("purpose")
        vs_type = request.json.get("type")
        use_case_uuid = request.json.get("use_case_uuid")
        effective_start = request.json.get("effective_start")
        effective_end = request.json.get("effective_end")
        version_description = request.json.get("version_description")

        new_vs = ValueSet.create(
            name=name,
            title=title,
            publisher=publisher,
            contact=contact,
            value_set_description=value_set_description,
            immutable=immutable,
            experimental=experimental,
            purpose=purpose,
            vs_type=vs_type,
            use_case_uuid=use_case_uuid,
            effective_start=effective_start,
            effective_end=effective_end,
            version_description=version_description,
        )
        return jsonify(new_vs.serialize())


@value_sets_blueprint.route(
    "/ValueSets/<string:identifier>/duplicate", methods=["POST"]
)
def duplicate_value_set_and_version(identifier):
    """Duplicates a ValueSet and its associated version, returning the new ValueSet's UUID."""
    value_set = ValueSet.load(identifier)
    name = (request.json.get("name"),)
    title = (request.json.get("title"),)
    contact = (request.json.get("contact"),)
    value_set_description = (request.json.get("value_set_description"),)
    purpose = (request.json.get("purpose"),)
    effective_start = request.json.get("effective_start")
    effective_end = request.json.get("effective_end")
    version_description = request.json.get("version_description")
    duplicated_value_set_uuid = value_set.duplicate_vs(
        name,
        title,
        contact,
        value_set_description,
        purpose,
        effective_start,
        effective_end,
        version_description,
        use_case_uuid=None,
    )
    return str(duplicated_value_set_uuid), 201


@value_sets_blueprint.route(
    "/ValueSets/<string:identifier>/actions/perform_terminology_update",
    methods=["POST"],
)
def perform_terminology_update_for_value_set(identifier):
    old_terminology_version_uuid = request.json.get("old_terminology_version_uuid")
    new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")
    new_value_set_effective_start = request.json.get("new_value_set_effective_start")
    new_value_set_effective_end = request.json.get("new_value_set_effective_end")
    new_value_set_description = request.json.get("new_value_set_description")

    value_set = ValueSet.load(identifier)
    result = value_set.perform_terminology_update(
        old_terminology_version_uuid=old_terminology_version_uuid,
        new_terminology_version_uuid=new_terminology_version_uuid,
        effective_start=new_value_set_effective_start,
        effective_end=new_value_set_effective_end,
        description=new_value_set_description,
    )
    return jsonify(result)


@value_sets_blueprint.route(
    "/ValueSets/<string:value_set_uuid>/versions/<string:version_uuid>/rules/update_terminology",
    methods=["POST"],
)
def update_terminology_version_of_rules_in_value_set(value_set_uuid, version_uuid):
    """Updates the terminology version of rules in the specified ValueSet version."""
    old_terminology_version_uuid = request.json.get("old_terminology_version_uuid")
    new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")

    value_set_version = ValueSetVersion.load(version_uuid)
    value_set_version.update_rules_for_terminology(
        old_terminology_version_uuid=old_terminology_version_uuid,
        new_terminology_version_uuid=new_terminology_version_uuid,
    )
    return "OK"


@value_sets_blueprint.route(
    "/ValueSetRules/<string:rule_uuid>",
    methods=["PATCH"],
)
def update_single_rule(rule_uuid):
    """Updates the terminology version of a single ValueSet rule."""
    new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")
    rule = VSRule.load(rule_uuid)
    rule.update(new_terminology_version_uuid)
    return "OK"


@value_sets_blueprint.route(
    "/ValueSets/<string:value_set_uuid>/versions/<string:version_uuid>",
    methods=["PATCH"],
)
def version_status_update(value_set_uuid, version_uuid):
    """Updates the status of a ValueSet version."""
    new_status = request.json.get("status")
    vs_version = ValueSetVersion.load(version_uuid)
    vs_version.update(status=new_status)
    return "OK"


@value_sets_blueprint.route("/ValueSets/all/")
def get_all_value_sets():
    """Returns a list of all ValueSets with their expanded content, filtered by status."""
    status = request.values.get("status").split(",")
    value_sets = ValueSet.load_all_value_sets_by_status(status)
    for x in value_sets:
        x.expand()
    serialized = [x.serialize() for x in value_sets]
    return jsonify(serialized)


@value_sets_blueprint.route("/ValueSets/<string:identifier>/versions/")
def get_value_set_versions(identifier):
    """Returns a list of metadata for all versions of a specified ValueSet."""
    uuid = ValueSet.name_to_uuid(identifier)
    return jsonify(ValueSet.load_version_metadata(uuid))


@value_sets_blueprint.route(
    "/ValueSets/<string:identifier>/versions/new", methods=["POST"]
)
def create_new_vs_version(identifier):
    """Creates a new version of a specified ValueSet and returns its UUID."""
    value_set = ValueSet.load(identifier)
    effective_start = request.json.get("effective_start")
    effective_end = request.json.get("effective_end")
    description = request.json.get("description")
    new_version_uuid = value_set.create_new_version_from_specified_previous(
        effective_start, effective_end, description
    )
    return str(new_version_uuid), 201


@value_sets_blueprint.route("/ValueSets/<string:value_set_uuid>", methods=["DELETE"])
def delete_value_set(value_set_uuid):
    """Deletes a ValueSet and returns a 'Deleted' confirmation."""
    value_set = ValueSet.load(value_set_uuid)
    value_set.delete()
    return "Deleted", 200


# @app.route('/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>', methods=['DELETE'])
# def delete_vs_version(value_set_uuid, vs_version_uuid):
#     vs_version = ValueSetVersion.load(vs_version_uuid)
#     if str(vs_version.value_set.uuid) != str(value_set_uuid):
#         raise BadRequest(f"{vs_version_uuid} is not a version of value set with uuid {value_set_uuid}")
#     vs_version.delete()
#     return "Deleted", 200


@value_sets_blueprint.route(
    "/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>/explicitly_included_codes/",
    methods=["POST", "GET"],
)
def explicitly_included_code_to_vs_version(value_set_uuid, vs_version_uuid):
    """
    Handles both GET and POST requests for explicitly included codes in a ValueSet version.
    GET: Returns a list of all explicitly included codes for the specified ValueSet version.
    POST: Adds an explicitly included code to the specified ValueSet version and returns a 'Created' confirmation.
    """
    if request.method == "GET":
        vs_version = ValueSetVersion.load(vs_version_uuid)
        explicit_code_inclusions = ExplicitlyIncludedCode.load_all_for_vs_version(
            vs_version
        )
        return jsonify([x.serialize() for x in explicit_code_inclusions])

    if request.method == "POST":
        code_uuid = request.json.get("code_uuid")
        code = Code.load_from_custom_terminology(code_uuid)
        vs_version = ValueSetVersion.load(vs_version_uuid)

        new_explicit_code = ExplicitlyIncludedCode(
            code=code, value_set_version=vs_version, review_status="pending"
        )
        new_explicit_code.save()
        return "Created", 201


@value_sets_blueprint.route("/ValueSets/<string:identifier>/most_recent_active_version")
def get_most_recent_version(identifier):
    """Returns the most recent active version"""
    uuid = ValueSet.name_to_uuid(identifier)
    version = ValueSet.load_most_recent_active_version(uuid)
    version.expand()
    return jsonify(version.serialize())


@value_sets_blueprint.route("/ValueSets/expansions/<string:expansion_uuid>/report")
def load_expansion_report(expansion_uuid):
    """
    Retrieve a report for a specific ValueSet expansion identified by the expansion_uuid.
    Returns the report as a CSV file attachment.
    """
    report = ValueSetVersion.load_expansion_report(expansion_uuid)
    file_buffer = StringIO()
    file_buffer.write(report)
    file_buffer.seek(0)
    response = Response(
        file_buffer,
        mimetype="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename={expansion_uuid}-report.csv"
        },
    )
    return response


@value_sets_blueprint.route("/ValueSets/rule_set/execute", methods=["POST"])
def process_rule_set():
    """Allows for the real-time execution of rules, used on the front-end to preview output of a rule set"""
    rules_input = request.get_json()
    result = execute_rules(rules_input)
    return jsonify(result)


@value_sets_blueprint.route(
    "/ValueSets/diff",
    methods=["GET"],
)
def diff_new_version_against_previous():
    """
    Compare a new ValueSet version against a previous version using their UUIDs.
    Returns the differences between the versions as JSON data.
    """
    previous_version_uuid = request.json.get("previous_version_uuid")
    new_version_uuid = request.json.get("new_version_uuid")
    diff = ValueSetVersion.diff_for_removed_and_added_codes(
        previous_version_uuid, new_version_uuid
    )
    return jsonify(diff)


@value_sets_blueprint.route(
    "/ValueSets/<string:version_uuid>/prerelease", methods=["GET", "POST"]
)
def get_value_set_version_prerelease(version_uuid):
    """
    Retrieve or create a prerelease version of a ValueSet identified by the version_uuid.
    Handles both GET and POST requests.
    Returns JSON data for the prerelease ValueSet version.
    """
    object_type = "value_set"
    if request.method == "POST":
        force_new = request.values.get("force_new") == "true"
        vs_version = ValueSetVersion.load(version_uuid)
        vs_version.expand(force_new=force_new)
        value_set_to_json, initial_path = vs_version.prepare_for_oci()
        value_set_to_datastore = set_up_object_store(
            value_set_to_json, initial_path, folder="prerelease"
        )
        return jsonify(value_set_to_datastore)
    if request.method == "GET":
        value_set = get_object_type_from_db(version_uuid)
        if not value_set:
            return {"message": "value_set uuid not found."}
        value_set_from_object_store = get_object_type_from_object_store(
            object_type, value_set, folder="prerelease"
        )
        return jsonify(value_set_from_object_store)


@value_sets_blueprint.route(
    "/ValueSets/<string:version_uuid>/published", methods=["GET", "POST"]
)
def get_value_set_version_published(version_uuid):
    """
    Retrieve or publish a ValueSet version identified by the version_uuid.

    This endpoint handles both GET and POST requests. For GET requests, it returns the JSON data
    for the published ValueSet version. For POST requests, it publishes the ValueSet version to OCI,
    sets the version to active,applicable previous versions are retired or obsolete, pushes to Simplifier, and
    returns the JSON data of the published ValueSet.

    Args:
        version_uuid (str): The UUID of the ValueSet version to retrieve or publish.

    Returns:
        flask.Response: A JSON response containing the data of the published ValueSet version.

    Raises:
        NotFound: If the ValueSet version with the specified UUID is not found.
    """
    # object_type = "value_set"
    if request.method == "POST":
        force_new = request.values.get("force_new")
        value_set_version = ValueSetVersion.load(version_uuid)
        value_set_version.publish(force_new)
        return "Published"

    if request.method == "GET":
        return_content = request.values.get("return_content")
        if return_content == "false":
            return_content = False

        value_set_version = ValueSetVersion.load(version_uuid)

        value_set_from_object_store = get_json_from_oci(
            resource_type="value_set",
            resource_schema_version=VALUE_SET_SCHEMA_VERSION,
            release_status="published",
            resource_id=value_set_version.value_set.uuid,
            resource_version=value_set_version.version,
            return_content=return_content,
        )
        return jsonify(value_set_from_object_store)


@value_sets_blueprint.route(
    "/ValueSets/<string:version_uuid>/simplifier", methods=["POST"]
)
def push_value_set_version_to_simplifier(version_uuid):
    force_new = request.values.get("force_new") == "true"
    vs_version = ValueSetVersion.load(version_uuid)
    vs_version.expand(force_new=force_new)
    value_set_to_json, initial_path = vs_version.prepare_for_oci()
    resource_id = value_set_to_json["id"]
    resource_type = value_set_to_json["resourceType"]  # param for Simplifier
    # Check if the 'expansion' and 'contains' keys are present
    if (
        "expansion" in value_set_to_json
        and "contains" in value_set_to_json["expansion"]
    ):
        # Store the original total value
        original_total = value_set_to_json["expansion"]["total"]

        # Limit the contains list to the top 50 entries
        value_set_to_json["expansion"]["contains"] = value_set_to_json["expansion"][
            "contains"
        ][:50]

        # Set the 'total' field to the original total
        value_set_to_json["expansion"]["total"] = original_total
    publish_to_simplifier(resource_type, resource_id, value_set_to_json)
    return jsonify(value_set_to_json)
