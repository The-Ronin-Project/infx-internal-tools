from flask import Blueprint, request, jsonify
import dataclasses
from app.registries.models import (
    GroupMember,
    Group,
    Registry,
    LabGroupMember,
    DuplicateRegistryNameError,
)
from app.models.codes import *
from app.terminologies.models import *

registries_blueprint = Blueprint("registries", __name__, url_prefix="/registries")


@registries_blueprint.route("/", methods=["POST", "GET"])
def create_or_get_registry():
    if request.method == "POST":
        # create registry
        title = request.json.get("title")
        registry_type = request.json.get("registry_type")
        sorting_enabled = request.json.get("sorting_enabled")

        if sorting_enabled is not None:
            if type(sorting_enabled) != bool:
                raise BadRequestWithCode(
                    "bad-request", "sorting_enabled must be boolean"
                )
        try:
            new_registry = Registry.create(
                title=title,
                registry_type=registry_type,
                sorting_enabled=sorting_enabled,
            )
            return jsonify(new_registry.serialize()), 201

        except DuplicateRegistryNameError as e:
            return jsonify({"error": str(e)}), 409  # 409 Conflict

    elif request.method == "GET":
        # get all registries (for main page with list of registries)
        registries = Registry.load_all_registries()
        return jsonify([registry.serialize() for registry in registries])


@registries_blueprint.route("/<string:registry_uuid>", methods=["PATCH", "GET"])
def update_registry_metadata(registry_uuid):
    # Update the metadata of a specific registry
    if request.method == "PATCH":
        title = request.json.get("title")
        sorting_enabled = request.json.get("sorting_enabled")
        registry_type = request.json.get("registry_type")

        registry = Registry.load(registry_uuid)
        if not registry:
            return jsonify({"error": "Registry not found"}), 404

        registry.update(
            title=title, sorting_enabled=sorting_enabled, registry_type=registry_type
        )

        return jsonify(registry=dataclasses.asdict(registry))

    elif request.method == "GET":
        # Load the registry using its UUID
        registry = Registry.load(registry_uuid)

        # If the registry is not found, return an error response
        if not registry:
            return jsonify({"error": "Registry not found"}), 404

            # Return the registry's metadata in the response
        return jsonify(registry=dataclasses.asdict(registry))


@registries_blueprint.route("/<string:registry_uuid>/groups/", methods=["POST"])
def create_group(registry_uuid):
    if request.method == "POST":
        registry_uuid = registry_uuid
        title = request.json.get("title")

        # Create new group
        new_group = Group.create(registry_uuid=registry_uuid, title=title)

        return jsonify(new_group.serialize())


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>", methods=["PATCH", "DELETE"]
)
def update_group(registry_uuid, group_uuid):
    group = Group.load(group_uuid)
    if not group:
        return jsonify({"error": "Group not found"}), 404

    if request.method == "PATCH":
        # Implement update logic
        title = request.json.get("title")
        group.update(title)
        return jsonify(group.serialize())
        # todo: let's have a separate conversation on how to update sequence appropriately

    elif request.method == "DELETE":
        group.delete()
        return jsonify({"success": "Group deleted"})


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/", methods=["POST"]
)
def create_group_member(registry_uuid, group_uuid):
    if request.method == "POST":
        registry = Registry.load(registry_uuid)

        group_uuid = group_uuid
        title = request.json.get("title")
        value_set_uuid = request.json.get("value_set_uuid")

        if registry.data_type == "lab":
            pass
        elif registry.data_type == "vital":
            pass
        else:
            # Create new group member
            new_group_member = GroupMember.create(
                group_uuid=group_uuid,
                title=title,
                value_set_uuid=value_set_uuid,
            )

        return jsonify(new_group_member.serialize())
    pass


@registries_blueprint.route("/<string:registry_uuid>/groups", methods=["GET"])
def get_registry_groups(registry_uuid):
    # Load the registry using its UUID
    registry = Registry.load(registry_uuid)

    # If the registry is not found, return an error response
    if not registry:
        return jsonify({"error": "Registry not found"}), 404

        # Load all the groups associated with the registry
    groups = Group.load_all_for_registry(registry_uuid)

    # Convert the groups to dictionaries
    groups_dicts = [group.to_dict() for group in groups]

    # Return the groups in the response
    return jsonify(groups=groups_dicts)


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/<string:member_uuid>",
    methods=["PATCH", "DELETE"],
)
def update_group_member(registry_uuid, group_uuid, member_uuid):
    if request.method == "PATCH":
        title = request.json.get("title")
        value_set_uuid = request.json.get("value_set_uuid")

        group_member = GroupMember.load(member_uuid)
        if not group_member:
            return jsonify({"error": "Group member not found"}), 404

        group_member.update(title=title, value_set_uuid=value_set_uuid)

        return "update complete"

    elif request.method == "DELETE":
        # Implement delete logic
        pass
