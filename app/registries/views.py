from flask import Blueprint, request, jsonify
import dataclasses
from app.registries.models import (
    GroupMember,
    Group,
    Registry,
    LabsGroup,
    VitalsGroupMember,
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

        new_registry = Registry.create(
            title=title,
            registry_type=registry_type,
            sorting_enabled=sorting_enabled,
        )
        return jsonify(new_registry.serialize()), 201

    elif request.method == "GET":
        # get all registries (for main page with list of registries)
        registries = Registry.load_all_registries()
        return jsonify([registry.serialize() for registry in registries])


@registries_blueprint.route("/<string:registry_uuid>", methods=["PATCH", "GET"])
def update_registry_metadata(registry_uuid):
    registry = Registry.load(registry_uuid)

    # Update the metadata of a specific registry
    if request.method == "PATCH":
        title = request.json.get("title")
        sorting_enabled = request.json.get("sorting_enabled")
        registry_type = request.json.get("registry_type")

        registry.update(
            title=title, sorting_enabled=sorting_enabled, registry_type=registry_type
        )

        return jsonify(registry=dataclasses.asdict(registry))

    elif request.method == "GET":
        # Return the registry's metadata in the response
        return jsonify(registry.serialize())


@registries_blueprint.route("/<string:registry_uuid>/groups/", methods=["POST", "GET"])
def create_group(registry_uuid):
    if request.method == "POST":
        title = request.json.get("title")

        # todo: identify and instantiate the correct type of group

        # Create new group
        new_group = Group.create(registry_uuid=registry_uuid, title=title)

        return jsonify(new_group.serialize())
    elif request.method == "GET":
        registry = Registry.load(registry_uuid)

        # Load all the groups associated with the registry
        registry.load_groups()

        # Return the groups in the response
        return jsonify([group.serialize() for group in registry.groups])


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>",
    methods=["PATCH", "DELETE", "GET"],
)
def update_group(registry_uuid, group_uuid):
    group = Group.load(group_uuid)

    if request.method == "PATCH":
        # Implement update logic
        title = request.json.get("title")
        group.update(title)
        return jsonify(group.serialize())

    elif request.method == "DELETE":
        group.delete()
        return jsonify(group.serialize())

    elif request.method == "GET":
        return jsonify(group.serialize())


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/actions/reorder/<string:direction>",
    methods=["POST"],
)
def reorder_group(registry_uuid, group_uuid, direction):
    if direction not in ["next", "previous"]:
        raise BadRequestWithCode(
            "Group.reorder.direction",
            "'up' and 'down' are the only valid directions to reorder a Group Member",
        )
    group = Group.load(group_uuid)
    group.swap_sequence(direction=direction)
    return "Reordered", 200


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/",
    methods=["POST", "GET"],
)
def create_group_member(registry_uuid, group_uuid):
    registry = Registry.load(registry_uuid)

    if request.method == "POST":
        title = request.json.get("title")
        value_set_uuid = request.json.get("value_set_uuid")

        # use VitalsGroupMember for vitals
        if registry.registry_type == "vitals":
            new_group_member = VitalsGroupMember.create(
                group_uuid=group_uuid,
                title=title,
                value_set_uuid=value_set_uuid,
                ucum_ref_units=request.json.get("ucum_ref_units"),
                ref_range_high=request.json.get("ref_range_high"),
                ref_range_low=request.json.get("ref_range_low"),
            )
        # labs, documents, and all others can use the generic class
        # because they have no additional data
        else:
            # Create a new group member
            new_group_member = GroupMember.create(
                group_uuid=group_uuid,
                title=title,
                value_set_uuid=value_set_uuid,
            )

            # Return the group member in the response
            return jsonify(new_group_member.serialize())
    elif request.method == "GET":
        # Load all the members associated with the group
        group = Group.load(group_uuid)
        group.load_members()

        # Return the members in the response
        return jsonify([group_member.serialize() for group_member in group.members])


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/<string:member_uuid>",
    methods=["PATCH", "DELETE"],
)
def update_group_member(registry_uuid, group_uuid, member_uuid):
    group_member = GroupMember.load(member_uuid)

    if request.method == "PATCH":
        title = request.json.get("title")
        value_set_uuid = request.json.get("value_set_uuid")
        group_member.update(title=title, value_set_uuid=value_set_uuid)
        return jsonify(group_member.serialize())

    elif request.method == "DELETE":
        group_member.delete()
        return jsonify(group_member.serialize())


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/<string:member_uuid>/actions/reorder/<string:direction>",
    methods=["POST"],
)
def reorder_group_member(registry_uuid, group_uuid, member_uuid, direction):
    if direction not in ["next", "previous"]:
        raise BadRequestWithCode(
            "GroupMember.reorder.direction",
            "'up' and 'down' are the only valid directions to reorder a Group Member",
        )
    group_member = GroupMember.load(member_uuid)
    group_member.swap_sequence(direction=direction)
    return "Reordered", 200
