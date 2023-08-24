from flask import Blueprint, request, jsonify
import dataclasses
from app.registries.models import (
    GroupMember,
    Group,
    Registry,
    LabsGroupMember,
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
    "/<string:registry_uuid>/groups/<string:group_uuid>", methods=["PATCH", "DELETE"]
)
def update_group(registry_uuid, group_uuid):
    group = Group.load(group_uuid)

    if request.method == "PATCH":
        # Implement update logic
        title = request.json.get("title")
        group.update(title)
        return jsonify(group.serialize())
        # todo: let's have a separate conversation on how to update sequence appropriately

    elif request.method == "DELETE":
        group.delete()
        return jsonify(group.serialize())


@registries_blueprint.route(
    "/<string:registry_uuid>/groups/<string:group_uuid>/members/", methods=["POST", "GET"]
)
def create_group_member(registry_uuid, group_uuid):
        registry = Registry.load(registry_uuid)

        if request.method == "POST":
            # use LabsGroupMember for labs
            if registry.registry_type == "labs":
                pass
            # use VitalsGroupMember for vitals
            elif registry.registry_type == "vitals":
                pass
            else:
                # Create a new group member
                title = request.json.get("title")
                value_set_uuid = request.json.get("value_set_uuid")
                new_group_member = GroupMember.create(
                    group_uuid=group_uuid,
                    title=title,
                    value_set_uuid=value_set_uuid,
                )

                # Return the group member in the response
                return jsonify(new_group_member.serialize())
        elif request.method == "GET":
            # use LabsGroupMember for labs
            if registry.registry_type == "labs":
                raise NotFoundException(f'No Members in Group UUID: {group_uuid} in Labs Registry')
            # use VitalsGroupMember for vitals
            elif registry.registry_type == "vitals":
                raise NotFoundException(f'No Members in Group UUID: {group_uuid} in Vitals Registry ')
            else:
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
