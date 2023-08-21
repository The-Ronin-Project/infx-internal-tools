from flask import Blueprint, request, jsonify
import dataclasses
from app.registries.models import GroupMember, Group, Registry, LabGroupMember
from app.models.codes import *
from app.terminologies.models import *

registries_blueprint = Blueprint("registries", __name__, url_prefix='/registries')


@registries_blueprint.route("/", methods=["POST", "GET", "PUT"])
def create_or_get_registry():
    if request.method == "POST":
        # create registry
        pass
    elif request.method == "GET":
        # get all registries (for main page with list of registries)
        pass
    elif request.method == "PUT":
        # Update title, or other registry-level metadata
        pass


@registries_blueprint.route("/<string:registry_uuid>/groups/", methods=["POST"])
def create_group(registry_uuid):
    if request.method == "POST":
        registry_uuid = registry_uuid
        title = request.json.get("title")

        # Create new group
        new_group = Group.create(
            registry_uuid=registry_uuid,
            title=title
        )

        return jsonify(new_group.serialize())


@registries_blueprint.route("/<string:registry_uuid>/groups/<group_uuid>", methods=["PUT", "DELETE"])
def update_group():
    if request.method == 'PUT':
        # Implement update logic
        pass
    elif request.method == "DELETE":
        # Implement delete logic
        pass


@registries_blueprint.route("/<string:registry_uuid>/groups/<string:group_uuid>/members/", methods=["POST"])
def create_group_member(registry_uuid, group_uuid):
    if request.method == "POST":
        registry = Registry.load(registry_uuid)

        group_uuid = group_uuid
        title = request.json.get("title")
        value_set_uuid = request.json.get("value_set_uuid")

        if registry.data_type == 'lab':
            pass
        elif registry.data_type == 'vital':
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


@registries_blueprint.route("/<registry_id>/groups/<group_id>/members/<member_id>", methods=["PUT", "DELETE"])
def update_group_member():
    if request.method == 'PUT':
        # Implement update logic
        pass
    elif request.method == "DELETE":
        # Implement delete logic
        pass



