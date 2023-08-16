from flask import Blueprint, request, jsonify
import dataclasses
from app.registries.models import GroupMember
from app.models.codes import *
from app.terminologies.models import *

registries_blueprint = Blueprint("registries", __name__)


@registries_blueprint.route("/", methods=["POST", "GET"])
def create_or_get_registry():
    if request.method == "POST":
        # create registry
        pass
    elif request.method == "GET":
        # get all registries (for main page with list of registries)
        pass


@registries_blueprint.route("/<registry_id>", methods=["GET", "PUT"])
def get_specific_registry():
    if request.method == "GET":
        # return registry, including the groups so they can be displayed
        pass
    elif request.method == "PUT":
        # Update title, or other registry-level metadata
        pass


@registries_blueprint.route("/<registry_id>/groups/", methods=["POST"])
def create_group():
    # Implement create group logic
    pass


@registries_blueprint.route("/<registry_id>/groups/<group_uuid>", methods=["PUT", "DELETE"])
def update_group():
    if request.method == 'PUT':
        # Implement update logic
        pass
    elif request.method == "DELETE":
        # Implement delete logic
        pass


@registries_blueprint.route("/<registry_id>/groups/<group_uuid>/", methods=["POST"])
def create_group_member():
    if request.method == "POST":
        group_uuid = request.json.get("group_uuid")
        group_member_type = request.json.get("group_member_type")
        product_title = request.json.get("product_title")
        sequence = request.json.get("sequence")
        value_set_uuid = request.json.get("value_set_uuid")
        value_set_version_uuid = request.json.get("value_set_version_uuid")

        # You may want to add checks to ensure that the data is valid or present

        new_group_member = GroupMember.create(
            group_uuid=group_uuid,
            # group_member_type=group_member_type,
            product_element_type_label=product_title,  # change in pgAdmin
            sequence=sequence,
            # value_set_uuid=value_set_uuid,
            # value_set_version_uuid=value_set_version_uuid

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



