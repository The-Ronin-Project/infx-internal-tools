from flask import Blueprint, request, jsonify
import dataclasses

from app.models.codes import *
from app.terminologies.models import *

registries_blueprint = Blueprint("registries", __name__)


@registries_blueprint.route("/group_members", methods=["POST"])
def create_group_member():
    # Implement create logic
    pass


@registries_blueprint.route("/group_member/<group_member_uuid>", methods=["GET"])
def get_group_member():
    # Implement read logic
    pass


@registries_blueprint.route("/group_member/<group_member_uuid>", methods=["PUT"])
def update_group_member():
    # Implement update logic
    pass


@registries_blueprint.route("/group_member/<group_member_uuid>", methods=["DELETE"])
def delete_group_member():
    # Implement delete logic
    pass


@registries_blueprint.route("/groups", methods=["POST"])
def create_group():
    # Implement create logic
    pass


@registries_blueprint.route("/group/<group_uuid>", methods=["GET"])
def get_group():
    # Implement read logic
    pass


@registries_blueprint.route("/group/<group_uuid>", methods=["PUT"])
def update_group():
    # Implement update logic
    pass


@registries_blueprint.route("/group/<group_uuid>", methods=["DELETE"])
def delete_group():
    # Implement delete logic
    pass


@registries_blueprint.route("/registry/", methods=["POST", "GET"])
def create_registry():
    # create registry
    pass
