from flask import Blueprint, request, jsonify
import dataclasses

from app.models.codes import *
from app.terminologies.models import *

terminologies_blueprint = Blueprint('terminologies', __name__)


@terminologies_blueprint.route("/terminology/", methods=["POST"])
def create_terminology():
    """
    Create a new terminology with the provided parameters, such as terminology name, version, effective start and end dates,
    FHIR URI, standard status, and FHIR terminology.
    """
    if request.method == "POST":
        terminology = request.json.get("terminology")
        version = request.json.get("version")
        effective_start = request.json.get("effective_start")
        effective_end = request.json.get("effective_end")
        if effective_end == "":
            effective_end = None
        fhir_uri = request.json.get("fhir_uri")
        is_standard = request.json.get("is_standard")
        fhir_terminology = request.json.get("fhir_terminology")
        new_terminology = Terminology.create_new_terminology(
            terminology,
            version,
            effective_start,
            effective_end,
            fhir_uri,
            is_standard,
            fhir_terminology,
        )
        term = Terminology.serialize(new_terminology)
        return term


@terminologies_blueprint.route("/terminology/new_code", methods=["POST"])
def create_code():
    """
    Add one or multiple new codes to a terminology, providing information such as code system, version, code, display,
    and terminology version.
    """
    if request.method == "POST":
        payload = request.json
        if isinstance(payload, dict):
            payload = [payload]
        new_code = Code.add_new_code_to_terminology(payload)
        codes = []
        for x in new_code:
            code = Code.serialize(x)
            codes.append(code)
        return codes


@terminologies_blueprint.route("/terminology/<terminology_version_uuid>", methods=["GET"])
def get_terminology(terminology_version_uuid):
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    terminology = Terminology.load(terminology_version_uuid)

    # Paginate the codes within the Terminology instance
    start = (page - 1) * per_page
    end = start + per_page
    paginated_codes = terminology.codes[start:end]

    return jsonify(
        {
            "terminology": {
                "uuid": terminology.uuid,
                "name": terminology.name,
                "version": terminology.version,
                "effective_start": terminology.effective_start,
                "effective_end": terminology.effective_end,
                "fhir_uri": terminology.fhir_uri,
                "codes": [dataclasses.asdict(code) for code in paginated_codes],
            },
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_codes": len(terminology.codes),
                "total_pages": (len(terminology.codes) + per_page - 1) // per_page,
            },
        }
    )

# @app.route("/terminology/new_version/", methods=["POST"])
# def create_new_term_version():
#     """
#     Create a new terminology version with the provided parameters, such as terminology name, version, FHIR URI,
#     effective start and end dates, previous version UUID, standard status, and FHIR terminology.
#     """
#     if request.method == "POST":
#         terminology = request.json.get("terminology")
#         version = request.json.get("version")
#         fhir_uri = request.json.get("fhir_uri")
#         effective_start = request.json.get("effective_start")
#         effective_end = request.json.get("effective_end")
#         if effective_end == "":
#             effective_end = None
#         previous_version_uuid = request.json.get("previous_version_uuid")
#         is_standard = request.json.get("is_standard")
#         fhir_terminology = request.json.get("fhir_terminology")
#         new_terminology_version = Terminology.create_new_terminology(
#             previous_version_uuid,
#             terminology,
#             version,
#             fhir_uri,
#             is_standard,
#             fhir_terminology,
#             effective_start,
#             effective_end,
#         )
#         new_term_version = Terminology.serialize(new_terminology_version)
#         return new_term_version


@terminologies_blueprint.route("/terminology/new_version_from_previous", methods=["POST"])
def create_new_term_version_from_previous():
    """
    Create a new terminology version from a previous version, preserving its content and metadata while updating
    the version number, effective start and end dates.
    """
    if request.method == "POST":
        previous_terminology_version_uuid = request.json.get(
            "previous_terminology_version_uuid"
        )
        version = request.json.get("version")
        effective_start = request.json.get("effective_start")
        effective_end = request.json.get("effective_end")
        new_terminology_version = Terminology.new_terminology_version_from_previous(
            previous_terminology_version_uuid,
            version,
            effective_start,
            effective_end,
        )
        new_term_version = Terminology.serialize(new_terminology_version)
        return new_term_version