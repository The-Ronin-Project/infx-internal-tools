from deprecated.classic import deprecated
from flask import Blueprint, request, jsonify
import dataclasses

from app.models.codes import *
from app.terminologies.models import *

terminologies_blueprint = Blueprint("terminologies", __name__)


@terminologies_blueprint.route("/terminology/", methods=["POST", "GET"])
def create_terminology():
    """
    Create a new terminology with the provided parameters, such as terminology name, version, effective start and end dates,
    FHIR URI, standard status, and FHIR terminology.
    """

    # Did not find a use of this GET API endpoint from Retool. Not aware of a need to offer a GET API.
    if request.method == "GET":
        fhir_uri = request.values.get("fhir_uri")
        version = request.values.get("version")

        if not fhir_uri or not version:
            return (
                jsonify({"error": "fhir_uri and version parameters are required."}),
                400,
            )

        terminology = Terminology.load_by_fhir_uri_and_version(fhir_uri, version)

        if not terminology:
            return (
                jsonify({"error": "Terminology not found with the given parameters."}),
                404,
            )

        return jsonify(terminology.serialize())

    # POST is called from Retool.
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
        if new_terminology is None:
            raise BadRequestWithCode(
                "Terminology.create",
                f"Unable to create terminology {terminology} {version} with fhir_uri: {fhir_uri}, is_standard: {is_standard}, new_terminology: {new_terminology}"
            )
        term = Terminology.serialize(new_terminology)
        return term


@terminologies_blueprint.route("/terminology/new_code", methods=["POST"])
def create_code():
    """
    Add one or multiple new codes to a terminology, providing information such as code system, version, code, display,
    and terminology version.
    Here's a sample json for testing this endpoint:
    {
    "code": "test code",
    "display": "test display",
    "terminology_version_uuid": "d2ae0de5-0168-4f54-924a-1f79cf658939",
    "additional_data": {
        "data": "sweet sweet json"
        }
    }
    """
    if request.method == "POST":
        payload = request.json
        # If the payload is a dictionary, we will make it into a list.
        if isinstance(payload, dict):
            payload = [payload]

        terminology_version_uuids = [
            code_data.get("terminology_version_uuid") for code_data in payload
        ]
        terminology_version_uuids = list(set(terminology_version_uuids))
        if len(terminology_version_uuids) > 1:
            raise BadRequestWithCode(
                "MultipleTerminologiesNotAllowed",
                "Cannot load to multiple terminologies at the same time",
            )
        else:
            terminology = Terminology.load(terminology_version_uuids[0])

        codes = []
        for code_data in payload:
            code = Code(
                code=code_data.get("code"),
                display=code_data.get("display"),
                terminology_version_uuid=code_data.get("terminology_version_uuid"),
                additional_data=code_data.get("additional_data"),
                system=None,
                version=None,
            )
            codes.append(code)

        terminology.load_new_codes_to_terminology(codes)
        return "Complete"


@deprecated("Did not find a use of this API endpoint from Retool. Not aware of a need to offer an API.")
@terminologies_blueprint.route(
    "/terminology/<terminology_version_uuid>", methods=["GET"]
)
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


@terminologies_blueprint.route(
    "/terminology/new_version_from_previous", methods=["POST"]
)
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
