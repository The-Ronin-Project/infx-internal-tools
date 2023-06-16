import dataclasses
import io
from io import StringIO
import string
from uuid import uuid4
import logging
from app.helpers.structlog import config_structlog, common_handler
import structlog
from flask import Flask, jsonify, request, Response, make_response
from app.database import close_db
from app.value_sets.models import *
from app.concept_maps.models import *
from app.models.use_case import *
from app.models.teams import *
from app.models.surveys import *
from app.models.patient_edu import *
from app.models.teams import *
from app.concept_maps.versioning_models import *
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.errors import BadRequestWithCode
from app.helpers.simplifier_helper import (
    publish_to_simplifier,
)
import app.value_sets.views as value_set_views
import app.concept_maps.views as concept_map_views
import app.terminologies.views as terminology_views

import app.models.rxnorm as rxnorm
from werkzeug.exceptions import HTTPException
from app.helpers.oci_helper import (
    version_set_status_active,
    get_object_type_from_db,
    get_object_type_from_object_store,
    set_up_object_store,
    get_json_from_oci,
)

# Configure the logger when the application is imported. This ensures that
# everything below uses the same configured logger.
config_structlog()
logger = structlog.getLogger()
# This configures _all other loggers_ including every dependent module that
# has logging implemented to have the format defined in the helper module.
root_logger = logging.getLogger().root
root_logger.addHandler(common_handler)


def create_app(script_info=None):
    """
    Initializes and returns the Flask app instance.
    Sets up configurations and registers routes and error handlers.
    """
    app = Flask(__name__)
    app.config["MOCK_DB"] = config("MOCK_DB", False)
    app.config["ENABLE_DATADOG_APM"] = config("ENABLE_DATADOG_APM", True)

    if app.config["ENABLE_DATADOG_APM"]:
        from ddtrace import patch_all

        patch_all()

    app.teardown_appcontext(close_db)
    app.register_blueprint(value_set_views.value_sets_blueprint)
    app.register_blueprint(concept_map_views.concept_maps_blueprint)
    app.register_blueprint(terminology_views.terminologies_blueprint)

    @app.route("/ping")
    def ping():
        """Returns a simple 'OK' response to indicate that the service is up and running."""
        return "OK"

    @app.errorhandler(BadRequestWithCode)
    def handle_bad_request_with_code(e):
        """Handles BadRequestWithCode exceptions by returning a JSON response with the appropriate error code and message."""
        return jsonify({"code": e.code, "message": e.description}), e.http_status_code

    @app.errorhandler(HTTPException)
    def handle_exception(e):
        """Handles general HTTPExceptions by returning a JSON response with the appropriate error message and status code."""
        logger.critical(e.description, stack_info=True)
        return jsonify({"message": e.description}), e.code

    # UseCase Endpoints
    @app.route("/UseCase/", methods=["GET"])
    def use_case_registry():
        if request.method == "GET":
            all_cases = UseCase.load_all_use_cases()
            return jsonify(all_cases)

    # Teams Endpoints
    @app.route("/Teams/", methods=["GET"])
    def teams_registry():
        if request.method == "GET":
            all_teams = Teams.load_all_teams()
            return jsonify(all_teams)

    @app.route("/Teams/linked_use_case", methods=["GET", "POST"])
    def handle_linked_teams():
        if request.method == "GET":
            use_case_uuid = request.json.get("use_case_uuid")
            return jsonify(Teams.get_teams_by_use_case(use_case_uuid))
        if request.method == "POST":
            use_case_uuid = request.json.get("use_case_uuid")
            delete_all_teams_for_a_use_case(use_case_uuid)
            teams_to_associate = request.json.get("teams")
            for team_data in teams_to_associate:
                team = Teams(
                    name=team_data["name"],
                    slack_channel=team_data["slack_channel"],
                    team_uuid=uuid.UUID(team_data["team_uuid"]),
                )
                team.save()
            return jsonify({"status": "success"}), 200

            # Survey Endpoints

    @app.route("/surveys/<string:survey_uuid>")
    def export_survey(survey_uuid):
        """
        Export a survey identified by the survey_uuid as a CSV file.
        Returns the survey data as a CSV file attachment.
        """
        organization_uuid = request.values.get("organization_uuid")
        # print(survey_uuid, organization_uuid)
        exporter = SurveyExporter(survey_uuid, organization_uuid)

        file_buffer = StringIO()
        exporter.export_survey().to_csv(file_buffer)
        file_buffer.seek(0)
        response = Response(
            file_buffer,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={exporter.survey_title} {exporter.organization_name}.csv"
            },
        )
        return response

        # Patient Education Endpoints

    @app.route("/PatientEducation/", methods=["GET", "POST", "PATCH", "DELETE"])
    def get_external_resources():
        """
        Manage external resources using GET, POST, PATCH, and DELETE methods.
        GET: Retrieve all external resources.
        POST: Create a new external resource.
        PATCH: Update the status of an external resource.
        DELETE: Unlink an external resource.
        """
        if request.method == "PATCH":
            status = request.json.get("status")
            _uuid = request.json.get("uuid")
            updated_status = ExternalResource.update_status(status, _uuid)
            return (
                jsonify(updated_status)
                if updated_status
                else {"message": f"Could not update resource {_uuid}"}
            )
        if request.method == "POST":
            external_id = request.json.get("external_id")
            patient_term = request.json.get("patient_term")
            language = request.json.get("language")
            tenant_id = request.json.get("tenant_id")
            get_resource = ExternalResource(
                external_id, patient_term, language, tenant_id
            )
            return jsonify(get_resource)
        if request.method == "GET":
            all_resources = ExternalResource.get_all_external_resources()
            return jsonify(all_resources)
        if request.method == "DELETE":
            _uuid = request.json.get("uuid")
            remove_link = ExternalResource.unlink_resource(_uuid)
            return remove_link

    @app.route("/PatientEducation/export", methods=["POST"])
    def export_data():
        """Export data for a specified UUID."""
        if request.method == "POST":
            _uuid = request.json.get("uuid")
            export = ExternalResource.format_data_to_export(_uuid)
            return jsonify(export)

    # RxNorm custom search
    @app.route("/rxnorm_search", methods=["GET"])
    def rxnorm_search():
        """
        Perform an RxNorm search with an exact match or an approximate fallback search.
        """
        query_string = request.values.get("query_string")
        return jsonify(rxnorm.exact_with_approx_fallback_search(query_string))

    # Registry
    @app.route("/data_normalization/registry", methods=["GET"])
    def data_ingestion_registry():
        """
        Retrieve the data ingestion registry, which contains metadata about data sources and their ingestion processes.
        """
        if request.method == "GET":
            registry = DataNormalizationRegistry()
            registry.load_entries()
            return jsonify(registry.serialize())

    @app.route("/data_normalization/registry/actions/publish", methods=["POST"])
    def publish_data_normalization_registry_endpoint():
        """
        Publish the data normalization registry to an object store, allowing other services to access the registry information.
        """
        if request.method == "POST":
            post_registry = DataNormalizationRegistry()
            post_registry.load_entries()
            all_registries = post_registry.serialize()
            registries_to_post = DataNormalizationRegistry.publish_to_object_store(
                all_registries
            )
            return jsonify(registries_to_post)

    @app.route("/data_normalization/registry/actions/get_time", methods=["GET"])
    def get_last_published_time():
        """
        Retrieve the last published time of the data normalization registry, indicating when it was most recently updated.
        """
        last_update = DataNormalizationRegistry.get_oci_last_published_time()
        convert_last_update = DataNormalizationRegistry.convert_gmt_time(last_update)
        return convert_last_update

    @app.route("/TerminologyUpdate/ValueSets/report", methods=["GET"])
    def terminology_update_value_set_report():
        """
        Generate a report for updating value sets in a terminology, showing information about changes to value sets
        and their impact on the terminology.
        """
        terminology_fhir_uri = request.values.get("terminology_fhir_uri")
        exclude_version = request.values.get("exclude_version")
        if request.method == "GET":
            report = value_sets_terminology_update_report(
                terminology_fhir_uri, exclude_version
            )
            return jsonify(report)

    @app.route("/TerminologyUpdate/ValueSets/actions/perform_update", methods=["POST"])
    def perform_terminology_update_for_value_sets():
        old_terminology_version_uuid = request.json.get("old_terminology_version_uuid")
        new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")

        result = perform_terminology_update_for_all_value_sets(
            old_terminology_version_uuid=old_terminology_version_uuid,
            new_terminology_version_uuid=new_terminology_version_uuid,
        )

        return jsonify(result)

    return app


application = create_app()

if __name__ == "__main__":
    application.run(debug=True, host="0.0.0.0", port=5500)
