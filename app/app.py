import logging
from io import StringIO
from uuid import uuid4

import structlog
from deprecated.classic import deprecated
from flask import Flask, jsonify, request, Response
from werkzeug.exceptions import HTTPException

import app.concept_maps.views as concept_map_views
import app.models.rxnorm as rxnorm
import app.registries.views as registry_views
import app.tasks as tasks
import app.terminologies.views as terminology_views
import app.value_sets.views as value_set_views
from app.database import close_db, rollback_and_close_connection_if_open
from app.errors import BadRequestWithCode, NotFoundException
from app.helpers.structlog import config_structlog, common_handler
from app.models.data_ingestion_registry import (
    DataNormalizationRegistry,
)
from app.models.normalization_error_service import get_outstanding_errors
from app.models.patient_edu import *
from app.models.surveys import *
from app.models.teams import *
from app.value_sets.models import *
import app.concept_maps.models as concept_map_models

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
    app.register_blueprint(registry_views.registries_blueprint)

    @app.route("/ping")
    def ping():
        """Returns a simple 'OK' response to indicate that the service is up and running."""
        return "OK"

    @app.errorhandler(BadRequestWithCode)
    def handle_bad_request_with_code(e):
        """
        Handles BadRequestWithCode exceptions by returning a JSON response with the appropriate error code and message.
        """
        rollback_and_close_connection_if_open()
        return jsonify({"code": e.code, "message": e.description}), e.http_status_code

    @app.errorhandler(BadDataError)
    def handle_bad_data_error(e):
        """
        Handles BadDataError exceptions by returning a JSON response with the appropriate error code and message.
        """
        rollback_and_close_connection_if_open()
        return jsonify({"code": e.code, "message": e.description}), e.http_status_code

    @app.errorhandler(HTTPException)
    def handle_exception(e):
        """
        Handles general HTTPExceptions by returning a JSON response with the appropriate error message and status code.
        """
        rollback_and_close_connection_if_open()
        logger.critical(e.description, stack_info=True)
        return jsonify({"message": e.description}), e.code

    @app.errorhandler(NotFoundException)
    def handle_not_found(e):
        rollback_and_close_connection_if_open()
        return jsonify({"message": e.message}), 404

    # UseCase Endpoints
    @app.route("/usecase/", methods=["GET"])
    def use_case_registry():
        if request.method == "GET":
            all_cases = UseCase.load_all_use_cases()
            return jsonify(all_cases)

    @app.route("/usecase/create/", methods=["POST"])
    # todo: consistent conventions for REST - GET and POST use same route of "/usecase/"
    def create_use_case():
        data = request.get_json()
        name = data.get("name")
        description = data.get("description")
        status = data.get("status")
        if name is None:
            raise BadRequestWithCode(
                "UseCase.name.required",
                "Use Case name was not provided",
            )
        if description is None:
            raise BadRequestWithCode(
                "UseCase.description.required",
                "Use Case description was not provided",
            )
        if status is None:
            raise BadRequestWithCode(
                "UseCase.status.required",
                "Use Case status was not provided",
            )

        conn = get_db()
        existing_use_case = conn.execute(
            text(
                """  
                SELECT * FROM project_management.use_case  
                WHERE name = :name  
                """
            ),
            {"name": name},
        ).fetchone()
        if existing_use_case:
            raise BadRequestWithCode(
                "UseCase.name.duplicate",
                "Cannot create a new Use Case with the same name as an existing one",
            )

        new_uuid = uuid4()

        use_case = UseCase(
            uuid=new_uuid,
            name=name,
            description=description,
            point_of_contact=data.get("point_of_contact"),
            status=status,
            jira_ticket=data.get("jira_ticket"),
            point_of_contact_email=data.get("point_of_contact_email"),
        )

        use_case.save(use_case)
        # todo: consistent conventions for REST - POST success returns the created object
        return jsonify({"message": "UseCase saved successfully"}), 201

    @app.route("/usecases_from_team/", methods=["GET"])
    # todo: consistent conventions for REST - GET and POST use same route "/usecase/" - team_uuid is query param for GET
    def get_use_cases_from_team():
        if request.method == "GET":
            team_uuid_str = request.values.get("team_uuid")
            team_uuid = uuid.UUID(team_uuid_str)
            associated_use_cases = get_use_case_by_team(team_uuid)
            return jsonify(associated_use_cases)

            # Teams Endpoints

    @app.route("/teams/", methods=["GET"])
    def teams_registry():
        if request.method == "GET":
            all_teams = Teams.load_all_teams()
            return jsonify(all_teams)

    @app.route("/teams/linked_use_cases", methods=["GET", "POST"])
    def handle_linked_teams():
        """
            Handle GET and POST requests to fetch or overwrite the teams linked to a specified use case.

            This endpoint allows users to retrieve or update the team(s) associated with a given use case.

            Parameters
            ----------
            use_case_uuid this must be passed as a parameter when using the 'GET' method

            Returns
            -------
            Flask Response
                If the request method is GET, the response will contain a JSON object representing the teams associated with a given use case.
                If the request method is POST, the response will contain a JSON object with a success message.

            Request Body Example (POST)
            ---------------------------
            For a POST request, the request body should be a JSON object with the following structure:

            {
                "use_case_uuid": "bf161bfc-05f2-4b02-bd05-6a51bb884065",
                "teams": [
                    {"name": "interops", "slack_channel": "interops", "team_uuid": "60c121cb-0084-4680-9959-31eacabe5816"},
                    {"name": "data science", "slack_channel": "data-science-team", "team_uuid": "985854a9-2ed0-4ea4-ae34-b320c105707a"}
                ]
            }

        The "teams" field should contain a list of team objects to be linked to the use case.
        Each team object should have the following fields: "name" (the name of the team), "slack_channel" (the slack channel of the team), and "team_uuid" (the UUID of the team).
        """

        # GET is called from Retool.
        if request.method == "GET":
            use_case_uuid = request.values.get("use_case_uuid")
            return jsonify(get_teams_by_use_case(use_case_uuid))

        # Did not find a use of this POST API endpoint from Retool.
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
                set_up_use_case_teams_link(use_case_uuid, team.team_uuid)
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
        If more than one data output schema version is active during a transition between output schema versions,
        the registry is returned using the more recent of the two schema versions.
        """
        if request.method == "GET":
            from_oci = request.values.get("from_oci")
            if from_oci is not None:
                from_oci = from_oci.strip().lower() == "true"
            else:
                from_oci = False

            if from_oci:
                registry = DataNormalizationRegistry.get_last_published_registry()
                return jsonify(registry)

            else:
                registry = DataNormalizationRegistry()
                registry.load_entries()
                return jsonify(registry.serialize())

    @app.route("/data_normalization/registry/actions/publish", methods=["POST"])
    def publish_data_normalization_registry_endpoint():
        """
        Publish the data normalization registry to an object store, allowing other services to access the registry information.
        """
        if request.method == "POST":
            return jsonify(
                DataNormalizationRegistry.publish_data_normalization_registry()
            )

    @app.route("/data_normalization/registry/actions/get_time", methods=["GET"])
    def get_last_published_time():
        """
        Retrieve the last published time of the data normalization registry, indicating when it was most recently updated.
        """
        last_update = DataNormalizationRegistry.get_oci_last_published_time()
        convert_last_update = DataNormalizationRegistry.convert_gmt_time(last_update)
        return convert_last_update

    @app.route("/data_normalization/actions/load_outstanding_errors_to_custom_terminologies", methods=["POST"])
    def trigger_load_outstanding_errors_to_custom_terminologies():
        """
        Trigger the error validation load process via an API call.
        """
        tasks.load_outstanding_errors_to_custom_terminologies.apply_async()
        return "Started"

    @app.route("/data_normalization/outstanding_mapping_rows", methods=["GET"])
    def outstanding_errors():
        errors = get_outstanding_errors()
        return jsonify(errors)

    @app.route(
        "/data_normalization/actions/load_outstanding_codes_to_concept_map",
        methods=["POST"],
    )
    def load_outstanding_codes_to_new_concept_map_version():
        concept_map_uuid = request.json.get("concept_map_uuid")
        tasks.load_outstanding_codes_to_new_concept_map_version(concept_map_uuid)
        return "OK"

    @app.route(
        "/data_normalization/actions/resolve_issues_for_concept_map_version",
        methods=["POST"],
    )
    def resolve_issues_for_concept_map_version():
        """
        We resolve automatically when a new concept map version is published, but we also we need an API for manual runs
        """
        concept_map_version_uuid = request.json.get("concept_map_version_uuid")
        concept_map_version = concept_map_models.ConceptMapVersion(concept_map_version_uuid)
        concept_map_version.resolve_error_service_issues()
        return "Resolved"

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

    @app.route("/CeleryTask/Demo", methods=["GET"])
    def celery_task_demo():
        result = tasks.hello_world.delay()
        return "Task Created"

    return app


application = create_app()

if __name__ == "__main__":
    application.run(debug=True, host="0.0.0.0", port=5500)
