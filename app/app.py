from bdb import effective
from io import StringIO
from uuid import UUID
import re
import logging
from app.helpers.structlog import config_structlog, common_handler
import structlog
import os
from flask import Flask, jsonify, request, Response
from decouple import config
from app.models.value_sets import *
from app.models.concept_maps import *
from app.models.surveys import *
from app.models.patient_edu import *
from werkzeug.exceptions import HTTPException

# Configure the logger when the application is imported. This ensures that
# everything below uses the same configured logger.
config_structlog()
logger = structlog.getLogger()
# This configures _all other loggers_ including every dependent module that
# has logging implemented to have the format defined in the helper module.
root_logger = logging.getLogger().root
root_logger.addHandler(common_handler)


def create_app(script_info=None):

    app = Flask(__name__)
    app.config['MOCK_DB'] = config('MOCK_DB', False)
    app.config['ENABLE_DATADOG_APM'] = config('ENABLE_DATADOG_APM', True)
 
    if app.config['ENABLE_DATADOG_APM']:
        from ddtrace import patch_all
        patch_all()

    @app.route('/ping')
    def ping():
        return 'OK'
    
    @app.errorhandler(HTTPException)
    def handle_exception(e):
        logger.critical(e.description, stack_info=True)
        return jsonify({"message": e.description}), e.code
    
    # Value Set Endpoints
    # FHIR endpoint
    @app.route('/ValueSet/<string:uuid>/$expand')
    def expand_value_set(uuid):
        force_new = request.values.get('force_new') == 'true'
        vs_version = ValueSetVersion.load(uuid)
        vs_version.expand(force_new=force_new)
        return jsonify(vs_version.serialize())

    @app.route('/ValueSets/')
    def get_all_value_sets_metadata():
        active_only = False if request.values.get('active_only') == 'false' else True
        return jsonify(ValueSet.load_all_value_set_metadata(active_only))

    @app.route('/ValueSets/all/')
    def get_all_value_sets():
        status = request.values.get('status').split(',')
        value_sets = ValueSet.load_all_value_sets_by_status(status)
        for x in value_sets: x.expand()
        serialized = [x.serialize() for x in value_sets]
        return jsonify(serialized)

    @app.route('/ValueSets/<string:identifier>/versions/')
    def get_value_set_versions(identifier):
        uuid = ValueSet.name_to_uuid(identifier)
        return jsonify(ValueSet.load_version_metadata(uuid))

    @app.route('/ValueSets/<string:identifier>/versions/new', methods=['POST'])
    def create_new_vs_version(identifier):
        value_set = ValueSet.load(identifier)
        effective_start = request.json.get('effective_start')
        effective_end = request.json.get('effective_end')
        description = request.json.get('description')
        new_version_uuid = value_set.create_new_version(effective_start, effective_end, description)
        return str(new_version_uuid), 201

    @app.route('/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>', methods=['DELETE'])
    def delete_vs_version(value_set_uuid, vs_version_uuid):
        vs_version = ValueSetVersion.load(vs_version_uuid)
        if str(vs_version.value_set.uuid) != str(value_set_uuid):
            raise BadRequest(f"{vs_version_uuid} is not a version of value set with uuid {value_set_uuid}")
        vs_version.delete()
        return "Deleted", 200

    @app.route('/ValueSets/<string:identifier>/most_recent_active_version')
    def get_most_recent_version(identifier):
        uuid = ValueSet.name_to_uuid(identifier)
        version = ValueSet.load_most_recent_active_version(uuid)
        version.expand()
        return jsonify(version.serialize())

    @app.route('/ValueSets/expansions/<string:expansion_uuid>/report')
    def load_expansion_report(expansion_uuid):
        report = ValueSetVersion.load_expansion_report(expansion_uuid)
        file_buffer = StringIO()
        file_buffer.write(report)
        file_buffer.seek(0)
        response = Response(
            file_buffer,
            mimetype="text/plain",
            headers={
            "Content-Disposition": f"attachment; filename={expansion_uuid}-report.csv"
            }
        )
        return response

    @app.route('/ValueSets/rule_set/execute', methods=['POST'])
    def process_rule_set():
        """ Allows for the real-time execution of rules, used on the front-end to preview output of a rule set"""
        rules_input = request.get_json()
        result = execute_rules(rules_input)
        return jsonify(result)

    # Survey Endpoints
    @app.route('/surveys/<string:survey_uuid>')
    def export_survey(survey_uuid):
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
            })
        return response

    # Concept Map Endpoints
    @app.route('/ConceptMaps/<string:version_uuid>')
    def get_concept_map_version(version_uuid):
        concept_map_version = ConceptMapVersion(version_uuid)
        return jsonify(concept_map_version.serialize())

    # Patient Education Endpoints
    @app.route('/PatientEducation/external/', methods=['POST', 'GET'])
    def external_resource_download():
        ex_resource_id = request.json.get('ex_resource_id')
        resource_id = request.json.get('internal_resource_id')
        language = request.json.get('language')
        tenant_id = request.json.get('tenant_id')
        ex_resource = ExternalResource.locate_external_resource(language, ex_resource_id)
        # TODO if resource exists, skip download and jump to link function...
        link_resources = ExternalResource.link_external_and_internal_resources(
            ex_resource_id, resource_id, tenant_id
        )
        return jsonify(ex_resource) if ex_resource and link_resources else "Resource already exists."

    @app.route('/PatientEducation/', methods=['POST', 'GET'])
    def create_local_resource():
        language = request.json.get('language')
        title = request.json.get('title')
        body = request.json.get('body')
        resource = Resource(language, title, body)
        return 'Resource could not be created.' if not resource else jsonify(resource)

    @app.route('/PatientEducation/<resource_id>/', methods=['PATCH', 'GET'])
    def update_local_resource(resource_id):
        language = request.json.get('language')
        title = request.json.get('title')
        body = request.json.get('body')
        status = request.json.get('status')
        resource = Resource(language, title, body, status, resource_id)
        return 'Resource could not be updated, no resource found with that ID.' if not resource else jsonify(resource)

    @app.route('/PatientEducation/<resource_id>/delete', methods=['DELETE', 'GET'])
    def delete_local_resource(resource_id):
        resource = Resource.delete(resource_id)
        return f"{resource} has been removed." if resource else f"Transaction FAILED for {resource}"

    @app.route('/PatientEducation', methods=['GET'])
    def get_all_available_resources():
        # /<resource_uuid>/version/<ex_resource_uuid>
        # return all resources with linked external resources if any are found
        all_resources = Resource.get_all_resources_with_linked()
        return all_resources

    @app.route('/PatientEducation/remove/link', methods=['DELETE', 'GET'])
    def delete_linked_external_resource():  # will we delete or unlink?
        ex_resource_id = request.json.get('external_resource_id')
        Resource.delete_linked_ex_resource(ex_resource_id)

    return app


application = create_app()

if __name__ == '__main__':
    application.run(debug=True, host="0.0.0.0", port=5500)
