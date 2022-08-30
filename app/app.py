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

    @app.route('/ValueSets/', methods=['GET', 'POST'])
    def get_all_value_sets_metadata():
        if request.method == 'GET':
            active_only = False if request.values.get('active_only') == 'false' else True
            return jsonify(ValueSet.load_all_value_set_metadata(active_only))
        if request.method == 'POST':
            name = request.json.get('name')
            title = request.json.get('title')
            publisher = request.json.get('publisher')
            contact = request.json.get('contact')
            value_set_description = request.json.get('description')
            immutable = request.json.get('immutable')
            experimental = request.json.get('experimental')
            purpose = request.json.get('purpose')
            vs_type = request.json.get('type')
            use_case_uuid = request.json.get('use_case_uuid')
            effective_start = request.json.get('effective_start')
            effective_end = request.json.get('effective_end')
            version_description = request.json.get('version_description')

            new_vs = ValueSet.create(
                name = name,
                title = title,
                publisher = publisher,
                contact = contact,
                value_set_description=value_set_description,
                immutable=immutable,
                experimental=experimental,
                purpose=purpose,
                vs_type=vs_type,
                use_case_uuid=use_case_uuid,
                effective_start = effective_start,
                effective_end = effective_end,
                version_description = version_description
            )
            return jsonify(new_vs.serialize())

    @app.route('/ValueSets/<string:identifier>/duplicate', methods=['POST'])
    def duplicate_value_set_and_version(identifier):
        value_set = ValueSet.load(identifier)
        name = request.json.get('name'),
        title = request.json.get('title'),
        contact = request.json.get('contact'),
        value_set_description = request.json.get('value_set_description'),
        purpose = request.json.get('purpose'),
        effective_start = request.json.get('effective_start')
        effective_end = request.json.get('effective_end')
        version_description = request.json.get('version_description')
        duplicated_value_set_uuid = value_set.duplicate_vs(name, title, contact, value_set_description, purpose, effective_start, effective_end, version_description, use_case_uuid=None)
        return str(duplicated_value_set_uuid), 201
    
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

   """  @app.route('/ValueSets/<string:value_set_uuid>', methods=['DELETE'])
    def delet_value_set(value_set_uuid):
        value_set = ValueSet.load(value_set_uuid)
        value_set.delete()
        return "Deleted", 200

    @app.route('/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>', methods=['DELETE'])
    def delete_vs_version(value_set_uuid, vs_version_uuid):
        vs_version = ValueSetVersion.load(vs_version_uuid)
        if str(vs_version.value_set.uuid) != str(value_set_uuid):
            raise BadRequest(f"{vs_version_uuid} is not a version of value set with uuid {value_set_uuid}")
        vs_version.delete()
        return "Deleted", 200 """

    @app.route('/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>/explicitly_included_codes/', methods=['POST', 'GET'])
    def explicitly_included_code_to_vs_version(value_set_uuid, vs_version_uuid):
        if request.method == 'GET':
            vs_version = ValueSetVersion.load(vs_version_uuid)
            explicit_code_inclusions = ExplicitlyIncludedCode.load_all_for_vs_version(vs_version)
            return jsonify([x.serialize() for x in explicit_code_inclusions])

        if request.method == 'POST':
            code_uuid = request.json.get('code_uuid')
            code = Code.load_from_custom_terminology(code_uuid)
            vs_version = ValueSetVersion.load(vs_version_uuid)
            
            new_explicit_code = ExplicitlyIncludedCode(
                code=code,
                value_set_version=vs_version,
                review_status='pending'
            )
            new_explicit_code.save()
            return 'Created', 201

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
    @app.route('/PatientEducation/external/elsevier', methods=['GET', 'POST'])
    def external_resource_download():
        ex_resource_id = request.json.get('ex_resource_id')
        resource_version_id = request.json.get('internal_version_id')
        language = request.json.get('language')
        tenant_id = request.json.get('tenant_id')
        ex_resource = ExternalResource.locate_external_resource(language, ex_resource_id)
        link_res = ExternalResource.link_external_and_internal_resources(
            ex_resource.external_uuid, resource_version_id, tenant_id
        )
        if link_res:
            linked_resources = json.loads(json.dumps(link_res, default=lambda s: vars(s)))
            return linked_resources
        else:
            return "Resource already exists."

    @app.route('/PatientEducation/', methods=['GET', 'POST', 'PATCH'])
    def create_or_find_local_resources():
        if request.method == 'GET':
            all_resources = Resource.get_all_resources_with_linked()
            return jsonify(all_resources)
        if request.method == 'POST':
            language = request.json.get('language')
            title = request.json.get('title')
            body = request.json.get('body')
            resource = Resource(language, title, body)
            return 'Resource could not be created.' if not resource else jsonify(resource)
        if request.method == 'PATCH':
            version_uuid = request.json.get('version_uuid')
            status = request.json.get('status')
            resource = Resource.status_update(version_uuid, status)
            return resource

    @app.route('/PatientEducation/<version_uuid>/', methods=['GET', 'PATCH', 'DELETE'])
    def delete_update_get_local_resource(version_uuid):
        if request.method == 'GET':
            resource_to_get = Resource.get_specific_resource(version_uuid)
            return jsonify(resource_to_get)
        if request.method == 'PATCH':
            language = request.json.get('language')
            title = request.json.get('title')
            body = request.json.get('body')
            status = request.json.get('status')
            version = request.json.get('version')
            resource = Resource(language, title, body, status, version_uuid, version)
            return 'Resource could not be updated, no resource found with that ID.' if not resource else jsonify(resource)
        if request.method == 'DELETE':
            resource = Resource.delete(version_uuid)
            return resource if resource else f"Transaction FAILED for {resource}"

    @app.route('/PatientEducation/<resource_uuid>/version/new', methods=['POST'])
    def create_new_version_local(resource_uuid):
        new_version = Resource.new_version(resource_uuid)
        return jsonify(new_version)

    @app.route('/PatientEducation/remove-link/', methods=['DELETE', 'GET'])
    def delete_linked_external_resource():  # unlink
        ex_resource_id = request.json.get('external_resource_id')
        ExternalResource.delete_linked_ex_resource(ex_resource_id)
        return f"External Resource: {ex_resource_id} has been removed."

    @app.route('/PatientEducation/elsevier/', methods=['GET', 'POST', 'PATCH', 'DELETE'])
    def get_elsevier_only():
        if request.method == 'PATCH':
            status = request.json.get('status')
            _uuid = request.json.get('uuid')
            updated_status = ElsevierOnly.update_status(status, _uuid)
            return jsonify(updated_status) if updated_status else {'message': f'Could not update resource {_uuid}'}
        if request.method == 'POST':
            external_id = request.json.get('external_id')
            patient_term = request.json.get('patient_term')
            language = request.json.get('language')
            tenant_id = request.json.get('tenant_id')
            get_resource = ElsevierOnly(
                external_id, patient_term, language, tenant_id
            )
            return jsonify(get_resource)
        if request.method == 'GET':
            all_resources = ElsevierOnly.get_all_elsevier_only_resources()
            return jsonify(all_resources)
        if request.method == 'DELETE':
            _uuid = request.json.get('uuid')
            remove_link = ElsevierOnly.unlink_resource(_uuid)
            return remove_link

    return app


application = create_app()

if __name__ == '__main__':
    application.run(debug=True, host="0.0.0.0", port=5500)
