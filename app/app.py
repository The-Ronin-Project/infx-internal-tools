import io
from bdb import effective
from io import StringIO
import string
from uuid import UUID, uuid4
import logging
from app.helpers.structlog import config_structlog, common_handler
import structlog
import os
from flask import Flask, jsonify, request, Response, make_response
from decouple import config
from app.database import close_db
from app.models.value_sets import *
from app.models.concept_maps import *
from app.models.surveys import *
from app.models.patient_edu import *
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.errors import BadRequestWithCode
import app.models.rxnorm as rxnorm
from werkzeug.exceptions import HTTPException
from app.helpers.oci_helper import (
    oci_authentication,
    folder_path_for_oci,
    folder_in_bucket,
    pre_export_validate,
    save_to_object_store,
    version_set_status_active,
    get_object_type_from_db,
    get_object_type_from_object_store,
    check_for_prerelease_in_published,
    set_up_object_store,
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

    app = Flask(__name__)
    app.config["MOCK_DB"] = config("MOCK_DB", False)
    app.config["ENABLE_DATADOG_APM"] = config("ENABLE_DATADOG_APM", True)

    if app.config["ENABLE_DATADOG_APM"]:
        from ddtrace import patch_all

        patch_all()

    app.teardown_appcontext(close_db)

    @app.route("/ping")
    def ping():
        return "OK"

    @app.errorhandler(BadRequestWithCode)
    def handle_bad_request_with_code(e):
        return jsonify({"code": e.code, "message": e.description}), e.http_status_code

    @app.errorhandler(HTTPException)
    def handle_exception(e):
        logger.critical(e.description, stack_info=True)
        return jsonify({"message": e.description}), e.code

    # Value Set Endpoints
    # FHIR endpoint
    @app.route("/ValueSet/<string:uuid>/$expand")
    def expand_value_set(uuid):
        force_new = request.values.get("force_new") == "true"
        vs_version = ValueSetVersion.load(uuid)
        vs_version.expand(force_new=force_new)
        return jsonify(vs_version.serialize())

    @app.route("/ValueSets/", methods=["GET", "POST"])
    def get_all_value_sets_metadata():
        if request.method == "GET":
            active_only = (
                False if request.values.get("active_only") == "false" else True
            )
            return jsonify(ValueSet.load_all_value_set_metadata(active_only))
        if request.method == "POST":
            name = request.json.get("name")
            title = request.json.get("title")
            publisher = request.json.get("publisher")
            contact = request.json.get("contact")
            value_set_description = request.json.get("value_set_description")
            immutable = request.json.get("immutable")
            experimental = request.json.get("experimental")
            purpose = request.json.get("purpose")
            vs_type = request.json.get("type")
            use_case_uuid = request.json.get("use_case_uuid")
            effective_start = request.json.get("effective_start")
            effective_end = request.json.get("effective_end")
            version_description = request.json.get("version_description")

            new_vs = ValueSet.create(
                name=name,
                title=title,
                publisher=publisher,
                contact=contact,
                value_set_description=value_set_description,
                immutable=immutable,
                experimental=experimental,
                purpose=purpose,
                vs_type=vs_type,
                use_case_uuid=use_case_uuid,
                effective_start=effective_start,
                effective_end=effective_end,
                version_description=version_description,
            )
            return jsonify(new_vs.serialize())

    @app.route("/ValueSets/<string:identifier>/duplicate", methods=["POST"])
    def duplicate_value_set_and_version(identifier):
        value_set = ValueSet.load(identifier)
        name = (request.json.get("name"),)
        title = (request.json.get("title"),)
        contact = (request.json.get("contact"),)
        value_set_description = (request.json.get("value_set_description"),)
        purpose = (request.json.get("purpose"),)
        effective_start = request.json.get("effective_start")
        effective_end = request.json.get("effective_end")
        version_description = request.json.get("version_description")
        duplicated_value_set_uuid = value_set.duplicate_vs(
            name,
            title,
            contact,
            value_set_description,
            purpose,
            effective_start,
            effective_end,
            version_description,
            use_case_uuid=None,
        )
        return str(duplicated_value_set_uuid), 201

    @app.route(
        "/ValueSets/<string:value_set_uuid>/versions/<string:version_uuid>/rules/update_terminology",
        methods=["POST"],
    )
    def update_terminology_version_of_rules_in_value_set(value_set_uuid, version_uuid):
        old_terminology_version_uuid = request.json.get("old_terminology_version_uuid")
        new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")

        value_set_version = ValueSetVersion.load(version_uuid)
        value_set_version.update_rules_for_terminology(
            old_terminology_version_uuid=old_terminology_version_uuid,
            new_terminology_version_uuid=new_terminology_version_uuid,
        )
        return "OK"

    @app.route(
        "/ValueSetRules/<string:rule_uuid>",
        methods=["PATCH"],
    )
    def update_single_rule(rule_uuid):
        new_terminology_version_uuid = request.json.get("new_terminology_version_uuid")
        rule = VSRule.load(rule_uuid)
        rule.update(new_terminology_version_uuid)
        return "OK"

    @app.route(
        "/ValueSets/<string:value_set_uuid>/versions/<string:version_uuid>",
        methods=["PATCH"],
    )
    def version_status_update(value_set_uuid, version_uuid):
        new_status = request.json.get("status")
        vs_version = ValueSetVersion.load(version_uuid)
        vs_version.update(status=new_status)
        return "OK"

    @app.route("/ValueSets/all/")
    def get_all_value_sets():
        status = request.values.get("status").split(",")
        value_sets = ValueSet.load_all_value_sets_by_status(status)
        for x in value_sets:
            x.expand()
        serialized = [x.serialize() for x in value_sets]
        return jsonify(serialized)

    @app.route("/ValueSets/<string:identifier>/versions/")
    def get_value_set_versions(identifier):
        uuid = ValueSet.name_to_uuid(identifier)
        return jsonify(ValueSet.load_version_metadata(uuid))

    @app.route("/ValueSets/<string:identifier>/versions/new", methods=["POST"])
    def create_new_vs_version(identifier):
        value_set = ValueSet.load(identifier)
        effective_start = request.json.get("effective_start")
        effective_end = request.json.get("effective_end")
        description = request.json.get("description")
        new_version_uuid = value_set.create_new_version_from_previous(
            effective_start, effective_end, description
        )
        return str(new_version_uuid), 201

    @app.route("/ValueSets/<string:value_set_uuid>", methods=["DELETE"])
    def delete_value_set(value_set_uuid):
        value_set = ValueSet.load(value_set_uuid)
        value_set.delete()
        return "Deleted", 200

    # @app.route('/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>', methods=['DELETE'])
    # def delete_vs_version(value_set_uuid, vs_version_uuid):
    #     vs_version = ValueSetVersion.load(vs_version_uuid)
    #     if str(vs_version.value_set.uuid) != str(value_set_uuid):
    #         raise BadRequest(f"{vs_version_uuid} is not a version of value set with uuid {value_set_uuid}")
    #     vs_version.delete()
    #     return "Deleted", 200

    @app.route(
        "/ValueSets/<string:value_set_uuid>/versions/<string:vs_version_uuid>/explicitly_included_codes/",
        methods=["POST", "GET"],
    )
    def explicitly_included_code_to_vs_version(value_set_uuid, vs_version_uuid):
        if request.method == "GET":
            vs_version = ValueSetVersion.load(vs_version_uuid)
            explicit_code_inclusions = ExplicitlyIncludedCode.load_all_for_vs_version(
                vs_version
            )
            return jsonify([x.serialize() for x in explicit_code_inclusions])

        if request.method == "POST":
            code_uuid = request.json.get("code_uuid")
            code = Code.load_from_custom_terminology(code_uuid)
            vs_version = ValueSetVersion.load(vs_version_uuid)

            new_explicit_code = ExplicitlyIncludedCode(
                code=code, value_set_version=vs_version, review_status="pending"
            )
            new_explicit_code.save()
            return "Created", 201

    @app.route("/ValueSets/<string:identifier>/most_recent_active_version")
    def get_most_recent_version(identifier):
        uuid = ValueSet.name_to_uuid(identifier)
        version = ValueSet.load_most_recent_active_version(uuid)
        version.expand()
        return jsonify(version.serialize())

    @app.route("/ValueSets/expansions/<string:expansion_uuid>/report")
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
            },
        )
        return response

    @app.route("/ValueSets/rule_set/execute", methods=["POST"])
    def process_rule_set():
        """Allows for the real-time execution of rules, used on the front-end to preview output of a rule set"""
        rules_input = request.get_json()
        result = execute_rules(rules_input)
        return jsonify(result)

    @app.route(
        "/ValueSets/diff",
        methods=["GET"],
    )
    def diff_new_version_against_previous():
        previous_version_uuid = request.json.get("previous_version_uuid")
        new_version_uuid = request.json.get("new_version_uuid")
        diff = ValueSetVersion.diff_for_removed_and_added_codes(
            previous_version_uuid, new_version_uuid
        )
        return jsonify(diff)

    @app.route("/ValueSets/<string:version_uuid>/prerelease", methods=["GET", "POST"])
    def get_value_set_version_prerelease(version_uuid):
        object_type = "value_set"
        if request.method == "POST":
            force_new = request.values.get("force_new") == "true"
            vs_version = ValueSetVersion.load(version_uuid)
            vs_version.expand(force_new=force_new)
            value_set_to_json, initial_path = vs_version.prepare_for_oci()
            value_set_to_datastore = set_up_object_store(
                value_set_to_json, initial_path, folder="prerelease"
            )
            return jsonify(value_set_to_datastore)
        if request.method == "GET":
            value_set = get_object_type_from_db(version_uuid)
            if not value_set:
                return {"message": "value_set uuid not found."}
            value_set_from_object_store = get_object_type_from_object_store(
                object_type, value_set, folder="prerelease"
            )
            return jsonify(value_set_from_object_store)

    @app.route("/ValueSets/<string:version_uuid>/published", methods=["GET", "POST"])
    def get_value_set_version_published(version_uuid):
        object_type = "value_set"
        if request.method == "POST":
            force_new = request.values.get("force_new") == "true"
            vs_version = ValueSetVersion.load(version_uuid)
            vs_version.expand(force_new=force_new)
            value_set_to_json, initial_path = vs_version.prepare_for_oci()

            value_set_to_datastore = set_up_object_store(
                value_set_to_json, initial_path, folder="published"
            )
            version_set_status_active(version_uuid, object_type)
            return jsonify(value_set_to_datastore)
        if request.method == "GET":
            value_set = get_object_type_from_db(version_uuid)
            if not value_set:
                return {"message": "value_set uuid not found."}
            value_set_from_object_store = get_object_type_from_object_store(
                object_type, value_set, folder="published"
            )
            return jsonify(value_set_from_object_store)

    # Survey Endpoints
    @app.route("/surveys/<string:survey_uuid>")
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
            },
        )
        return response

    @app.route('/SourceConcepts/<string:source_concept_uuid>', methods=['PATCH'])
    def update_source_concept(source_concept_uuid):
        comments = request.json.get('comments')
        update_comments_source_concept(
            source_concept_uuid=source_concept_uuid,
            comments=comments
        )
        return "OK"

    # Concept Map Endpoints
    @app.route("/ConceptMaps/actions/new_version_from_previous", methods=["POST"])
    def new_cm_version_from_previous():
        previous_version_uuid = request.json.get("previous_version_uuid")
        new_version_description = request.json.get("new_version_description")
        new_version_num = request.json.get("new_version_num")
        new_source_value_set_version_uuid = request.json.get(
            "new_source_value_set_version_uuid"
        )
        new_target_value_set_version_uuid = request.json.get(
            "new_target_value_set_version_uuid"
        )

        new_version = ConceptMap.new_version_from_previous(
            previous_version_uuid=previous_version_uuid,
            new_version_description=new_version_description,
            new_version_num=new_version_num,
            new_source_value_set_version_uuid=new_source_value_set_version_uuid,
            new_target_value_set_version_uuid=new_target_value_set_version_uuid,
        )
        return "OK"

    @app.route("/ConceptMaps/<string:version_uuid>", methods=["GET"])
    def get_concept_map_version(version_uuid):
        concept_map_version = ConceptMapVersion(version_uuid)
        concept_map_to_json = concept_map_version.serialize()
        return jsonify(concept_map_to_json)

    @app.route("/ConceptMaps/<string:version_uuid>/actions/index", methods=["POST"])
    def index_targets(version_uuid):
        target_value_set_version_uuid = request.json.get(
            "target_value_set_version_uuid"
        )
        ConceptMap.index_targets(
            version_uuid, target_value_set_version_uuid=target_value_set_version_uuid
        )
        return "OK"

    @app.route("/ConceptMaps/", methods=["POST"])
    def create_initial_concept_map_and_version_one():
        if request.method == "POST":
            name = request.json.get("name")
            title = request.json.get("title")
            publisher = request.json.get("publisher")
            author = request.json.get("author")
            cm_description = request.json.get("cm_description")
            experimental = request.json.get("experimental")
            use_case_uuid = request.json.get("use_case_uuid")
            source_value_set_uuid = request.json.get("source_value_set_uuid")
            target_value_set_uuid = request.json.get("target_value_set_uuid")
            cm_version_description = request.json.get("cm_version_description")
            source_value_set_version_uuid = request.json.get(
                "source_value_set_version_uuid"
            )
            target_value_set_version_uuid = request.json.get(
                "target_value_set_version_uuid"
            )

            new_cm = ConceptMap.initial_concept_map_creation(
                name=name,
                title=title,
                publisher=publisher,
                author=author,
                use_case_uuid=use_case_uuid,
                cm_description=cm_description,
                experimental=experimental,
                source_value_set_uuid=source_value_set_uuid,
                target_value_set_uuid=target_value_set_uuid,
                cm_version_description=cm_version_description,
                source_value_set_version_uuid=source_value_set_version_uuid,
                target_value_set_version_uuid=target_value_set_version_uuid,
            )
            return jsonify(ConceptMap.serialize(new_cm))

    @app.route("/ConceptMaps/<string:version_uuid>/draft", methods=["GET"])
    def get_concept_map_draft(version_uuid):
        concept_map_version = ConceptMapVersion(version_uuid)
        csv_data, csv_field_names = concept_map_version.mapping_draft()
        # Create a CSV file-like object in memory
        si = io.StringIO()
        cw = csv.DictWriter(si, fieldnames=csv_field_names)
        cw.writeheader()
        cw.writerows(csv_data)

        # Return the CSV file as a response
        output = make_response(si.getvalue())
        filename = concept_map_version.concept_map.title.translate(
            str.maketrans("", "", string.whitespace)
        )
        output.headers[
            "Content-Disposition"
        ] = f"attachment; filename={filename}-{concept_map_version.version}draft.csv"
        output.headers["Content-type"] = "text/csv"
        return output

    @app.route("/ConceptMaps/<string:version_uuid>/prerelease", methods=["GET", "POST"])
    def get_concept_map_version_prerelease(version_uuid):
        object_type = "concept_map"
        if request.method == "POST":
            concept_map_version = ConceptMapVersion(
                version_uuid
            )  # using the version uuid to get the version metadata from the ConceptMapVersion class
            (
                concept_map_to_json,
                initial_path,
            ) = concept_map_version.prepare_for_oci()  # serialize the metadata
            concept_map_to_datastore = set_up_object_store(  # use the serialized data with an oci_helper function
                concept_map_to_json, initial_path, folder="prerelease"
            )
            return jsonify(
                concept_map_to_datastore
            )  # returns the serialized metadata posted to OCI
        if request.method == "GET":
            concept_map = get_object_type_from_db(  # concept_map will be a dictionary of overall concept_map uuid and version integer
                version_uuid, object_type
            )  # use the version uuid with an oci_helper function to check DB first
            if not concept_map:
                return {"message": "concept map uuid not found."}  # error if not in DB
            concept_map_from_object_store = get_object_type_from_object_store(  # uses the return from the above oci_helper function to call another  oci_helper function
                object_type, concept_map, folder="prerelease"
            )
            return jsonify(concept_map_from_object_store)  # returns the file from OCI

    @app.route("/ConceptMaps/<string:version_uuid>/published", methods=["GET", "POST"])
    def get_concept_map_version_published(version_uuid):
        object_type = "concept_map"
        if request.method == "POST":
            concept_map_version = ConceptMapVersion(
                version_uuid
            )  # using the version uuid to get the version metadata from the ConceptMapVersion class
            (
                concept_map_to_json,
                initial_path,
            ) = concept_map_version.prepare_for_oci()  # serialize the metadata
            concept_map_to_datastore = set_up_object_store(  # use the serialized data with an oci_helper function
                concept_map_to_json, initial_path, folder="published"
            )
            version_set_status_active(version_uuid, object_type)
            return jsonify(concept_map_to_datastore)
        if request.method == "GET":
            concept_map = get_object_type_from_db(version_uuid, object_type)
            if not concept_map:
                return {"message": "concept map uuid not found."}
            concept_map_from_object_store = get_object_type_from_object_store(
                object_type, concept_map, folder="published"
            )
            return jsonify(concept_map_from_object_store)

    @app.route("/ConceptMapSuggestions/", methods=["POST"])
    def mapping_suggestion():
        if request.method == "POST":
            source_concept_uuid = request.json.get("source_concept_uuid")
            code = request.json.get("code")
            display = request.json.get("display")
            terminology_version_uuid = request.json.get("terminology_version_uuid")
            suggestion_source = request.json.get("suggestion_source")
            confidence = request.json.get("confidence")
            new_uuid = uuid4()

            terminology_version = Terminology.load(terminology_version_uuid)
            code = Code(
                system=terminology_version.fhir_uri,
                version=terminology_version.version,
                code=code,
                display=display,
                terminology_version=terminology_version,
            )

            new_suggestion = MappingSuggestion(
                uuid=new_uuid,
                source_concept_uuid=source_concept_uuid,
                code=code,
                suggestion_source=suggestion_source,
                confidence=confidence,
            )

            new_suggestion.save()

            return jsonify(new_suggestion.serialize())

    @app.route("/mappings/", methods=["POST"])
    def create_concept_map():
        if request.method == "POST":
            source_concept_uuid = request.json.get("source_concept_uuid")
            relationship_code_uuid = request.json.get("relationship_code_uuid")
            target_concept_code = request.json.get("target_concept_code")
            target_concept_display = request.json.get("target_concept_display")
            target_concept_terminology_version_uuid = request.json.get(
                "target_concept_terminology_version_uuid"
            )
            mapping_comments = request.json.get("mapping_comments")
            author = request.json.get("author")
            review_status = request.json.get("review_status")

            source_code = Code.load_concept_map_source_concept(source_concept_uuid)

            relationship = MappingRelationship.load(relationship_code_uuid)

            target_code = Code(
                code=target_concept_code,
                display=target_concept_display,
                system=None,
                version=None,
                terminology_version=target_concept_terminology_version_uuid,
            )

            new_mapping = Mapping(
                source=source_code,
                relationship=relationship,
                target=target_code,
                mapping_comments=mapping_comments,
                author=author,
                review_status=review_status,
            )
            new_mapping.save()

            return jsonify(new_mapping.serialize())

    # Patient Education Endpoints
    @app.route("/PatientEducation/", methods=["GET", "POST", "PATCH", "DELETE"])
    def get_external_resources():
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
        if request.method == "POST":
            _uuid = request.json.get("uuid")
            export = ExternalResource.format_data_to_export(_uuid)
            return jsonify(export)

    # RxNorm custom search
    @app.route("/rxnorm_search", methods=["GET"])
    def rxnorm_search():
        query_string = request.values.get("query_string")
        return jsonify(rxnorm.exact_with_approx_fallback_search(query_string))

    # Registry
    @app.route("/data_normalization/registry", methods=["GET"])
    def data_ingestion_registry():
        if request.method == "GET":
            registry = DataNormalizationRegistry()
            registry.load_entries()
            return jsonify(registry.serialize())

    @app.route("/data_normalization/registry/actions/publish", methods=["POST"])
    def publish_data_normalization_registry():
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
        last_update = DataNormalizationRegistry.get_oci_last_published_time()
        convert_last_update = DataNormalizationRegistry.convert_gmt_time(last_update)
        return convert_last_update

    @app.route("/terminology/", methods=["POST"])
    def create_terminology():
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

    @app.route("/terminology/new_code", methods=["POST"])
    def create_code():
        if request.method == "POST":
            payload = request.json
            new_code = Code.add_new_code_to_terminology(payload)
            codes = []
            for x in new_code:
                code = Code.serialize(x)
                codes.append(code)
            return codes

    @app.route("/terminology/new_version/", methods=["POST"])
    def create_new_term_version():
        if request.method == "POST":
            terminology = request.json.get("terminology")
            version = request.json.get("version")
            fhir_uri = request.json.get("fhir_uri")
            effective_start = request.json.get("effective_start")
            effective_end = request.json.get("effective_end")
            if effective_end == "":
                effective_end = None
            previous_version_uuid = request.json.get("previous_version_uuid")
            is_standard = request.json.get("is_standard")
            fhir_terminology = request.json.get("fhir_terminology")
            new_terminology_version = Terminology.insert_new_terminology_version(
                previous_version_uuid,
                terminology,
                version,
                fhir_uri,
                is_standard,
                fhir_terminology,
                effective_start,
                effective_end,
            )
            new_term_version = Terminology.serialize(new_terminology_version)
            return new_term_version

    @app.route("/terminology/new_version_from_previous", methods=["POST"])
    def create_new_term_version_from_previous():
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

    return app


application = create_app()

if __name__ == "__main__":
    application.run(debug=True, host="0.0.0.0", port=5500)
