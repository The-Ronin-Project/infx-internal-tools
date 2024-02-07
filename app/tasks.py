from celery import Celery
from celery.schedules import crontab
from uuid import UUID
from decouple import config

from app.errors import NotFoundException
import app.value_sets.models
import app.concept_maps.models
import app.concept_maps.versioning_models
import app.models.mapping_request_service
import app.proofs_of_concept.data_migration
from app.database import get_db


BROKER_HOST = config("CELERY_BROKER_HOST", "localhost")
BROKER_PORT = config("CELERY_BROKER_PORT", "5672")
BROKER_TLS_ENFORCED = bool(config("CELERY_BROKER_TLS_ENFORCED", False))
broker_protocol = "rediss" if BROKER_TLS_ENFORCED else "redis"
BROKER_URL = f"{broker_protocol}://{BROKER_HOST}:{BROKER_PORT}//"
if BROKER_TLS_ENFORCED:
    BROKER_URL += "?ssl_cert_reqs=required"


celery_app = Celery("infx-tasks", broker=BROKER_URL)
celery_app.conf.task_always_eager = config('CELERY_TASK_ALWAYS_EAGER', False)
# celery_app.conf.task_always_eager = True
# celery_app.conf.broker_url = config('CELERY_BROKER_URL')


# Deprecated because error service / mapping request process is being ran from local dev
# @celery_app.on_after_configure.connect
# def setup_periodic_tasks(sender, **kwargs):
#     # Executes every 4hrs
#     sender.add_periodic_task(
#         crontab(hour="4"),
#         load_outstanding_errors_to_custom_terminologies.s(),
#     )


# Deprecated because error service / mapping request process is being ran from local dev
# @celery_app.task
# def load_outstanding_errors_to_custom_terminologies():
#     """ """
#     conn = get_db()
#
#     app.models.mapping_request_service.MappingRequestService.load_concepts_from_errors()
#
#     conn.commit()
#     conn.close()


@celery_app.task
def resolve_errors_after_concept_map_publish(concept_map_version_uuid):
    """ """
    conn = get_db()

    # same code here as for a synchronous api endpoint
    concept_map_version = app.concept_maps.models.ConceptMapVersion(concept_map_version_uuid)
    concept_map_version.resolve_error_service_issues()

    conn.commit()
    conn.close()


@celery_app.task
def load_outstanding_codes_to_new_concept_map_version(concept_map_uuid: str):
    conn = get_db()

    # Get the number of outstanding codes, this is stored and used after a new version has been created
    outstanding_code_count = app.models.mapping_request_service.get_count_of_outstanding_codes(concept_map_uuid)

    # Step 6: look up source and target value set
    
    # Instantiate the concept map object - attributes provide data for a concept map of ANY STATUS
    concept_map = app.concept_maps.models.ConceptMap(concept_map_uuid)  # instantiate object

    # Instantiate the concept map version object - attributes provide data for the most recent version of ANY STATUS
    concept_map_most_recent_version = concept_map.get_most_recent_version(
        active_only=False
    )

    # Get the source_value_set_version_uuid from the concept map object.
    # This source_value_set_version_uuid MAY OR MAY NOT be the most recent version, and MAY OR MAY NOT be active.
    # It is fine b/c every time we use this value, we follow up with load_latest_version() or load_most_recent_version()
    source_value_set_version_uuid = (
        concept_map_most_recent_version.source_value_set_version_uuid
    )

    # Get the target_value_set_uuid from the concept map object.
    # Use it to get the most recent active version of that target value set, if there is one. There must be one.

    target_value_set = app.value_sets.models.ValueSet.load(concept_map.target_value_set_uuid)

    new_target_value_set_version = (
        app.value_sets.models.ValueSet.load_most_recent_active_version(target_value_set.uuid)
    )
    if new_target_value_set_version is None:
        raise NotFoundException(f"Could not find an active version of target value set with UUID {UUID}")

    # Step 7: Create the new value set version
    # Get the most recent version of the terminology
    source_value_set_version = app.value_sets.models.ValueSetVersion.load(source_value_set_version_uuid)
    try:
        terminology_in_source_value_set = (
            source_value_set_version.lookup_terminologies_in_value_set_version()
        )
    except NotFoundException as e:
        raise e
    source_terminology = terminology_in_source_value_set[0]
    source_terminology = source_terminology.load_latest_version()

    new_source_value_set_version_description = (
        "New version created for loading outstanding codes"
    )

    # Create a new version of the value set from the specific version
    source_value_set_most_recent_version = app.value_sets.models.ValueSet.load_most_recent_version(
        uuid=concept_map.source_value_set_uuid
    )
    new_source_value_set_version = (
        app.value_sets.models.ValueSetVersion.create_new_version_from_specified_previous(
            version_uuid=source_value_set_most_recent_version.uuid,
            new_version_description=new_source_value_set_version_description,
            new_terminology_version_uuid=source_terminology.uuid,
        )
    )

    # Step 8: Publish the new value set version
    new_source_value_set_version.publish(force_new=True)

    # Step 9: Create new concept map version
    version_creator = app.concept_maps.versioning_models.ConceptMapVersionCreator()
    new_concept_map_version_description = (
        "New version created for loading outstanding codes"
    )
    require_review_for_non_equivalent_relationships = False
    require_review_no_maps_not_in_target = False

    version_creator.new_version_from_previous(
        previous_version_uuid=concept_map_most_recent_version.uuid,
        new_version_description=new_concept_map_version_description,
        new_source_value_set_version_uuid=new_source_value_set_version.uuid,
        new_target_value_set_version_uuid=new_target_value_set_version.uuid,
        require_review_for_non_equivalent_relationships=require_review_for_non_equivalent_relationships,
        require_review_no_maps_not_in_target=require_review_no_maps_not_in_target,
    )

    # After creating the new version, update the count
    new_version_uuid = version_creator.new_version_uuid
    app.concept_maps.models.ConceptMapVersion.update_loaded_concepts_count(new_version_uuid, outstanding_code_count)

    conn.commit()
    conn.close()
    return version_creator.new_version_uuid


@celery_app.task
def back_fill_concept_maps_to_simplifier():
    active_concept_map_versions_to_push = (
        app.concept_maps.models.ConceptMapVersion.get_active_concept_map_versions()
    )
    for concept_map_version in active_concept_map_versions_to_push:
        concept_map_object = app.concept_maps.models.ConceptMapVersion(concept_map_version)
        concept_map_object.to_simplifier()

    return "Active concept map versions back fill to Simplifier complete."


@celery_app.task
def perform_database_migration(table_name, granularity, segment_start, segment_count):
    app.proofs_of_concept.data_migration.migrate_database_table(
        table_name=table_name,
        granularity=granularity,
        segment_start=segment_start,
        segment_count=segment_count
    )


@celery_app.task
def perform_database_cleanup(table_name, granularity, segment_start, segment_count):
    app.proofs_of_concept.data_migration.cleanup_database_table(
        table_name=table_name,
        granularity=granularity,
        segment_start=segment_start,
        segment_count=segment_count
    )


@celery_app.task
def perform_mapping_request_check(page_size, requested_organization_id, requested_resource_type):
    """
    Last 2 inputs are strings: if omitted, all known possible values are used: all orgids or all resource types
    @param page_size (int) number of records to get from the Data Validation Error Service each time, default PAGE_SIZE
    @param requested_organization_id: Confluence page called "Organization Ids" under "Living Architecture" lists them
    @param requested_resource_type: must be a type load_concepts_from_errors() already supports (see ResourceType enum)
    """
    app.models.mapping_request_service.temporary_mapping_request_service(
        commit_by_batch=True,
        page_size=page_size,
        requested_organization_id=requested_organization_id,
        requested_resource_type=requested_resource_type,
        requested_issue_type=None
    )


@celery_app.task
def hello_world():
    print("Hello, World!")
    return "Hello, World!"
