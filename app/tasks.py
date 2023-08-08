from celery import Celery
from uuid import UUID
from app.models.normalization_error_service import (
    load_concepts_from_errors,
    lookup_concept_map_version_for_data_element,
)
from app.value_sets.models import ValueSetVersion, ValueSet
from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.terminologies.models import Terminology
from app.concept_maps.versioning_models import ConceptMapVersionCreator
from app.models.normalization_error_service import load_concepts_from_errors
from app.database import get_db

celery_app = Celery("infx-tasks")
celery_app.conf.task_always_eager = True


def load_outstanding_errors_to_custom_terminologies():
    """ """
    conn = get_db()

    load_concepts_from_errors()

    conn.commit()
    conn.close()


@celery_app.task
def load_outstanding_codes_to_new_concept_map_version(concept_map_uuid: UUID):
    conn = get_db()

    # Step 6: look up source and target value set
    concept_map = ConceptMap(concept_map_uuid)  # instantiate object

    concept_map_most_recent_version = concept_map.get_most_recent_version(
        active_only=False
    )

    # Get the source and target value set version UUIDs from the most_recent_active_version attribute
    source_value_set_version_uuid = (
        concept_map_most_recent_version.source_value_set_version_uuid
    )
    target_value_set_version_uuid = (
        concept_map_most_recent_version.target_value_set_version_uuid
    )

    # Step 7: Create the new value set version
    # Get the most recent version of the terminology
    source_value_set_version = ValueSetVersion.load(source_value_set_version_uuid)
    terminology_in_source_value_set = (
        source_value_set_version.lookup_terminologies_in_value_set_version()
    )
    source_terminology = terminology_in_source_value_set[0]
    source_terminology = source_terminology.load_latest_version()

    new_source_value_set_version_description = (
        "New version created for loading outstanding codes"
    )

    # Create a new version of the value set from the specific version
    source_value_set_most_recent_version = ValueSet.load_most_recent_version(
        uuid=concept_map.source_value_set_uuid
    )
    new_source_value_set_version = (
        ValueSetVersion.create_new_version_from_specified_previous(
            version_uuid=source_value_set_most_recent_version.uuid,
            new_version_description=new_source_value_set_version_description,
            new_terminology_version_uuid=source_terminology.uuid,
        )
    )

    # Step 8: Publish the new value set version
    new_source_value_set_version.publish(force_new=True)

    # Step 9: Create new concept map version
    version_creator = ConceptMapVersionCreator()
    new_concept_map_version_description = (
        "New version created for loading outstanding codes"
    )
    require_review_for_non_equivalent_relationships = False
    require_review_no_maps_not_in_target = False

    version_creator.new_version_from_previous(
        previous_version_uuid=concept_map_most_recent_version.uuid,
        new_version_description=new_concept_map_version_description,
        new_source_value_set_version_uuid=new_source_value_set_version.uuid,
        new_target_value_set_version_uuid=target_value_set_version_uuid,
        require_review_for_non_equivalent_relationships=require_review_for_non_equivalent_relationships,
        require_review_no_maps_not_in_target=require_review_no_maps_not_in_target,
    )
    conn.commit()
    conn.close()
    return version_creator.new_version_uuid


@celery_app.task
def back_fill_concept_maps_to_simplifier():
    active_concept_map_versions_to_push = (
        ConceptMapVersion.get_active_concept_map_versions()
    )
    for concept_map_version in active_concept_map_versions_to_push:
        concept_map_object = ConceptMapVersion(concept_map_version)
        concept_map_object.to_simplifier()

    return "Active concept map versions back fill to Simplifier complete."


@celery_app.task
def hello_world():
    return "Hello, World!"
