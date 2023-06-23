import datetime

from celery import Celery

from uuid import UUID
from app.models.normalization_error_service import (
    load_concepts_from_errors,
    lookup_concept_map_version_for_resource_type,
)
from app.value_sets.models import ValueSetVersion
from app.concept_maps.models import ConceptMap
from app.terminologies.models import Terminology
from app.concept_maps.versioning_models import ConceptMapVersionCreator
from app.helpers.oci_helper import set_up_object_store
from app.helpers.simplifier_helper import publish_to_simplifier
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.models.normalization_error_service import load_concepts_from_errors
from flask import jsonify

app = Celery("infx-tasks")
app.conf.task_always_eager = True


def load_outstanding_errors_to_custom_terminologies():
    """ """
    load_concepts_from_errors()


def load_outstanding_codes_to_new_concept_map_version(concept_map_uuid: UUID):

    # Step 6: look up source and target value set
    concept_map = ConceptMap(concept_map_uuid)  # instantiate object
    # Get the source and target value set version UUIDs from the most_recent_active_version attribute
    source_value_set_version_uuid = (
        concept_map.most_recent_active_version.source_value_set_version_uuid
    )
    target_value_set_version_uuid = (
        concept_map.most_recent_active_version.target_value_set_version_uuid
    )

    # Step 7: Create the new value set version
    # Get the most recent version of the terminology
    terminologies = Terminology.load_terminologies_for_value_set_version(
        source_value_set_version_uuid
    )
    # Sort the terminologies by version in descending order and get the first item
    most_recent_terminology = terminologies[0]

    new_source_value_set_version_description = (
        "New version created for loading outstanding codes"
    )

    # Create a new version of the value set from the specific version
    new_source_value_set_version = (
        ValueSetVersion.create_new_version_from_specified_previous(
            source_value_set_version_uuid,
            new_source_value_set_version_description,
            most_recent_terminology,
        )
    )

    # Step 8: Publish the new value set version
    new_source_value_set_version.expand(force_new=True)
    value_set_to_json, initial_path = new_source_value_set_version.prepare_for_oci()
    value_set_to_json_copy = value_set_to_json.copy()  # Simplifier requires status

    value_set_to_datastore = set_up_object_store(
        value_set_to_json, initial_path, folder="published"
    )

    new_source_value_set_version.version_set_status_active()
    new_source_value_set_version.retire_and_obsolete_previous_version()
    value_set_uuid = new_source_value_set_version.value_set.uuid
    resource_type = "ValueSet"  # param for Simplifier
    value_set_to_json_copy["status"] = "active"
    # Check if the 'expansion' and 'contains' keys are present
    if (
        "expansion" in value_set_to_json_copy
        and "contains" in value_set_to_json_copy["expansion"]
    ):
        # Store the original total value
        original_total = value_set_to_json_copy["expansion"]["total"]

        # Limit the contains list to the top 50 entries
        value_set_to_json_copy["expansion"]["contains"] = value_set_to_json[
            "expansion"
        ]["contains"][:50]

        # Set the 'total' field to the original total
        value_set_to_json_copy["expansion"]["total"] = original_total
    publish_to_simplifier(resource_type, value_set_uuid, value_set_to_json_copy)

    # Publish new version of data normalization registry
    try:
        DataNormalizationRegistry.publish_data_normalization_registry()
    except:
        pass

    # Step 9: Create new concept map version
    version_creator = ConceptMapVersionCreator()
    new_concept_map_version_description = (
        "New version created for loading outstanding codes"
    )
    require_review_for_non_equivalent_relationships = False
    require_review_no_maps_not_in_target = False

    ConceptMapVersionCreator.new_version_from_previous(
        concept_map.most_recent_active_version,
        new_concept_map_version_description,
        new_source_value_set_version,
        target_value_set_version_uuid,
        require_review_for_non_equivalent_relationships,
        require_review_no_maps_not_in_target,
    )
    return version_creator.new_version_uuid