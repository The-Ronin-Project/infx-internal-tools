import datetime

from celery import Celery

from uuid import UUID

from app.errors import BadRequestWithCode
from app.models.normalization_error_service import (
    load_concepts_from_errors,
    lookup_concept_map_version_for_resource_type,
)
from app.value_sets.models import ValueSetVersion
from app.terminologies.models import Terminology

app = Celery("infx-tasks")
app.conf.task_always_eager = True


def load_outstanding_errors_to_custom_terminologies():
    """ """
    pass


def load_outstanding_codes_to_new_concept_map_version(concept_map_uuid: UUID):
    # Step 6: Create the new value set version
    new_source_value_set_version = source_value_set_version.new_version(
        "new version created by the condition incremental load system"
    )

    if new_terminology_version_created:
        new_source_value_set_version.update_rules_for_new_terminology_version(
            # todo: only applicable if you had to create a new terminology version
            old_terminology_version_uuid=None,
            new_terminology_version_uuid=None,
        )

    # todo: Publish new value set version
    new_source_value_set_version.publish()

    # todo: Create new concept map version
    new_concept_map_version = concept_map_version.new_version(
        new_version_description="Automatically created concept map version from condition incremental load",
        new_source_value_set_version_uuid=new_source_value_set_version.additional_data.get(
            "version_uuid"
        ),
        new_target_value_set_version_uuid=target_value_set_version.additional_data.get(
            "version_uuid"
        ),
    )

    # todo: register new data in appropriate queue for triaging
