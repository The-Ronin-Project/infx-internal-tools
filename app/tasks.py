from celery import Celery

from uuid import UUID

from app.models.normalization_error_service import load_concepts_from_errors, lookup_concept_map_version_for_resource_type
from app.value_sets.models import ValueSetVersion

app = Celery('infx-tasks')
app.conf.task_always_eager = True


def load_outstanding_errors_to_custom_terminologies():
    """

    """
    # Step 1: Retrieve new concepts from errors which need to get into concept maps
    concepts_to_load = load_concepts_from_errors()

    for key, concept_list in concepts_to_load.items():
        organization, resource_type = key

        # Step 2: Identify which Concept Map the new concepts should be added to
        # This is done w/ a lookup in the data normalization registry using the resource type and tenant/organization id
        concept_map_version = lookup_concept_map_version_for_resource_type(
            resource_type=resource_type,  # This would be condition, medication, observation, etc..
            organization=organization  # This is the term for tenant/hospital
        )

        # Step 3: Identify the value sets involved with the concept map
        source_value_set_version = ValueSetVersion.load(concept_map_version.source_value_set_version_uuid)
        target_value_set_version = ValueSetVersion.load(concept_map_version.target_value_set_version_uuid)

        # TODO: verify source and target value set versions are the latest versions

        # # Data integrity check on the value sets
        # source_response = requests.get(f"{BASE_URL}/ValueSets/{source_value_set_version}/most_recent_active_version")
        # target_response = requests.get(f"{BASE_URL}/ValueSets/{target_value_set_version}/most_recent_active_version")
        #
        # if source_response.status_code == 200 and target_response.status_code == 200:
        #     latest_source_version = source_response.json()
        #     latest_target_version = target_response.json()
        #
        #     # Check if the given versions match the latest versions
        #     return (
        #             source_value_set_version.uuid == latest_source_version["uuid"]
        #             and target_value_set_version.uuid == latest_target_version["uuid"]
        #     )
        #
        # return False

        # Step 4: Identify the client terminology from the source value set that we need to load the new concepts to
        source_terminologies = source_value_set_version.lookup_terminologies_in_value_set_version()

        # Data integrity check: only one terminology in the source value set
        # This is important because the only codes in the source value set should be from a single client terminology
        # If this is not the case, something is wrong with the value set and we don't want to proceed and make it worse
        if len(source_terminologies) > 1:
            raise Exception('Multiple terminologies in source value set; cannot automatically add codes')

        # If there's only one, that's the terminology we want to load the new concepts to
        source_terminology = source_terminologies[0]

        # Step 5: Load the new concepts to the relevant client terminology

        # Terminologies have a certain window where they can be edited after creation.
        # This is specified by the terminology's effective dates.
        # We will attempt to load the new concepts to the terminology.
        # If we fail, then we'll create a new version and load them there.

        new_terminology_version_created = False  # Represents whether we have to create a new terminology version

        try:
            source_terminology.load_additional_concepts(concept_list)
        except Exception as e:
            new_terminology_version_created = True
            raise e


def load_outstanding_codes_to_new_concept_map_version(concept_map_uuid: UUID):
    # Step 6: Create the new value set version
    new_source_value_set_version = source_value_set_version.new_version(
        "new version created by the condition incremental load system")

    if new_terminology_version_created:
        new_source_value_set_version.update_rules_for_new_terminology_version(
            # todo: only applicable if you had to create a new terminology version
            old_terminology_version_uuid=None,
            new_terminology_version_uuid=None
        )

    # todo: Publish new value set version
    new_source_value_set_version.publish()

    # todo: Create new concept map version
    new_concept_map_version = concept_map_version.new_version(
        new_version_description="Automatically created concept map version from condition incremental load",
        new_source_value_set_version_uuid=new_source_value_set_version.additional_data.get('version_uuid'),
        new_target_value_set_version_uuid=target_value_set_version.additional_data.get('version_uuid')
    )

    # todo: register new data in appropriate queue for triaging
