import json

import pytest
from _pytest.python_api import raises

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.errors import BadRequestWithCode
from app.models.data_ingestion_registry import DataNormalizationRegistry, DNRegistryEntry
from app.value_sets.models import ValueSet


@pytest.mark.skip(
    reason="External calls: Reads from the database. To run on a local dev machine, comment out this 'skip' " +
        "annotation. To support future test automation, any external calls must be mocked."
)
def test_serialized_data_normalization_registry():
    """
    Functions called by this test do not change data or write to OCI. They serialize data found in the database.
    If the data changes, results may change. The selected test cases are not expected to change often.

    This test does not call any outside services other than the database.
    Note: This test calls some functions that WOULD call to outside services, such as OCI,
    but each of these test cases triggers an exception on purpose, so the callout does not occur.

    "Correct paths" in the following test cases will vary depending on current values of the following properties:
        ConceptMap.database_schema_version (example: 3)
        ConceptMap.next_schema_version (example: 4)
        ValueSet.database_schema_version (example: 2) - at this time there is no ValueSet.next_schema_version property
        DataNormalizationRegistry.database_schema_version (example: 3)
        DataNormalizationRegistry.next_schema_version (example: 4)
    Test Cases use "now" and "next" to refer to the database_schema_version and next_schema_version

    Test Cases:
        Serialize a DNRegistry without a ConceptMap schema - see it serializes ConceptMaps "next", ValueSets "now"
        Serialize a DNRegistry as ConceptMaps "now" - see it serializes ConceptMaps "now", ValueSets "now"
        Serialize same DNRegistry as ConceptMaps "next" - see it serializes ConceptMaps "next", ValueSets "now"
        Attempt to serialize a DNRegistry to a non-empty but invalid ConceptMaps schema version, see an exception.
        Serialize a DNRegistryEntry without a ConceptMap schema - see it serializes ConceptMaps "next"
        Serialize a DNRegistryEntry as ConceptMaps "now" - see it serializes ConceptMaps "now"
        Serialize same DNRegistryEntry as ConceptMaps "next" - see it serializes ConceptMaps "next"
        Attempt to serialize a DNRegistryEntry to a non-empty but invalid ConceptMaps schema version, see an exception.
    """
    # Setup for tests
    concept_map_uuid = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"  # "Apposnd Conditions to SNOMED CT" v1-v4
    concept_map = ConceptMap(
            concept_map_uuid,
            load_mappings_for_most_recent_active=False,
        )
    entry = DNRegistryEntry(
        resource_type="Patient",
        data_element="Patient.status",
        tenant_id="apposnd",
        source_extension_url="http://projectronin.io/fhir/StructureDefinition/Extension/tenant-sourceConditionCode",
        registry_uuid="24e71b66-1843-4c43-a6a5-be67c9c91b0a",
        profile_url="null",
        registry_entry_type="concept_map",
        concept_map=concept_map,
    )
    current_registry = DataNormalizationRegistry()
    current_registry.load_entries()
    concept_map_path_now = f"{ConceptMap.object_storage_folder_name}/v{ConceptMap.database_schema_version}/"
    concept_map_path_next = f"{ConceptMap.object_storage_folder_name}/v{ConceptMap.next_schema_version}/"
    value_set_path_now = f"{ValueSet.object_storage_folder_name}/v{ValueSet.database_schema_version}/"

    # Serialize a DNRegistryEntry without a ConceptMap schema - see it serializes ConceptMaps "next"
    entry_serialized = DNRegistryEntry.serialize(entry)
    assert concept_map_path_next in entry_serialized.get("filename")

    # Serialize a DNRegistryEntry as ConceptMaps "now" - see it serializes ConceptMaps "now"
    entry_serialized = DNRegistryEntry.serialize(entry, ConceptMap.database_schema_version)
    assert concept_map_path_now in entry_serialized.get("filename")

    # Serialize same DNRegistryEntry as ConceptMaps "next" - see it serializes ConceptMaps "next"
    entry_serialized = DNRegistryEntry.serialize(entry, ConceptMap.next_schema_version)
    assert concept_map_path_next in entry_serialized.get("filename")

    # Attempt to serialize a DNRegistryEntry to a non-empty but invalid ConceptMaps schema version, see an exception.
    with raises(BadRequestWithCode):
        DNRegistryEntry.serialize(entry, 999)

    # Serialize a DNRegistry without a ConceptMap schema - see it serializes ConceptMaps "next", ValueSets "now"
    current_registry_serialized = current_registry.serialize()
    found_concept_map = False
    found_value_set = False
    while not found_concept_map and not found_value_set:
        for entry in current_registry_serialized:
            if not found_value_set and entry.get("registry_entry_type") == "value_set":
                found_value_set = True
                assert value_set_path_now in entry.get("filename")
            elif not found_concept_map and entry.get("registry_entry_type") == "concept_map":
                found_concept_map = True
                assert concept_map_path_next in entry.get("filename")
            else:
                continue

    # Serialize a DNRegistry as ConceptMaps "now" - see it serializes ConceptMaps "now", ValueSets "now"
    current_registry_serialized = current_registry.serialize(ConceptMap.database_schema_version)
    found_concept_map = False
    found_value_set = False
    while not found_concept_map and not found_value_set:
        for entry in current_registry_serialized:
            if not found_value_set and entry.get("registry_entry_type") == "value_set":
                found_value_set = True
                assert value_set_path_now in entry.get("filename")
            elif not found_concept_map and entry.get("registry_entry_type") == "concept_map":
                found_concept_map = True
                assert concept_map_path_now in entry.get("filename")
            else:
                continue

    # Serialize same DNRegistry as ConceptMaps "next" - see it serializes ConceptMaps "next", ValueSets "now"
    current_registry_serialized = current_registry.serialize(ConceptMap.next_schema_version)
    found_concept_map = False
    found_value_set = False
    while not found_concept_map and not found_value_set:
        for entry in current_registry_serialized:
            if not found_value_set and entry.get("registry_entry_type") == "value_set":
                found_value_set = True
                assert value_set_path_now in entry.get("filename")
            elif not found_concept_map and entry.get("registry_entry_type") == "concept_map":
                found_concept_map = True
                assert concept_map_path_next in entry.get("filename")
            else:
                continue

    # Attempt to serialize a DNRegistry to a non-empty but invalid ConceptMaps schema version, see an exception.
    with raises(BadRequestWithCode):
        current_registry.serialize(999)

    # Note: Depending on current values in the DataNormalizationRegistry class, 1 of the following 2 cases always needs
    # mocking, because only 1 of the 2 can be true at one time. To be sure of full coverage, mock them both in the test.
    # todo: Mock equal valid values for DataNormalizationRegistry.database_schema_version, next_schema_version, retest
    # todo: Mock unequal valid values for DataNormalizationRegistry.database_schema_version, next_schema_version, retest

    # Convenience: Can set a breakpoint at this line when done stepping and want to ensure it reaches the end cleanly.
    assert True


@pytest.mark.skip(
    reason="External calls: Reads from the database, writes to the OCI data store. This is a utility, not a test." +
        "Do not write out to the database or write out to OCI data store, from tests. "
)
def test_norm_registry_output_to_oci():
    """
    Not a test. Really a tool for developers to push content to OCI for urgent reasons.
    """
    DataNormalizationRegistry.publish_data_normalization_registry()
    # Look in OCI to see the data normalization registry files (open up registry.json to see updates)
    # If DataNormalizationRegistry.database_schema_version, next_schema_version are different, see output for both.
