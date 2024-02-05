import json
from unittest import skip

import pytest

from app.concept_maps.models import ConceptMap, ConceptMapVersion


@skip("This is a utility, not a test. Use only for OCI repairs by Informatics Systems team members.")
def test_concept_map_output_to_oci():
    """
    Not a test. Really a tool for developers to push content to OCI for urgent reasons. Be aware that if there is
    already an output file for a version number present in the OCI folder, this function will not overwrite it. If it is
    imperative to overwrite the previously output file for a version number, you must remove that file from OCI first.
    """
    test_concept_map_version_uuid =  # use this invalid value on purpose when merging
    concept_map_output_to_oci(test_concept_map_version_uuid, ConceptMap.database_schema_version)
    if ConceptMap.database_schema_version != ConceptMap.next_schema_version:
        concept_map_output_to_oci(test_concept_map_version_uuid, ConceptMap.next_schema_version)


def concept_map_output_to_oci(test_concept_map_version_uuid: str, schema_version: int):
    """
    Helper function for test_concept_map_output_to_oci.
    @param test_concept_map_version_uuid, see tests/concept_maps/test_concept_maps.py for a list of safe values
    @param schema_version: current and/or next schema version for ConceptMap as input by test_concept_map_output().
    """
    test_concept_map_version = ConceptMapVersion(test_concept_map_version_uuid)
    if test_concept_map_version is None:
        print(f"Version with UUID {test_concept_map_version_uuid} is None")
    else:
        test_concept_map_version.send_to_oci(schema_version)
    # look in OCI to see the value set and data normalization registry files (open up registry.json to see updates)
