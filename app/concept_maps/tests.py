import pytest

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.value_sets.models import ValueSetVersion, ValueSet
from app.terminologies.models import Terminology
from app.database import get_db


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_concept_map_publish():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # NOTE: Locally edit ConceptMap.object_storage_folder_name and DataNormalizationRegistry.object_storage_folder_name
    # to names that are not used for product output. Under the DataNormalizationRegistry.object_storage_folder_name
    # create a "v3" (or correct current number) folder and copy current registry.json and registry_diff.json files there
    test_concept_map_uuid = "e9229d03-526e-423f-ad57-c52f2ea4475e"  # test october 27 2022
    test_concept_map_version = ConceptMap(test_concept_map_uuid).get_most_recent_version()
    test_concept_map_version.publish()
    # look in OCI to see the value set and data normalization registry files (open up registry.json to see updates)

    # NOTE: Locally restore ValueSet.object_storage_folder_name and DataNormalizationRegistry.object_storage_folder_name
