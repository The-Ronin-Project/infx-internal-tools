import pytest

from app.value_sets.models import ValueSetVersion, ValueSet
from app.terminologies.models import Terminology
from app.database import get_db


def test_lookup_terminologies_in_value_set_version():
    conn = get_db()

    loinc_2_74 = Terminology.load('554805c6-4ad1-4504-b8c7-3bab4e5196fd')  # LOINC 2.74

    value_set_version = ValueSetVersion.load('2441d5b7-9c64-4cac-b274-b70001f05e3f') #todo: replace w/ dedicated value set for automated tests
    value_set_version.expand()
    terminologies_in_vs = value_set_version.lookup_terminologies_in_value_set_version()

    assert terminologies_in_vs == [loinc_2_74]

    conn.rollback()
    conn.close()


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_value_set_publish():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # NOTE: Locally change ValueSet.object_storage_folder_name and DataNormalizationRegistry.object_storage_folder_name
    # to names that are not used for product output. Under the DataNormalizationRegistry.object_storage_folder_name
    # create a "v3" (or correct current number) folder and copy current registry.json and registry_diff.json files there
    test_value_set_uuid = "fc82ec39-7b9f-4d74-9a34-adf86db1a50f"  # Automated Testing Value Set
    test_value_set = ValueSet.load_most_recent_version(test_value_set_uuid, active_only=False)
    test_value_set.publish(force_new=True)
    # look in OCI to see the value set and data normalization registry files (open up registry.json to see updates)

    # NOTE: Locally restore ValueSet.object_storage_folder_name and DataNormalizationRegistry.object_storage_folder_name
