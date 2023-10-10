import json

import pytest
from _pytest.python_api import raises

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.concept_maps.views import get_concept_map_version_prerelease
from app.errors import BadRequestWithCode
from app.helpers.oci_helper import set_up_object_store


@pytest.mark.skip(reason="Before running locally, comment out 3 lines that call the Error Service in concept_maps/models.py. Then comment out the skip annotation.")
def test_serialized_schema_versions():
    """
    Functions called by this test do not change data or write to OCI. They serialize data or read from OCI artifacts.
    If the data changes, results may change. The selected test cases are not expected to change often.

    Test Cases:
        Serialize a version as v3 and test that it has no items that are in v4 only.
        Serialize the same version as v4 and test that it has all items that are in v4 only.
        Serialize a version that has no mappings as v3 and test that it is fine. Group list is empty.
        Serialize a version that has no mappings as v4 and test that it is fine. Group list is empty.
        Attempt to publish as v4, a version that has no mappings, and test that it raises an exception.
        Attempt to write to OCI as v4, a version that has no mappings, and test that it raises an exception.
    """
    # Step 1: small concept map with a version that has no mappings
    concept_map_uuid = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"  # "Apposnd Conditions to SNOMED CT" v1-v4
    concept_map_version_2 = ConceptMapVersion.load_by_concept_map_uuid_and_version(concept_map_uuid, 2)
    concept_map_version_4 = ConceptMapVersion.load_by_concept_map_uuid_and_version(concept_map_uuid, 4)

    # v3 serialize a version that has mappings and test that it is fine.  Group list has entries.
    serialized_v3 = concept_map_version_4.serialize(include_internal_info=True, schema_version=3)
    group = serialized_v3.get("group")
    assert group is not None
    assert len(group) > 0
    sourceCanonical =  serialized_v3.get("sourceCanonical")
    assert sourceCanonical is None
    targetCanonical = serialized_v3.get("targetCanonical")
    assert targetCanonical is None

    # v4 serialize a version that has mappings and test that it is fine. Group list has entries.
    serialized_v4 = concept_map_version_4.serialize(include_internal_info=True, schema_version=4)
    group = serialized_v4.get("group")
    assert group is not None
    assert len(group) > 0
    sourceCanonical = serialized_v4.get("sourceCanonical")
    assert sourceCanonical == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
    targetCanonical =  serialized_v4.get("targetCanonical")
    assert targetCanonical == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"

    # v3 serialize a version that has no mappings as v3 and test that it is fine. Group list is empty.
    serialized_v3 = concept_map_version_2.serialize(include_internal_info=True, schema_version=3)
    group = serialized_v3.get("group")
    assert group is not None
    assert len(group) == 0
    sourceCanonical =  serialized_v3.get("sourceCanonical")
    assert sourceCanonical is None
    targetCanonical = serialized_v3.get("targetCanonical")
    assert targetCanonical is None

    # v4 serialize a version that has no mappings and test that it is fine. Group list is empty.
    serialized_v4 = concept_map_version_2.serialize(include_internal_info=True, schema_version=4)
    group = serialized_v4.get("group")
    assert group is not None
    assert len(group) == 0
    sourceCanonical = serialized_v4.get("sourceCanonical")
    assert sourceCanonical == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
    targetCanonical =  serialized_v4.get("targetCanonical")
    assert targetCanonical == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"

    # Cannot write to Simplifier, a concept map with 0 mappings - the current schema (v4) forbids this case
    with raises(BadRequestWithCode):
        concept_map_version_2.to_simplifier()

    # Cannot publish, a v4 version with 0 mappings
    with raises(BadRequestWithCode):
        concept_map_version_2.publish(schema_version=4)

    # Cannot store in OCI, a v4 version with 0 mappings
    with raises(BadRequestWithCode):
        concept_map_version_2.prepare_for_oci(schema_version=4)


@pytest.mark.skip(reason="Before running locally, comment out 3 lines that call the Error Service in concept_maps/models.py. Then comment out the skip annotation.")
def test_diff_mappings_and_metadata():
    """
    Functions called by this test do not change data or write to OCI. They serialize data or read from OCI artifacts.
    If the data changes, results may change. The selected test cases are not expected to change often.

    Test Cases:
        Invalid order of previous_version and next_version
        Nonexistent previous_version or next_version
        Different valid combinations of previous_version and next_version
        Different valid combinations of added, modified, unchanged, and removed codes
        Previous concept map version with no codes
        New concept map version with no codes
        Compare to itself, same version, same schema: summary unchanged, codes all unchanged
        Compare to itself, same version, schema v3 vs. v4: summary has correct changes, codes all unchanged
        Concept map output format schemas v3 vs. v4: should not, and does not, affect diff logic
        Correct output of full diff
        Correct output of summary section
        Correct output of added mappings
        Correct output of removed mappings
    """
    # Step 1: small concept map
    concept_map_uuid = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"  # "Apposnd Conditions to SNOMED CT" v1-v4
    concept_map = ConceptMap(concept_map_uuid)

    # wrong version order
    with raises(BadRequestWithCode):
        concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=2, new_version=1)

    # nonexistent new version
    with raises(BadRequestWithCode):
        concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=2, new_version=201)

    # nonexistent previous version
    with raises(BadRequestWithCode):
        concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=200, new_version=201)

    # v0 > v1 - first version, some added, compare full output for added - serialize v4
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=0,
        new_version=1,
        previous_schema_version=4
    )
    added_codes = result.get("added_codes")
    with open("../test/resources/concept_map/mappings_added_0_to_1.json") as added_test_json:
        added_test = json.loads(added_test_json.read())
        assert added_codes == added_test
    removed_codes = result.get("removed_codes")
    assert removed_codes == []
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 10
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 0
    assert result.get("new_total") == 10
    assert result.get("version") == 1

    # v1 > v2 - all removed - serialize default
    result = concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=1, new_version=2)
    assert result.get("removed_count") == 10
    assert result.get("added_count") == 0
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 10
    assert result.get("new_total") == 0
    assert result.get("version") == 2

    # v0 > v2 - skip some intervening versions, no difference, all empty - serialize default
    result = concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=0, new_version=2)
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 0
    assert result.get("previous_total") == 0
    assert result.get("new_total") == 0
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("version") == 2

    # v2 > v3 - some added - serialize default
    result = concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=2, new_version=3)
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 10
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 0
    assert result.get("new_total") == 10
    assert result.get("version") == 3

    # v3 > v4 - some added - serialize default
    result = concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=3, new_version=4)
    modified_codes = result.get("modified_codes")
    with open("../test/resources/concept_map/mappings_modified_3_to_4.json") as modified_test_json:
        modified_test = json.loads(modified_test_json.read())
        assert modified_codes == modified_test
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 96
    assert result.get("modified_count") == 10
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 10
    assert result.get("new_total") == 106
    assert result.get("version") == 4

    # v1 > v4 - skip some intervening versions, totals are correct - serialize default
    result = concept_map.diff_mappings_and_metadata(concept_map_uuid, previous_version=1, new_version=4)
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 96
    assert result.get("modified_count") == 10
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 10
    assert result.get("new_total") == 106
    assert result.get("version") == 4

    # v4 > v4 - compare version to itself, same schema, totals are correct - schema v3
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=4,
        new_version=4,
        previous_schema_version=3,
        new_schema_version=3
    )
    summary = result.get("summary_diff")
    assert summary.get("sourceCanonical") is None
    assert summary.get("targetCanonical") is None
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 0
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 106
    assert result.get("previous_total") == 106
    assert result.get("new_total") == 106
    assert result.get("version") == 4

    # v4 > v4 - compare version to itself, totals are correct - schema v3 vs. v4
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=4,
        new_version=4,
        previous_schema_version=3,
        new_schema_version=4
    )
    summary = result.get("summary_diff")
    with open("../test/resources/concept_map/summary_diff_4_to_4_schema_v3_to_v4.json") as summary_diff_4_to_4_json:
        summary_test = json.loads(summary_diff_4_to_4_json.read())
        assert summary == summary_test
    assert summary["sourceCanonical"]["new_value"] == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
    assert summary["targetCanonical"]["new_value"] == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"
    assert summary["sourceCanonical"]["old_value"] == "None"
    assert summary["targetCanonical"]["old_value"] == "None"
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 0
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 106
    assert result.get("previous_total") == 106
    assert result.get("new_total") == 106
    assert result.get("version") == 4

    # v0 > v4 - skip all versions, totals are correct - serialize v3
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=0,
        new_version=4,
        previous_schema_version=3
    )
    assert result.get("removed_count") == 0
    assert result.get("added_count") == 106
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 0
    assert result.get("new_total") == 106
    assert result.get("version") == 4

    # Step 2: larger concept map
    concept_map_uuid = "615434d9-7a4f-456d-affe-4c2d87845a37"  # "MDA Observation to Ronin Observation" v1-v7
    concept_map = ConceptMap(concept_map_uuid)

    # v2 > v3 - complete replace, all mappings - serialize v4
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=2,
        new_version=3,
        previous_schema_version=4
    )
    summary = result.get("summary_diff")
    with open("../test/resources/concept_map/summary_diff_2_to_3_schema_v4.json") as summary_diff_2_to_3_json:
        summary_test = json.loads(summary_diff_2_to_3_json.read())
        assert summary == summary_test
    assert result.get("removed_count") == 6600
    assert result.get("added_count") == 7268
    assert result.get("modified_count") == 0
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 6600
    assert result.get("new_total") == 7268
    assert result.get("version") == 3

    # v4 > v5 - some in, some out, compare output for removed - serialize v3
    result = concept_map.diff_mappings_and_metadata(
        concept_map_uuid,
        previous_version=4,
        new_version=5,
        previous_schema_version=3
    )
    removed_codes = result.get("removed_codes")
    summary = result.get("summary_diff")
    with open("../test/resources/concept_map/summary_diff_4_to_5_schema_v3.json") as diff_4_to_5_json:
        summary_test = json.loads(diff_4_to_5_json.read())
        assert summary == summary_test
    with open("../test/resources/concept_map/mappings_removed_4_to_5.json") as removed_test_json:
        removed_test = json.loads(removed_test_json.read())
        assert removed_codes == removed_test
    assert result.get("removed_count") == 10
    assert result.get("added_count") == 124
    assert result.get("modified_count") == 7262
    assert result.get("unchanged_count") == 0
    assert result.get("previous_total") == 7272
    assert result.get("new_total") == 7386
    assert result.get("version") == 5


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_concept_map_output_to_oci():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # schema_version = ConceptMap.database_schema_version
    schema_version = ConceptMap.next_schema_version

    test_concept_map_version_uuid = "(insert value here)"
    test_concept_map_version = ConceptMapVersion(test_concept_map_version_uuid)
    if test_concept_map_version is None:
        print(f"Version with UUID {test_concept_map_version_uuid} is None")
    else:
        concept_map_to_json, initial_path = test_concept_map_version.prepare_for_oci(schema_version)
        set_up_object_store(
            concept_map_to_json,
            initial_path + f"/published/{test_concept_map_version.concept_map.uuid}",
            folder="published",
            content_type="json",
        )
    # look in OCI to see the value set and data normalization registry files (open up registry.json to see updates)
