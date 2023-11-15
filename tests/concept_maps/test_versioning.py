import json
import unittest
from _pytest.python_api import raises

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.errors import BadRequestWithCode
from app.helpers.file_helper import resources_folder


class VersioningTests(unittest.TestCase):
    def test_diff_mappings_and_metadata(self):
        """
        Functions called by this test do not change data or write to OCI. They serialize data or read from OCI artifacts.
        If the data changes, results may change. The selected test cases are not expected to change often.

        The ConceptMap.diff_mappings_and_metadata() function
        compares concept map versions to assert which mappings were removed and added between versions. For clarity of
        results, verifies that previous and new are versions of the same map and previous is earlier or the same as new.

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
        with (open(resources_folder(__file__, "mappings_added_0_to_1.json"))) as added_test_json:
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
        with (open(resources_folder(__file__, "mappings_modified_3_to_4.json"))) as modified_test_json:
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

        # v4 > v4 - compare version to itself, same schema, totals are correct - schema v4
        result = concept_map.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version=4,
            new_version=4,
            previous_schema_version=4,
            new_schema_version=4
        )
        summary = result.get("summary_diff")
        assert summary.get("sourceCanonical") == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
        assert summary.get("targetCanonical") == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"
        assert result.get("removed_count") == 0
        assert result.get("added_count") == 0
        assert result.get("modified_count") == 0
        assert result.get("unchanged_count") == 106
        assert result.get("previous_total") == 106
        assert result.get("new_total") == 106
        assert result.get("version") == 4

        # v0 > v4 - skip all versions, totals are correct - serialize v4
        result = concept_map.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version=0,
            new_version=4,
            previous_schema_version=4
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
        with (open(resources_folder(__file__, "summary_diff_2_to_3_schema_v4.json"))) as summary_diff_2_to_3_json:
            summary_test = json.loads(summary_diff_2_to_3_json.read())
            assert summary == summary_test
        assert result.get("removed_count") == 6600
        assert result.get("added_count") == 7268
        assert result.get("modified_count") == 0
        assert result.get("unchanged_count") == 0
        assert result.get("previous_total") == 6600
        assert result.get("new_total") == 7268
        assert result.get("version") == 3

        # v4 > v5 - some in, some out, compare output for removed - serialize v4
        result = concept_map.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version=4,
            new_version=5,
            previous_schema_version=4
        )
        removed_codes = result.get("removed_codes")
        summary = result.get("summary_diff")
        with (open(resources_folder(__file__, "summary_diff_4_to_5_schema_v4.json"))) as diff_4_to_5_json:
            summary_test = json.loads(diff_4_to_5_json.read())
            assert summary == summary_test
        with (open(resources_folder(__file__, "mappings_removed_4_to_5.json"))) as removed_test_json:
            removed_test = json.loads(removed_test_json.read())
            assert removed_codes == removed_test
        assert result.get("removed_count") == 10
        assert result.get("added_count") == 124
        assert result.get("modified_count") == 7262
        assert result.get("unchanged_count") == 0
        assert result.get("previous_total") == 7272
        assert result.get("new_total") == 7386
        assert result.get("version") == 5


if __name__ == '__main__':
    unittest.main()
