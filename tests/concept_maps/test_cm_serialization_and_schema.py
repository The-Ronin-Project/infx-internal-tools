import json
import unittest

from _pytest.python_api import raises

import app.app
from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.errors import BadRequestWithCode
from app.helpers.data_helper import load_json_string, cleanup_json_string, serialize_json_object
from app.helpers.file_helper import resources_folder
from app.database import get_db
from app.app import create_app
from app.enum.concept_maps_for_content import ConceptMapsForContent


class ConceptMapOutputTests(unittest.TestCase):
    """
    There are known UUID values for concept_maps.concept_map rows that are safe to use in any tests
    - for these see app.enum.concept_maps_for_systems.ConceptMapsForSystems class doc.

     For test cases in ConceptMapOutputTests, only SELECT operations are needed and many test cases are so
     complex we could easily introduce confusing errors into any fake test data. Therefore, tests in this class use
     concept map rows that are not in the "safe to test" group. For example, test_serialized_schema_versions() uses the
     following concept map. "apposnd" identifies the Epic EHR vendor testing sandbox: "AppOrchardSandbox" or "apposnd"
    ```
    concept_map_uuid = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"  # "Apposnd Conditions to SNOMED CT" v1-v4
    ```
    """

    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_serialized_schema_versions(self):
        """
        Functions called by this test do not change data or write to OCI. They serialize data found in the database.
        If the data changes, results may change. The selected test cases are not expected to change often.

        This test does not call any outside services other than the database.
        Note: This test calls some functions that WOULD call to outside services, such as Simplifier or OCI,
        but each of these test cases triggers an exception on purpose, so the callout does not occur.

        Test cases removed:
            For v4>v5, TMP, Interops, DP, Infx confirmed: a "dual version" transition is not needed for ConceptMaps

        Test Cases:
            Serialize the same version as v(n)
            Attempt to publish using a version number that is not supported.
            Attempt to publish using a version number that is formerly valid, recently outmoded.
        """
        concept_map_uuid = "85d2335c-791e-4db5-a98d-c0c32949a65e"  # Apposnd Observation to Ronin Observation
        test_concept_map = ConceptMap(concept_map_uuid)
        concept_map_version = test_concept_map.get_most_recent_version(active_only=True)

        # v5 serialize a version that has mappings and test that it is fine. Group list has entries.
        serialized_v5 = concept_map_version.serialize(include_internal_info=True, schema_version=5)
        group = serialized_v5.get("group")
        assert group is not None
        assert len(group) > 0
        assert "http://projectronin.io/fhir/ValueSet/" in serialized_v5.get("sourceCanonical")
        assert "http://projectronin.io/fhir/ValueSet/" in serialized_v5.get("targetCanonical")

        # Cannot write a bad version number of schema format
        with raises(BadRequestWithCode):
            concept_map_version.serialize(include_internal_info=True, schema_version=0)

        # Cannot write a formerly valid, now outmoded version number of schema format
        with raises(BadRequestWithCode):
            concept_map_version.serialize(include_internal_info=True, schema_version=3)

    def test_concept_map_output(self):
        """
        Functions called by this test do not change data or write to OCI. They serialize data found in the database.
        If the data changes, results may change. The selected test cases in the helper are not expected to change often.

        Test that the ConceptMap current and/or next schema version is correctly defined in the ConceptMap class right now.
        Then run the concept_map_output_for_schema test for each version (only once, if they are the same number right now).
        """
        assert ConceptMap.database_schema_version == 5
        assert ConceptMap.next_schema_version == 5  # during "dual_output" each *_schema_version has a different value
        versions = 1
        self.concept_map_observation_with_depends_on_output_for_schema(ConceptMap.database_schema_version)
        if ConceptMap.database_schema_version != ConceptMap.next_schema_version:
            versions += 1
            self.concept_map_observation_with_depends_on_output_for_schema(ConceptMap.next_schema_version)
        assert versions == 1  # When the versions are different, this value must be 2 instead of 1

    @staticmethod
    def concept_map_observation_with_depends_on_output_for_schema(schema_version: int):
        """
        Helper function for test_concept_map_output.
        @param schema_version: current and/or next schema version for ConceptMap as input by test_concept_map_output().
        """
        # init values
        object_storage_folder_name = "ConceptMaps"
        concept_map = app.concept_maps.models.ConceptMap(ConceptMapsForContent.MDA_OBSERVATION.value)  # has depends_on

        # start the test
        test_concept_map_version = concept_map.get_most_recent_version(
            active_only=True,
            load_mappings=True,
            pending_only=False
        )
        if test_concept_map_version is None:
            raise ValueError(f"No active version of ConceptMap with UUID: {ConceptMapsForContent.MDA_OBSERVATION.value}")
        else:
            with (open(resources_folder(__file__, f"serialized_v{schema_version}.json")) as serialized_json):
                # call the output function we are testing
                (concept_map_data, initial_path) = test_concept_map_version.prepare_for_oci(schema_version)

                # did we get the high-level attributes?
                assert "resourceType" in concept_map_data
                assert (
                    concept_map_data.get("id") == ConceptMapsForContent.MDA_OBSERVATION.value and
                    concept_map_data.get("url") is not None and
                    concept_map_data.get("id") == concept_map_data.get("url")[39:] and
                    "http://projectronin.io/fhir/ConceptMap/" in concept_map_data.get("url")
                )
                assert concept_map_data["date"] == "2024-02-21T22:45:27.010444+00:00"
                assert "_description" in concept_map_data

                # is the OCI path correct?
                assert initial_path == f"{object_storage_folder_name}/v{schema_version}"

                # limit output file size and avoid issues from random element order: use only a few groups and elements
                sample_count = 0
                sample_list = [
                    # mapper, no reviewer:
                    "87cb279d3af095ca1ba5eca47180daf8",
                    "906a4c31339b57d596eab20233b6d026",
                    "219946a1c7ca92f749ddb098ecaca770",
                    "685ffa8ac92ae4643ca43b41f1dd6971",
                    # mapper and reviewer:
                    "ad0f0b7a9638e4a23faa6a874abc7dd2",
                    "773fe88abdc51449fa7070b6f9b17f1b",
                    "3031e49ac51f160616bf3a661fee6db5",
                    # depends_on Observation:
                    "eaeca8b800a747a675dbb0643de7d8b9",
                    "653d0d0276b16a26b95ecee739d3c0a0",
                    "21348901e5d40a3a6115c3f7dcbaf7f8"
                ]
                groups = []
                for group in concept_map_data["group"]:
                    elements = []
                    for element in group["element"]:
                        for target in element["target"]:
                            if target["id"] in sample_list:
                                elements.append(element)
                                sample_count += 1
                            if sample_count >= len(sample_list):
                                break
                        if sample_count >= len(sample_list):
                            break
                    if len(elements) > 0:
                        test_group = group
                        test_group["element"] = elements
                        groups.append(test_group)
                    if sample_count >= len(sample_list):
                        break
                del concept_map_data["group"]
                concept_map_data["group"] = groups

                # did we get the high-level attributes in group?
                assert "source" in concept_map_data["group"][0]
                assert "targetVersion" in concept_map_data["group"][0]
                assert "element" in concept_map_data["group"][0]

                # the "code_id"
                test_code = concept_map_data["group"][0]["element"][0]["_code"]["extension"][0]
                assert "id" in test_code
                assert test_code["url"] == "http://projectronin.io/fhir/StructureDefinition/Extension/canonicalSourceData"

                # the "mapping_id"
                test_target = concept_map_data["group"][0]["element"][0]["target"][0]
                assert "id" in test_target

                # read the comparison file
                test_file = serialized_json.read()
                serialized_test = cleanup_json_string(test_file)
                serialized_concept_map_string = serialize_json_object(concept_map_data)

                # compare serialized concept map with the comparison file
                assert True  # PyCharm runs past the last line, even with a breakpoint on it; can put a breakpoint here
                assert serialized_concept_map_string == serialized_test

    def test_concept_map_condition_with_nlp_and_automap(self):
        # init values
        schema_version = ConceptMap.next_schema_version
        object_storage_folder_name = "ConceptMaps"
        concept_map = app.concept_maps.models.ConceptMap(
            ConceptMapsForContent.PSJ_CONDITION.value,
            load_mappings_for_most_recent_active=False
        )

        # start the test - will need version 5 or later for PSJ_CONDITION - none of those are "active" yet - just get it
        test_concept_map_version = concept_map.get_most_recent_version(
            active_only=False,
            load_mappings=True,
            pending_only=False
        )
        if test_concept_map_version is None:
            raise ValueError(f"No version of ConceptMap with UUID: {ConceptMapsForContent.PSJ_CONDITION.value}")
        else:
            with (open(resources_folder(__file__, f"serialized_nlp_automap_v{schema_version}.json")) as serialized_json):
                # call the output function we are testing
                (concept_map_data, initial_path) = test_concept_map_version.prepare_for_oci(schema_version)

                # did we get the high-level attributes?
                assert "resourceType" in concept_map_data
                assert (
                    concept_map_data.get("id") == ConceptMapsForContent.PSJ_CONDITION.value and
                    concept_map_data.get("url") is not None and
                    concept_map_data.get("id") == concept_map_data.get("url")[39:] and
                    "http://projectronin.io/fhir/ConceptMap/" in concept_map_data.get("url")
                )
                # cannot test a date of now() because it always changes - date is now() in a "pending" concept map
                del concept_map_data["date"]
                assert "_description" in concept_map_data

                # is the OCI path correct?
                assert initial_path == f"{object_storage_folder_name}/v{schema_version}"

                # limit output file size and avoid issues from random element order: use only a few groups and elements
                sample_count_in_group_limit = 3
                groups = []
                for group in concept_map_data["group"]:
                    elements = []
                    sample_count_in_group = 0
                    for element in group["element"]:
                        elements.append(element)
                        sample_count_in_group += 1
                        if sample_count_in_group >= sample_count_in_group_limit:
                            break
                    if len(elements) > 0:
                        test_group = group
                        test_group["element"] = elements
                        groups.append(test_group)
                del concept_map_data["group"]
                concept_map_data["group"] = groups

                # did we get the high-level attributes in group?
                assert "source" in concept_map_data["group"][0]
                assert "targetVersion" in concept_map_data["group"][0]
                assert "element" in concept_map_data["group"][0]

                # the "code_id"
                test_code = concept_map_data["group"][0]["element"][0]["_code"]["extension"][0]
                assert "id" in test_code
                assert test_code["url"] == "http://projectronin.io/fhir/StructureDefinition/Extension/canonicalSourceData"

                # the "mapping_id"
                test_target = concept_map_data["group"][0]["element"][0]["target"][0]
                assert "id" in test_target

                # read the comparison file
                test_file = serialized_json.read()
                serialized_test = cleanup_json_string(test_file)
                serialized_concept_map_string = serialize_json_object(concept_map_data)

                # compare serialized concept map with the comparison file
                assert True  # PyCharm runs past the last line, even with a breakpoint on it; can put a breakpoint here
                assert serialized_concept_map_string == serialized_test

    def test_concept_map_condition_with_automap_only(self):
        # init values
        schema_version = ConceptMap.next_schema_version
        object_storage_folder_name = "ConceptMaps"
        concept_map = app.concept_maps.models.ConceptMap(
            ConceptMapsForContent.MDA_CONDITION.value,
            load_mappings_for_most_recent_active=False
        )

        # start the test
        test_concept_map_version = concept_map.get_most_recent_version(
            active_only=True,
            load_mappings=True,
            pending_only=False
        )
        if test_concept_map_version is None:
            raise ValueError(f"No active version of ConceptMap with UUID: {ConceptMapsForContent.MDA_CONDITION.value}")
        else:
            with (open(resources_folder(__file__, f"serialized_automap_only_v{schema_version}.json")) as serialized_json):
                # call the output function we are testing
                (concept_map_data, initial_path) = test_concept_map_version.prepare_for_oci(schema_version)

                # did we get the high-level attributes?
                assert "resourceType" in concept_map_data
                assert (
                    concept_map_data.get("id") == ConceptMapsForContent.MDA_CONDITION.value and
                    concept_map_data.get("url") is not None and
                    concept_map_data.get("id") == concept_map_data.get("url")[39:] and
                    "http://projectronin.io/fhir/ConceptMap/" in concept_map_data.get("url")
                )
                assert concept_map_data["date"] == "2024-02-09T19:28:35.394999+00:00"
                assert "_description" in concept_map_data

                # is the OCI path correct?
                assert initial_path == f"{object_storage_folder_name}/v{schema_version}"

                # limit output file size and avoid issues from random element order: use only a few groups and elements
                sample_count_in_group_limit = 3
                apos_count_limit = 3
                colon_count_limit = 1
                groups = []
                apos_count = 0
                colon_count = 0
                for group in concept_map_data["group"]:
                    elements = []
                    sample_count_in_group = 0
                    for element in group["element"]:
                        ext = element["_code"]["extension"][0]
                        if apos_count <= apos_count_limit and (
                            "valueCodeableConcept" in ext and "'s" in ext["valueCodeableConcept"]["text"]
                        ):
                            elements.append(element)
                            apos_count += 1
                        elif colon_count <= colon_count_limit and (
                            "valueCodeableConcept" in ext and "micr.:leuk" in ext["valueCodeableConcept"]["text"]
                        ):
                            elements.append(element)
                            colon_count += 1
                        else:
                            if sample_count_in_group <= sample_count_in_group_limit:
                                elements.append(element)
                                sample_count_in_group += 1
                    if len(elements) > 0:
                        test_group = group
                        test_group["element"] = elements
                        groups.append(test_group)
                del concept_map_data["group"]
                concept_map_data["group"] = groups

                # did we get the high-level attributes in group?
                assert "source" in concept_map_data["group"][0]
                assert "targetVersion" in concept_map_data["group"][0]
                assert "element" in concept_map_data["group"][0]

                # the "code_id"
                test_code = concept_map_data["group"][0]["element"][0]["_code"]["extension"][0]
                assert "id" in test_code
                assert test_code["url"] == "http://projectronin.io/fhir/StructureDefinition/Extension/canonicalSourceData"

                # the "mapping_id"
                test_target = concept_map_data["group"][0]["element"][0]["target"][0]
                assert "id" in test_target

                # read the comparison file
                test_file = serialized_json.read()
                serialized_test = cleanup_json_string(test_file)
                serialized_concept_map_string = serialize_json_object(concept_map_data)

                # compare serialized concept map with the comparison file
                assert True  # PyCharm runs past the last line, even with a breakpoint on it; can put a breakpoint here
                assert serialized_concept_map_string == serialized_test

    def test_concept_map_empty_group(self):
        concept_map = app.concept_maps.models.ConceptMap(ConceptMapsForContent.MDA_OBSERVATION.value)  # has depends_on
        # Set load_mappings=False on purpose to create the case of an empty group
        test_concept_map_version = concept_map.get_most_recent_version(active_only=True, load_mappings=False, pending_only=False)
        if test_concept_map_version is None:
            raise ValueError(f"No active version of ConceptMap with UUID: {ConceptMapsForContent.MDA_OBSERVATION.value}")
        with raises(BadRequestWithCode) as e:
            test_concept_map_version.prepare_for_oci(ConceptMap.database_schema_version)
        result = e.value
        assert result.code == "ConceptMap.prepareForOci.missingMappings"
        assert result.description == "Will not output a ConceptMap with no mappings defined."


if __name__ == '__main__':
    unittest.main()
