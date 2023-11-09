import json
import unittest

from _pytest.python_api import raises

from app.concept_maps.models import ConceptMap, ConceptMapVersion
from app.errors import BadRequestWithCode


class OutputTests(unittest.TestCase):
    def test_serialized_schema_versions(self):
        """
        Functions called by this test do not change data or write to OCI. They serialize data found in the database.
        If the data changes, results may change. The selected test cases are not expected to change often.

        This test does not call any outside services other than the database.
        Note: This test calls some functions that WOULD call to outside services, such as Simplifier or OCI,
        but each of these test cases triggers an exception on purpose, so the callout does not occur.

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

        # Cannot write to Simplifier, a concept map with 0 mappings - v4 or later forbids this case
        with raises(BadRequestWithCode):
            concept_map_version_2.to_simplifier()

        # Cannot publish, a v4 or later version with 0 mappings
        with raises(BadRequestWithCode):
            concept_map_version_2.publish()

        # Cannot store in OCI, a v4 or later version with 0 mappings
        with raises(BadRequestWithCode):
            concept_map_version_2.prepare_for_oci()

        # Convenience: Can set a breakpoint at this line when done stepping and want to ensure it reaches the end cleanly.
        assert True


    def test_concept_map_output(self):
        """
        Functions called by this test do not change data or write to OCI. They serialize data found in the database.
        If the data changes, results may change. The selected test cases in the helper are not expected to change often.

        Test that the ConceptMap current and/or next schema version is correctly defined in the ConceptMap class right now.
        Then run the concept_map_output_for_schema test for each version (only once, if they are the same number right now).
        """
        assert ConceptMap.database_schema_version == 3
        assert ConceptMap.next_schema_version == 4
        self.concept_map_output_for_schema(ConceptMap.database_schema_version)
        if ConceptMap.database_schema_version != ConceptMap.next_schema_version:
            self.concept_map_output_for_schema(ConceptMap.next_schema_version)


    @staticmethod
    def concept_map_output_for_schema(schema_version: int):
        """
        Helper function for test_concept_map_output.
        @param schema_version: current and/or next schema version for ConceptMap as input by test_concept_map_output().
        """
        test_concept_map_version_uuid = "316f6438-6197-4bb0-a932-ed6c48d2a860"  # this map has dependsOn; many others do not
        test_concept_map_version = ConceptMapVersion(test_concept_map_version_uuid)
        if test_concept_map_version is None:
            print(f"Version with UUID {test_concept_map_version_uuid} is None")
        else:
            with (open(f"../resources/concept_maps/serialized_v{schema_version}.json") as serialized_json):
                # call the output function we are testing
                (serialized_concept_map, initial_path) = test_concept_map_version.prepare_for_oci(schema_version)

                # is the OCI path correct?
                assert initial_path == f"ConceptMaps/v{schema_version}"

                # remove timestamp of "now" because it cannot match the timestamp of any other output sample
                del serialized_concept_map["date"]

                # limit output file size and avoid issues from random element order: cut all but 1 group and 1 element
                test_group = None
                for group in serialized_concept_map["group"]:
                    if group.get("target") == "http://loinc.org":
                        for element in group["element"]:
                            if element["target"][0].get("code") == "11433-0":
                                elements = [element]
                                break
                        test_group = {"element": elements}
                        break
                del serialized_concept_map["group"]
                serialized_concept_map["group"] = [test_group]

                # read the string from the comparison file and call json.loads() to load it as an object
                test_file = serialized_json.read()
                serialized_test = json.loads(test_file)

                # compare serialized concept map object with the object loaded from the file string
                assert serialized_concept_map == serialized_test


if __name__ == '__main__':
    unittest.main()
