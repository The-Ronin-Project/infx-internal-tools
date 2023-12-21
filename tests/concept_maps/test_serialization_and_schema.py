import json
import unittest

from _pytest.python_api import raises

from app.concept_maps.models import ConceptMap, ConceptMapVersion, transform_struct_string_to_json
from app.errors import BadRequestWithCode
from app.helpers.file_helper import resources_folder
from app.database import get_db
from app.app import create_app


class ConceptMapOutputTests(unittest.TestCase):
    """
    There are known UUID values for concept_maps.concept_map rows that are safe to use in any tests
    - for these see ConceptMapTests class doc.

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

        Test Cases:
            Serialize the same version as v(n) and test that it has all items that are in v(n) only.
            Attempt to publish as v(n), a version that violates v(n) requirements, and test that it raises an exception.
            Attempt to publish using a version number that is not supported.
            Attempt to publish using a version number that is formerly valid, recently outmoded.
        Test Cases when dual versions are active (database_schema_version and next_schema_version are different):
            Serialize a version as v(n) and test that it has no items that are in v(n+1) only.
            Serialize a version as v(n+1) and test that it has no items that are in v(n) only.
        """
        # Step 1: small concept map with a version that has no mappings
        concept_map_uuid = "3704f2b0-7a8c-4455-ab2e-ffbcda91e1e3"  # "Apposnd Conditions to SNOMED CT" v1-v4
        concept_map_version_2 = ConceptMapVersion.load_by_concept_map_uuid_and_version(concept_map_uuid, 2)
        concept_map_version_4 = ConceptMapVersion.load_by_concept_map_uuid_and_version(concept_map_uuid, 4)

        # v4 serialize a version that has mappings and test that it is fine. Group list has entries.
        serialized_v4 = concept_map_version_4.serialize(include_internal_info=True, schema_version=4)
        group = serialized_v4.get("group")
        assert group is not None
        assert len(group) > 0
        sourceCanonical = serialized_v4.get("sourceCanonical")
        assert sourceCanonical == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
        targetCanonical =  serialized_v4.get("targetCanonical")
        assert targetCanonical == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"

        # v4 serialize a version that has no mappings and test that it is fine. Group list is empty.
        serialized_v4 = concept_map_version_2.serialize(include_internal_info=True, schema_version=4)
        group = serialized_v4.get("group")
        assert group is not None
        assert len(group) == 0
        sourceCanonical = serialized_v4.get("sourceCanonical")
        assert sourceCanonical == "http://projectronin.io/fhir/ValueSet/020d5cb0-193b-4240-b024-005802f860aa"
        targetCanonical =  serialized_v4.get("targetCanonical")
        assert targetCanonical == "http://projectronin.io/fhir/ValueSet/8b58bcea-82e3-4c09-a7c2-ce7d9e8dad4c"

        # Cannot write a bad version number of schema format
        with raises(BadRequestWithCode):
            concept_map_version_4.serialize(include_internal_info=True, schema_version=0)

        # Cannot write a formerly valid, now outmoded version number of schema format
        with raises(BadRequestWithCode):
            concept_map_version_4.serialize(include_internal_info=True, schema_version=3)

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
        assert ConceptMap.database_schema_version == 4
        assert ConceptMap.next_schema_version == 4
        versions = 1
        if ConceptMap.database_schema_version != ConceptMap.next_schema_version:
            versions += 1
            self.concept_map_output_for_schema(ConceptMap.next_schema_version)
        assert versions == 1  # When the versions are different, this value must be 2 instead of 1

    def test_convert_struct_null_display(self):
        example_string = "{null, Comorbidities found via Retrieve Dx}"
        json_output = transform_struct_string_to_json(example_string)

        self.assertEqual(json_output, '{"code": null, "display": "Comorbidities found via Retrieve Dx"}')

    def test_convert_struct_psj_format_with_urn(self):
        example_http_string = "{[{18107-3, Cardiac echo study Procedure stress method, http://loinc.org, null}], Stress Echo Adult with Treadmill Stress}"
        http_string_output = transform_struct_string_to_json(example_http_string)

        self.assertEqual(http_string_output, '{"coding": [{"code": "18107-3", "display": "Cardiac echo study Procedure stress method", "system": "http://loinc.org"}], "text": "Stress Echo Adult with Treadmill Stress"}')

        example_uri_string = "{[{72137, External ED Note, urn:oid:1.2.840.114350.1.13.297.2.7.4.686783.100, null}], External ED Note}"
        uri_string_output = transform_struct_string_to_json(example_uri_string)

        self.assertEqual(uri_string_output, '{"coding": [{"code": "72137", "display": "External ED Note", "system": "urn:oid:1.2.840.114350.1.13.297.2.7.4.686783.100"}], "text": "External ED Note"}')

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
            with (open(resources_folder(__file__, f"serialized_v{schema_version}.json")) as serialized_json):
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
