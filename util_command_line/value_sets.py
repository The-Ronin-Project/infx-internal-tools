from unittest import skip

from app.value_sets.models import ValueSetVersion, ValueSet


@skip("This is a utility, not a test. Use only for OCI repairs by Informatics Systems team members.")
def test_value_set_output_to_oci():
    """
    Not a test. Really a tool for developers to push content to OCI for urgent reasons. Be aware that if there is
    already an output file for a version number present in the OCI folder, this function will not overwrite it. If it is
    imperative to overwrite the previously output file for a version number, you must remove that file from OCI first.
    """
    test_value_set_version_uuid = "(insert the version uuid here)"  # use this invalid value on purpose when merging
    value_set_output_to_oci(test_value_set_version_uuid, ValueSet.database_schema_version)
    if ValueSet.database_schema_version != ValueSet.next_schema_version:
        value_set_output_to_oci(test_value_set_version_uuid, ValueSet.next_schema_version)


def value_set_output_to_oci(test_value_set_version_uuid: str, schema_version: int):
    """
    Helper function for test_value_set_output_to_oci.
    @param test_value_set_version_uuid, see tests/value_sets/test_value_sets.py for a list of safe values
    @param schema_version: current and/or next schema version for ValueSet as input by test_value_set_output().
    """
    test_value_set_version = ValueSetVersion(test_value_set_version_uuid)
    if test_value_set_version is None:
        print(f"Version with UUID {test_value_set_version_uuid} is None")
    else:
        test_value_set_version.send_to_oci(schema_version)
    # look in OCI to see the value set files
