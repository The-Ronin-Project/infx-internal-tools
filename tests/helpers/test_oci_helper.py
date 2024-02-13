import unittest

import pytest
from decouple import config

from app.helpers import oci_helper


class OciHelperTests(unittest.TestCase):

    """
    The tests below verify the behavior of the oci_helper methods. They are more integration tests than unit tests as
    they do interact with OCI. You will need to have an OCI key in your local env. Writing is restricted to the
    self.oci_test_dir_root value in order to protect production artifacts.

    TODO: When available, use the DISABLE_OCI_WRITE config value to make these tests safer.
    """

    def setUp(self):
        self.object_storage_client = oci_helper.oci_authentication()
        self.namespace = self.object_storage_client.get_namespace().data
        self.bucket_name = config("OCI_CLI_BUCKET")
        self.oci_test_dir_root = "infix-testing-folder"
        self.oci_test_file_name = "oci_overwrite_test"
        self.oci_test_file_extension = "txt"
        self.oci_test_file_path = f"{self.oci_test_dir_root}/{self.oci_test_file_name}.{self.oci_test_file_extension}"
        self.oci_test_file_content: dict = {
            "version": "oci_overwrite_test",
            "what_is_this": "This is a test file generated by a unit test"
        }


    def test_file_in_bucket_finds_file(self):
        """
        Given: A path to a file that exists in the OCI bucket
        When: oci_helper.folder_in_bucket is called
        Then: The file is found and the method returns true
        """

        folder_in_bucket = oci_helper.file_in_bucket(
            self.oci_test_file_path, self.object_storage_client, self.bucket_name, self.namespace)

        assert folder_in_bucket


    def test_file_in_bucket_fails_with_folder(self):
        """
        This test demonstrates that file_in_bucket() does not work to find folders and should only be used with files
        Given: A path to a folder that exists in the OCI bucket
        When: oci_helper.folder_in_bucket is called
        Then: The folder is not identifiable and the method returns false
        """
        path = "ConceptMaps/v4"
        folder_in_bucket = oci_helper.file_in_bucket(
            path, self.object_storage_client, self.bucket_name, self.namespace)

        assert not folder_in_bucket

    def test_file_in_bucket_does_not_find_file(self):
        """
        Given: A path to a test file that does not exist in the OCI bucket
        When: oci_helper.folder_in_bucket is called
        Then: The file is not found and the method returns false
        """
        path = "ConceptMaps/v4/published/03659ed9-c591-4bbc-9bcf-37260e0e402f/a_file_that_does_not_exist.txt"
        folder_in_bucket = oci_helper.file_in_bucket(
            path, self.object_storage_client, self.bucket_name, self.namespace)

        assert not folder_in_bucket


    def test_folder_path_for_oci(self):
        """
        Happy path test for folder_path_for_oci()
        Given: - A top level path to a test file in OCI
               - A dictionary containing a version field
               - A content type string
        When: oci_helper.folder_path_for_oci is called
        Then: The returned path will be <path>/<version>.<content type>
        """
        actual_folder_path = oci_helper.folder_path_for_oci(
            self.oci_test_file_content, self.oci_test_dir_root, self.oci_test_file_extension
        )
        assert actual_folder_path == self.oci_test_file_path


    def test_set_up_and_save_to_object_store_does_not_overwrite_by_default(self):
        """
        Given: - Path to an OCI file
               - Content of file
               - A content type string
        When: oci_helper.set_up_and_save_to_object_store is called with overwrite_enabled = false (default)
        Then: The method raises an exception
        """
        with pytest.raises(ValueError):
            oci_helper.set_up_and_save_to_object_store(
                self.oci_test_file_content,
                self.oci_test_file_path,
                overwrite_allowed=False
            )


    def test_set_up_and_save_to_object_store_does_overwrite_when_enabled(self):
        """
        Given: - A top level path to a test file in OCI
               - A dictionary containing a version field and some arbitrary content
               - A content type string
        When: oci_helper.set_up_and_save_to_object_store is called with overwrite_enabled = true
        Then: The method does not raise an exception. We make the assumption that the file is updated in OCI but don't
            confirm the data was actually updated in OCI in this test.
        """
        returned_content = oci_helper.set_up_and_save_to_object_store(
            self.oci_test_file_content,
            self.oci_test_file_path,
            overwrite_allowed=True
        )

        assert self.oci_test_file_content == returned_content
