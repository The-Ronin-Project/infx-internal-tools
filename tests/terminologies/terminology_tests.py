import unittest
import uuid
import time
import app.terminologies.models
import app.models.codes
from app.app import create_app
from app.database import get_db


class TerminologyTests(unittest.TestCase):
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

    def test_deduplicate_on_insert(self) -> None:
        """
        When `load_new_codes_to_terminology` is called on a Terminology (app.terminologies.models.Terminology)
        with the on_conflict_do_nothing parameter passed in as True, we want to verify that the deduplication
        behavior is occurring correctly.

        To do this, we will call `load_new_codes_to_terminology` and pass in a code already known to exist, as
        well as a new one. We will expect the final count to only be 1 (representing the new one).
        """
        duplicate_insert_test_terminology = app.terminologies.models.Terminology.load(
            "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"
        )  # Duplicate Insert Test Terminology

        code1 = app.models.codes.Code(
            code="test1",
            display="Test 1",
            system=None,
            version=None,
            terminology_version_uuid=duplicate_insert_test_terminology.uuid,
            custom_terminology_code_uuid=uuid.uuid4(),
        )  # This code is known to already exist in this terminology

        current_unix_timestamp = str(time.time())
        new_code = app.models.codes.Code(
            code=f"test{current_unix_timestamp}",
            display=f"Test {current_unix_timestamp}",
            system=None,
            version=None,
            terminology_version_uuid=duplicate_insert_test_terminology.uuid,
            custom_terminology_code_uuid=uuid.uuid4(),
        )  # Use a unit timestamp to form a new code

        inserted_count = (
            duplicate_insert_test_terminology.load_new_codes_to_terminology(
                [code1, new_code], on_conflict_do_nothing=True
            )
        )
        self.assertEqual(1, inserted_count)

    def test_get_terminology(self):
        """
        Unit test for get_terminology API endpoint where a terminology is looked up by its fhir_uri and version.
        """

        # Happy path where both valid fhir_uri and version are provided
        response = self.client.get(
            "/terminology/",
            query_string={
                "fhir_uri": "http://projectronin.io/fhir/CodeSystem/apposnd/DocumentType",
                "version": 4,
            },
            content_type="application/json",
        )
        result = response.json
        assert result.get("name") == "apposnd_document_type"
        assert result.get("fhir_terminology") is False

        # Test when fhir_uri is empty
        response = self.client.get(
            "/terminology/",
            query_string={
                "version": 4,
            },
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("error") == "fhir_uri and version parameters are required."

        # Test when version is empty
        response = self.client.get(
            "/terminology/",
            query_string={
                "fhir_uri": "http://projectronin.io/fhir/CodeSystem/apposnd/DocumentType",
                "version": "",
            },
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("error") == "fhir_uri and version parameters are required."

        # Test when fhir_uri is not a valid uri
        response = self.client.get(
            "/terminology/",
            query_string={
                "fhir_uri": "http://projectronin.io/fhir/CodeSystem/mock/DocumentType",
                "version": 4,
            },
            content_type="application/json",
        )
        result = response.json
        assert response.status == "404 NOT FOUND"
        assert (
            result.get("message")
            == "No terminology is found with the provided fhir_uri: http://projectronin.io/fhir/CodeSystem/mock/DocumentType and version: 4"
        )


if __name__ == "__main__":
    unittest.main()
