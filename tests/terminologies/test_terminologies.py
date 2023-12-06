import unittest
import uuid
import time
import app.terminologies.models
import app.models.codes
from app.app import create_app
from app.database import get_db
from app.errors import NotFoundException
from pytest import raises


class TerminologyTests(unittest.TestCase):
    """
    Safe Terminology UUIDs, see CodeTests class doc
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

    safe_term_uuid_fake = "d2ae0de5-0168-4f54-924a-1f79cf658939"
    safe_term_uuid_old = "3c9ed300-0cb8-47af-8c04-a06352a14b8d"
    safe_term_uuid_dupl = "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"
    safe_vsv_uuid = "58e792d9-1264-4f18-b16e-6292cb7ca597"

    def test_deduplicate_on_insert(self) -> None:
        """
        When `load_new_codes_to_terminology` is called on a Terminology (app.terminologies.models.Terminology)
        with the on_conflict_do_nothing parameter passed in as True, we want to verify that the deduplication
        behavior is occurring correctly.

        To do this, we will call `load_new_codes_to_terminology` and pass in a code already known to exist, as
        well as a new one. We will expect the final count to only be 1 (representing the new one).
        """
        duplicate_insert_test_terminology = app.terminologies.models.Terminology.load(
            self.safe_term_uuid_dupl
        )

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

    def test_get_terminology_happy(self):
        """
        Happy path where both valid fhir_uri and version are provided
        """
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

    def test_get_terminology_fhir_uri_empty(self):
        """
        Test when fhir_uri is empty
        """
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

    def test_get_terminology_version_empty(self):
        """
        Test when version is empty
        """
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

    def test_get_terminology_fhir_uri_invalid(self):
        """
        Test when fhir_uri is not a valid uri
        """
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
            ==
            "No terminology is found with the provided fhir_uri: "
            + "http://projectronin.io/fhir/CodeSystem/mock/DocumentType and version: 4"
        )

    def test_load_terminology_uuid_string(self):
        """
        happy path - string input
        """
        terminology = app.terminologies.models.Terminology.load(self.safe_term_uuid_fake)
        assert terminology.name == "Test  ONLY: fake/fhir_uri"

    def test_load_terminology_uuid_object(self):
        """
        happy path - UUID object input
        """
        uuid_object = uuid.UUID(self.safe_term_uuid_fake)
        terminology = app.terminologies.models.Terminology.load(uuid_object)
        assert terminology.name == "Test  ONLY: fake/fhir_uri"

    def test_load_terminology_null_uuid(self):
        """
        Cannot load a Terminology if the UUID is None
        """
        with raises(NotFoundException) as e:
            app.terminologies.models.Terminology.load(None)
        result = e.value
        assert result.message == f"No data found for terminology version UUID: None"

    def test_load_terminology_bad_uuid(self):
        """
        Cannot load a Terminology if the UUID is not the UUID of any Terminology
        """
        with raises(NotFoundException) as e:
            app.terminologies.models.Terminology.load(self.safe_vsv_uuid)
        result = e.value
        assert result.message == f"No data found for terminology version UUID: {self.safe_vsv_uuid}"


if __name__ == "__main__":
    unittest.main()
