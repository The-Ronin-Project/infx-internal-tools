import json
import unittest
import uuid
import time
from datetime import datetime, date

import app.terminologies.models
import app.models.codes
from app.app import create_app
from app.database import get_db
from app.errors import NotFoundException
from pytest import raises


class TerminologyTests(unittest.TestCase):
    """
    There are 5 public.terminology_versions rows safe to use in tests that also pass checks to allow codes to be created
    - for descriptions of these rows, see CodeTests class doc.

    For some additional terminology rows, use this SQL query:
    ```
    select * from public.terminology_versions
    where terminology like 'Test%'
    order by fhir_uri, terminology, version desc
    ```
    Of the public.terminology_versions that return from the above query for safe Terminology version UUIDs,
    those with corresponding custom_terminologies.code rows can be listed using this query:
    ```
    select * from custom_terminologies.code
    where terminology_version_uuid in
    (
    'a95fce32-b127-4bdf-8fc4-0ee4277fb9dd',
    '011497ab-1092-46c5-b66a-95e4acef599b',
    'd2ae0de5-0168-4f54-924a-1f79cf658939',
    '390edf3e-af57-4280-b4f1-9661b5bb66d9',
    '19ef56aa-ad4d-4ae8-aa0c-f43bdb6ed01a',
    '24360d99-7630-46c4-946d-eb12b6865db8',
    'd49d2294-c6f2-4d2e-9c84-33d629db828f',
    'ded05ca2-3573-4524-b33c-9528017d057e',
    '10a64265-bc22-4881-b33d-eddb0855bdf8'
    )
    order by terminology_version_uuid
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

    def test_create_new_terminology_happy(self):
        """
        create_new_terminology() helper method - happy path - with serialize()

        Inputs:
            terminology (str): The name of the new terminology.
            version (str): The version of the new terminology.
            effective_start (datetime.datetime): The effective start date of the new terminology.
            effective_end (datetime.datetime): The effective end date of the new terminology.
            fhir_uri (str): The FHIR URI of the new terminology.
            is_standard (bool): Whether the new terminology is standard or not.
            fhir_terminology (bool): Whether the new terminology is a FHIR terminology.
        """
        terminology = app.terminologies.models.Terminology.create_new_terminology(
            terminology=f"test name {datetime.utcnow()}",
            version="999",
            effective_start=datetime(2023, 12, 3),
            effective_end=datetime(2030, 12, 2),
            fhir_uri="fake_uri",
            is_standard=False,
            fhir_terminology=True,
        )
        assert terminology is not None
        serialized = app.terminologies.models.Terminology.serialize(terminology)
        assert len(serialized) == 8
        assert serialized.get("uuid") is not None
        assert "test name " in serialized.get("name")
        assert serialized.get("version") == "999"
        assert serialized.get("effective_start") == date(2023, 12, 3)
        assert serialized.get("effective_end") == date(2030, 12, 2)
        assert serialized.get("fhir_uri") == "fake_uri"
        assert serialized.get("is_standard") == False
        assert serialized.get("fhir_terminology") == True

    def test_create_terminology_happy(self):
        """
        create_terminology() API endpoint - happy path
        """
        response = self.client.post(
            "/terminology/",
            data=json.dumps(
                {
                    "terminology": f"test name {datetime.utcnow()}",
                    "version": "99",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                    "fhir_uri": f"fake_uri {datetime.utcnow()}",
                    "is_standard": False,
                    "fhir_terminology": True
                }
            ),
            content_type="application/json",
        )
        serialized = response.json
        assert len(serialized) == 8
        assert serialized.get("uuid") is not None
        assert "test name" in serialized.get("name")
        assert serialized.get("version") == "99"
        assert "03 Dec 2023 00:00:00" in serialized.get("effective_start")
        assert "02 Dec 2023 00:00:00" in serialized.get("effective_end")
        assert "fake_uri" in serialized.get("fhir_uri")
        assert serialized.get("is_standard") == False
        assert serialized.get("fhir_terminology") == True

    def test_create_terminology_no_terminology(self):
        """
        create_terminology() API endpoint - no terminology name value is supplied
        """
        response = self.client.post(
            "/terminology/",
            data=json.dumps(
                {
                    "version": f"{datetime.utcnow()}",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                    "fhir_uri": f"fake_uri {datetime.utcnow()}",
                    "is_standard": False,
                    "fhir_terminology": False
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_terminology.database_error"
        error_text = """(psycopg2.errors.NotNullViolation) null value in column "terminology" of relation "terminology_versions" violates not-null constraint"""
        assert error_text in result.get("message")

    def test_create_terminology_no_version(self):
        """
        create_terminology() API endpoint - no terminology name value is supplied
        """
        response = self.client.post(
            "/terminology/",
            data=json.dumps(
                {
                    "terminology": f"test name {datetime.utcnow()}",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                    "fhir_uri": f"fake_uri {datetime.utcnow()}",
                    "is_standard": False,
                    "fhir_terminology": False
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_terminology.database_error"
        error_text = """(psycopg2.errors.NotNullViolation) null value in column "version" of relation "terminology_versions" violates not-null constraint"""
        assert error_text in result.get("message")

    def test_create_terminology_no_is_standard(self):
        """
        create_terminology() API endpoint - no terminology name value is supplied
        """
        response = self.client.post(
            "/terminology/",
            data=json.dumps(
                {
                    "terminology": f"test name {datetime.utcnow()}",
                    "version": f"{datetime.utcnow()}",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                    "fhir_uri": f"fake_uri {datetime.utcnow()}",
                    "fhir_terminology": False,
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_terminology.database_error"
        error_text = """(psycopg2.errors.NotNullViolation) null value in column "is_standard" of relation "terminology_versions" violates not-null constraint"""
        assert error_text in result.get("message")

    def test_create_terminology_no_fhir_terminology(self):
        """
        create_terminology() API endpoint - no terminology name value is supplied
        """
        response = self.client.post(
            "/terminology/",
            data=json.dumps(
                {
                    "terminology": f"test name {datetime.utcnow()}",
                    "version": f"{datetime.utcnow()}",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                    "fhir_uri": f"fake_uri {datetime.utcnow()}",
                    "is_standard": False,
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_terminology.database_error"
        error_text = """(psycopg2.errors.NotNullViolation) null value in column "fhir_terminology" of relation "terminology_versions" violates not-null constraint"""
        assert error_text in result.get("message")

    def test_new_terminology_version_from_previous_happy(self):
        """
        new_terminology_version_from_previous() helper method - happy path - with serialize()

        Inputs:
            previous_version_uuid (UUID): The UUID of the previous terminology version.
            version (str): The version of the new terminology.
            effective_start (datetime.datetime): The effective start date of the new terminology.
            effective_end (datetime.datetime): The effective end date of the new terminology.
        """
        terminology = app.terminologies.models.Terminology.new_terminology_version_from_previous(
            f"{self.safe_term_uuid_dupl}",
            "999",
            effective_start=datetime(2023, 12, 3),
            effective_end=datetime(2030, 12, 2),
        )
        serialized = app.terminologies.models.Terminology.serialize(terminology)
        assert len(serialized) == 8
        assert serialized.get("uuid") is not None
        assert serialized.get("name") == "Test ONLY: Duplicate Insert Test"
        assert serialized.get("version") == "999"
        assert serialized.get("effective_start") == date(2023, 12, 3)
        assert serialized.get("effective_end") == date(2030, 12, 2)
        assert serialized.get("fhir_uri") == "http://testing/duplicateInsertTest"
        assert serialized.get("is_standard") == False
        assert serialized.get("fhir_terminology") == False

    def test_create_term_from_previous_happy(self):
        """
        create_new_term_version_from_previous() API endpoint - happy path
        """
        response = self.client.post(
            "/terminology/new_version_from_previous",
            data=json.dumps(
                {
                    "previous_terminology_version_uuid": f"{self.safe_term_uuid_dupl}",
                    "version": "99",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                }
            ),
            content_type="application/json",
        )
        serialized = response.json
        assert len(serialized) == 8
        assert serialized.get("uuid") is not None
        assert serialized.get("name") == "Test ONLY: Duplicate Insert Test"
        assert serialized.get("version") == "99"
        assert "03 Dec 2023 00:00:00" in serialized.get("effective_start")
        assert "02 Dec 2023 00:00:00" in serialized.get("effective_end")
        assert serialized.get("fhir_uri") == "http://testing/duplicateInsertTest"
        assert serialized.get("is_standard") == False
        assert serialized.get("fhir_terminology") == False

    def test_create_term_from_previous_not_found(self):
        """
        create_new_term_version_from_previous() - previous not found
        """
        response = self.client.post(
            "/terminology/new_version_from_previous",
            data=json.dumps(
                {
                    "previous_terminology_version_uuid": f"{self.safe_vsv_uuid}",
                    "version": "99",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_new_term_version_from_previous.no_previous"
        assert result.get("message") == f"No Terminology found with UUID: {self.safe_vsv_uuid}"

    def test_create_term_from_previous_no_version(self):
        """
        create_new_term_version_from_previous() - version not input for new terminology
        """
        response = self.client.post(
            "/terminology/new_version_from_previous",
            data=json.dumps(
                {
                    "previous_terminology_version_uuid": f"{self.safe_term_uuid_dupl}",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_new_term_version_from_previous.database_error"
        assert f"""(psycopg2.errors.NotNullViolation) null value in column "version" of relation "terminology_versions" violates not-null constraint""" in result.get("message")

    def test_create_term_from_previous_conflict_version(self):
        """
        create_new_term_version_from_previous() - version value already exists
        """
        response = self.client.post(
            "/terminology/new_version_from_previous",
            data=json.dumps(
                {
                    "previous_terminology_version_uuid": f"{self.safe_term_uuid_dupl}",
                    "version": "1",
                    "effective_start": "2023-12-03",
                    "effective_end": "2023-12-02",
                }
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_new_term_version_from_previous.database_error"
        error_text = "(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint \"unique_version_fhir_uri\""
        assert error_text in result.get("message")

if __name__ == "__main__":
    unittest.main()
