import datetime
import json
import unittest

from sqlalchemy import text
from werkzeug.exceptions import NotFound

import app.terminologies.models
import app.models.codes
from app.app import create_app
from app.database import get_db


class CodeTests(unittest.TestCase):
    """
    There are 3 public.terminology_versions rows safe to use in tests that also pass checks to allow codes to be created
    ```
    terminology                    version uri                 is_standard fhir_terminology effective_start _end
    "Test ONLY: fake/fhir_uri"         "3" "fake/fhir_uri"                      false false "2023-04-12"
    "Test ONLY: http://test_test.com"  "1" "http://test_test.com"               false false                 "2023-09-01"
    "Test ONLY: Duplicate Insert Test" "1" "http://testing/duplicateInsertTest" false false "2023-11-09"    "2030-11-16"
    ```
    ```
    terminology                        uuid                                   variable            purpose
    "Test ONLY: fake/fhir_uri"         "d2ae0de5-0168-4f54-924a-1f79cf658939" safe_term_uuid_fake has no expiry date
    "Test ONLY: http://test_test.com"  "3c9ed300-0cb8-47af-8c04-a06352a14b8d" safe_term_uuid_test expiry date has passed
    "Test ONLY: Duplicate Insert Test" "d14cbd3a-aabe-4b26-b754-5ae2fbd20949" safe_term_uuid_dupl has future expiry date

    How to check for values being written by these tests - in case cleanup is not done properly (Ctrl-C or Stop):

    select * from public.terminology_versions
    where uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939', '3c9ed300-0cb8-47af-8c04-a06352a14b8d',
    'd14cbd3a-aabe-4b26-b754-5ae2fbd20949')

    select * from custom_terminologies.code
    where terminology_version_uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939', '3c9ed300-0cb8-47af-8c04-a06352a14b8d',
    'd14cbd3a-aabe-4b26-b754-5ae2fbd20949')
    order by terminology_version_uuid

    delete from custom_terminologies.code
    where terminology_version_uuid in
    ('d2ae0de5-0168-4f54-924a-1f79cf658939','3c9ed300-0cb8-47af-8c04-a06352a14b8d','d14cbd3a-aabe-4b26-b754-5ae2fbd20949')
    and code like 'test code at 20%'

    """
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        # this executes after each test function, but does not stop lower-level functions from committing db changes
        self.conn.rollback()
        self.conn.close()

    safe_term_uuid_fake = "d2ae0de5-0168-4f54-924a-1f79cf658939"
    safe_term_uuid_test = "3c9ed300-0cb8-47af-8c04-a06352a14b8d"
    safe_term_uuid_dupl = "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"

    def test_create_code_happy_future(self):
        """
         happy path - all inputs - expiry date in future
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code at {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_dupl,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

    def test_create_code_happy_forever(self):
        """
        happy path - all inputs - no expiry date
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code at {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        assert response.text == "Complete"

    def test_create_code_outdated(self):
        """
        outdated terminology version - expiry date in past
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": f"""test code {datetime.datetime.utcnow()}""",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_test,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.load_new_codes.closed_to_new_codes"
        assert result.get("message") == "Cannot add new codes to a terminology that has ended its effective period."

    def test_create_code_not_unique(self):
        """
        non-unique code value - the code "test code" already exists in the safe_term_uuid_fake Terminology
        """
        response = self.client.post(
            "/terminology/new_code",
            data=json.dumps(
                [
                    {
                        "code": "test code",
                        "display": "test display",
                        "terminology_version_uuid": self.safe_term_uuid_fake,
                        "depends_on_value": "a",
                        "depends_on_display": "b",
                        "depends_on_property": "c",
                        "depends_on_system": "d",
                        "additional_data": {
                            "data": "sweet sweet json"
                        }
                    }
                ]
            ),
            content_type="application/json",
        )
        result = response.json
        assert response.status == "400 BAD REQUEST"
        assert result.get("code") == "Terminology.create_code.database_error"
        error_text = "(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint"
        assert error_text in result.get("message")


if __name__ == "__main__":
    unittest.main()
