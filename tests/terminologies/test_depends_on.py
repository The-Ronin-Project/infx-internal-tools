import datetime
import json
import unittest
import uuid

from pytest import raises
from sqlalchemy import text

import app.models.codes
from app.app import create_app
from app.database import get_db
from app.helpers.format_helper import prepare_depends_on_attributes_for_code_id


class DependsOnDataTests(unittest.TestCase):
    """
    These tests instantiate the Code class directly, as opposed to using API calls as below.
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

    def test_setup_from_database_columns(self):
        test_depends_on_value = """
        {
          "coding": [
            {
              "code": "EPIC#31000073350",
              "display": "age at diagnosis",
              "system": "urn:oid:1.2.840.114350.1.13.412.2.7.2.727688"
            }
          ],
          "text": "FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - AGE AT DIAGNOSIS"
        }
        """
        new_depends_on = app.models.codes.DependsOnData.setup_from_database_columns(
            depends_on_property="Observation.code",
            depends_on_value_schema=app.models.codes.DependsOnSchemas.CODEABLE_CONCEPT.value,
            depends_on_value_simple=None,
            depends_on_value_jsonb=test_depends_on_value,
            depends_on_system=None,
            depends_on_display=None
        )
        self.assertEqual(app.models.codes.FHIRCodeableConcept, type(new_depends_on.depends_on_value))
        self.assertEqual(new_depends_on.depends_on_property, "Observation.code")

    def test_serialize_for_code_id(self):
        """
        Tests DependsOnData.serialize_for_code_id(),
        format_helper.py prepare_depends_on_attributes_for_code_id(),
        data_helper.py normalized_source_codeable_concept()
        """
        test_depends_on_value = """
        {
          "text": "FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - AGE AT DIAGNOSIS",
          "coding": [
            {
              "code": "EPIC#31000073350",
              "display": "age at diagnosis",
              "system": "urn:oid:1.2.840.114350.1.13.412.2.7.2.727688"
            }
          ]
        }
        """
        new_depends_on = app.models.codes.DependsOnData.setup_from_database_columns(
            depends_on_property="Observation.code",
            depends_on_value_schema=app.models.codes.DependsOnSchemas.CODEABLE_CONCEPT.value,
            depends_on_value_simple=None,
            depends_on_value_jsonb=test_depends_on_value,
            depends_on_system=None,
            depends_on_display=None
        )
        depends_on_for_code_id_1 = prepare_depends_on_attributes_for_code_id(new_depends_on)
        assert depends_on_for_code_id_1 == '{"coding":[{"code":"EPIC#31000073350","display":"age at diagnosis","system":"urn:oid:1.2.840.114350.1.13.412.2.7.2.727688"}],"text":"FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - AGE AT DIAGNOSIS"}Observation.code'
        depends_on_for_code_id_2 = new_depends_on.serialize_for_code_id()
        assert depends_on_for_code_id_2 == depends_on_for_code_id_1
