import unittest
import uuid

import app.concept_maps.models
import app.models.codes
from app.app import create_app
from app.database import get_db
from app.errors import NotFoundException
from pytest import raises


class MappingRelationshipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update(
            {
                "TESTING": True,
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    test_uuid = "2ccf263a-b5a4-4cd5-908e-a988ff965319"
    bad_uuid = "58e792d9-1264-4f18-b16e-6292cb7ca597"

    def test_load_by_uuid(self):
        """
        happy path - UUID input
        """
        uuid_object = uuid.UUID(self.test_uuid)
        mapping_relationship = app.concept_maps.models.MappingRelationship.load_by_uuid(
            uuid_object
        )
        self.assertEqual(mapping_relationship.uuid, uuid_object)
        self.assertEqual(mapping_relationship.code, "related-to")
        self.assertEqual(mapping_relationship.display, "Related To")

    def test_load_by_null_uuid(self):
        """
        Cannot load a mapping relationship code if the UUID is None
        """
        with raises(NotFoundException) as e:
            app.concept_maps.models.MappingRelationship.load_by_uuid(None)
        result = e.value
        assert result.message == f"No data found for mapping relationship UUID: None"

    def test_load_by_bad_uuid(self):
        """
        Cannot load a mapping relationship code if the UUID is not the UUID of any mapping relationship
        """
        with raises(NotFoundException) as e:
            app.concept_maps.models.MappingRelationship.load_by_uuid(self.bad_uuid)
        result = e.value
        assert (
            result.message
            == f"No data found for mapping relationship UUID: {self.bad_uuid}"
        )


if __name__ == "__main__":
    unittest.main()
