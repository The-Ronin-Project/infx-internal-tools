import unittest

from app.app import create_app
from app.database import get_db


class ObservationInterpretationRegistryTests(unittest.TestCase):
    """
    These tests will test that flexible registries of type `observation_interpretation` include all
    the regular required fields, as well as the `product_item_long_label` field.

    For these tests, we will use the `TEST ONLY Observation Interpretation Registry` and
    the `Test group` within it.
    """

    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update(
            {
                "TESTING": True,
            }
        )
        self.client = self.app.test_client()

        self.test_registry_uuid = "d9ae8dd2-5f08-42a7-bbdf-f44c503a3e11"
        self.test_group_uuid = "8c69fe0f-d268-4cac-920c-1cb3cbcafd2d"

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_add_group_member(self):
        """Test adding H, High to the registry"""
        response = self.client.post(
            f"registries/{self.test_registry_uuid}/groups/{self.test_group_uuid}/members/",
            json={
                "value_set_uuid": "72c1c498-f559-4639-b3b1-229d2472029c",  # High Interpretation value set
                "title": "H",
                "product_item_long_label": "High",
            },
        )
        json_response = response.get_json()
        self.assertEqual(json_response.get("group_uuid"), self.test_group_uuid)
        self.assertIn("product_item_long_label", json_response)

        product_item_long_label = json_response.get("product_item_long_label")
        self.assertEqual(product_item_long_label, "High")

        title = json_response.get("title")
        self.assertEqual(title, "H")

    def test_export(self):
        response = self.client.get(f"registries/{self.test_registry_uuid}/pending")
        self.assertEqual(type(response.json), list)
        first_item = response.json[0]

        self.assertEqual(first_item.get("productGroupLabel"), "Test group")
        self.assertEqual(first_item.get("productItemLabel"), "A")
        self.assertEqual(first_item.get("productItemLongLabel"), "Abnormal")
        self.assertEqual(
            first_item.get("valueSetUuid"), "72c1c498-f559-4639-b3b1-229d2472029c"
        )
