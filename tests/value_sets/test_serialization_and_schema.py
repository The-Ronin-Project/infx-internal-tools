import unittest
from app.database import get_db
from app.app import create_app
from app.value_sets.models import ValueSetVersion  # Replace with your actual import


class ValueSetVersionSerializationTests(unittest.TestCase):
    """
    Test serialization of ValueSetVersion instances across different schema versions.
    """

    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({"TESTING": True})
        self.client = self.app.test_client()

        self.mock_value_set_version = ValueSetVersion.load(
            "6dc32499-2804-4b94-8e9a-8c2bad44fb88"
        )  # Test ONLY: Test August 29 (no map + diabetes)

    def test_serialize_schema_version_2(self):
        """
        Test serialization of a ValueSetVersion instance using schema version 2.
        """
        serialized_data = self.mock_value_set_version.serialize(schema_version=2)
        self.assertEqual(
            serialized_data["description"],
            "no map + diabetes Updated for ICD-10 CM, 2024  update",
        )
        self.assertEqual(
            serialized_data["useContext"],
            [
                {
                    "code": {
                        "code": "workflow",
                        "display": "Workflow Setting",
                        "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
                    },
                    "valueCodeableConcept": {
                        "coding": [{"code": "Testing"}, {"code": "test edit"}]
                    },
                }
            ],
        )  #
        self.assertEqual(serialized_data["extension"][0]["valueString"], "2")

    def test_serialize_schema_version_5(self):
        """
        Test serialization of a ValueSetVersion instance using schema version 5.
        """
        serialized_data = self.mock_value_set_version.serialize(schema_version=5)
        self.assertEqual(serialized_data["description"], "no map + diabetes")
        self.assertEqual(
            serialized_data["useContext"],
            [
                [
                    {
                        "code": {
                            "code": "workflow",
                            "display": "Workflow Setting",
                            "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
                        },
                        "valueCodeableConcept": {
                            "coding": [{"code": "Testing"}, {"code": "test edit"}]
                        },
                    }
                ]
            ],
        )
        self.assertEqual(serialized_data["extension"][0]["valueString"], "5")

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()


if __name__ == "__main__":
    unittest.main()
