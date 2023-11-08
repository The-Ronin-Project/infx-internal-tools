import unittest
import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.database import get_db


class ValueSetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_value_set_expand(self):
        """
        Expand the 'Automated Testing Value Set' value set and verify the outputs
        """
        value_set_version_uuid = "58e792d9-1264-4f18-b16e-6292cb7ca597"
        value_set_version = app.value_sets.models.ValueSetVersion.load(
            value_set_version_uuid
        )
        value_set_version.expand(force_new=True)

        self.assertEqual(11509, len(value_set_version.expansion))
