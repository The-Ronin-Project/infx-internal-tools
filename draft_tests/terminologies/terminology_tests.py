import unittest
import uuid
import time
import app.terminologies.models
import app.models.codes
from app.database import get_db


class TerminologyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()

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
        duplicate_insert_test_terminology = app.terminologies.models.Terminology.load("d14cbd3a-aabe-4b26-b754-5ae2fbd20949")  # Duplicate Insert Test Terminology

        code1 = app.models.codes.Code(
            code='test1',
            display='Test 1',
            system=None,
            version=None,
            terminology_version_uuid=duplicate_insert_test_terminology.uuid,
            custom_terminology_code_uuid=uuid.uuid4(),
        )  # This code is known to already exist in this terminology

        current_unix_timestamp = str(time.time())
        new_code = app.models.codes.Code(
            code=f'test{current_unix_timestamp}',
            display=f'Test {current_unix_timestamp}',
            system=None,
            version=None,
            terminology_version_uuid=duplicate_insert_test_terminology.uuid,
            custom_terminology_code_uuid=uuid.uuid4(),
        )  # Use a unit timestamp to form a new code

        inserted_count = duplicate_insert_test_terminology.load_new_codes_to_terminology(
            [code1, new_code],
            on_conflict_do_nothing=True
        )
        self.assertEqual(1, inserted_count)


if __name__ == '__main__':
    unittest.main()
