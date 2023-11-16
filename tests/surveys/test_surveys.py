import hashlib
import unittest
from app.database import get_db
from app.app import create_app


class SurveyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_survey_export(self):
        response = self.app.test_client().get(
            "/surveys/34775510-1267-11ec-b9a3-77c9d91ff3f2?organization_uuid=866632f0-ff85-11eb-9f47-ffa6d132f8a4"
        )
        # print(hashlib.md5(response.data).hexdigest())
        assert hashlib.md5(response.data).hexdigest() == "2282000fef5e7d7b4101de88299c5da2"


if __name__ == '__main__':
    unittest.main()
