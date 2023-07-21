import unittest
from app.database import get_db

import app.models.normalization_error_service
import app.models.models


class NormalizationErrorServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_db()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_lookup_registry_cerner_sandbox_observations(self):
        """
        Tests that the appropriate concept map to normalize Observations in
        the Cerner sandbox can be looked up
        """
        cerner_sandbox_organization = app.models.models.Organization(id="ejh3j95h")

        concept_map = app.models.normalization_error_service.lookup_concept_map_version_for_data_element(
            data_element="Observation.code",
            organization=cerner_sandbox_organization,
        )
        print(concept_map)
        assert 1 == 0

    def test_lookup_registry_psj_prod_observations(self):
        """
        Tests that the appropriate concept map to normalize Observations in
        PSJ Pord can be looked up
        """
        psj_prod_organization = app.models.models.Organization(id="v7r1eczk")

        concept_map = app.models.normalization_error_service.lookup_concept_map_version_for_data_element(
            data_element="Observation.code",
            organization=psj_prod_organization,
        )
        print(concept_map)
        assert 1 == 0
