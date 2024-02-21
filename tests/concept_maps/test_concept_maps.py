import unittest

import app.app
from app.database import get_db
import app.concept_maps.models


class ConceptMapTests(unittest.TestCase):
    """
    There are 21 concept_maps.concept_map rows safe to use in tests.
    For the list, see app.util.enum.concept_maps_for_systems.ConceptMapsForSystems

    You may wish to choose a concept map based on its source_value_set_uuid or target_value_set_uuid.
    Here is an example of a query you can run:
    ```
    select cm.title, cm.uuid, cmv.uuid as version_uuid, cmv.version, cmv.status
    from concept_maps.concept_map as cm
    join concept_maps.concept_map_version as cmv
    on cm.uuid = cmv.concept_map_uuid
    where cm.source_value_set_uuid = 'ddcbf55b-312d-4dd9-965d-f72c4bc51ddc'
    and title like 'Test ONLY:%'
    order by cm.uuid asc, cmv.version desc
    ```
    """

    def setUp(self) -> None:
        self.conn = get_db()
        self.app = app.app.create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_load_concept_map(self):
        test_cm_version_uuid = "3534855d-e9f5-4514-9446-5a5d7b30edb4"  # Apposnd Observation to Ronin Observation version 12
        cm_version = app.concept_maps.models.ConceptMapVersion(test_cm_version_uuid)

        print(cm_version.mappings)