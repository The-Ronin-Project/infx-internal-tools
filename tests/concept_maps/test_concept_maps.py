import unittest

import app.app
from app.database import get_db
import app.concept_maps.models
import app.models.codes


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

    def test_load_concept_map_apposnd_observation(self):
        # Apposnd Observation to Ronin Observation version 12
        test_cm_version_uuid = "3534855d-e9f5-4514-9446-5a5d7b30edb4"

        cm_version = app.concept_maps.models.ConceptMapVersion(test_cm_version_uuid)

        source_concept_uuid_to_mapping = dict()
        for source_concept, mapping in cm_version.mappings.items():
            source_concept_uuid_to_mapping[str(source_concept.uuid)] = mapping

        no_map_source_concept_uuid = "c93a4c3d-c1e3-43c0-88ea-903d63780c7a"  # A no-map
        unreviewed_mapping_source_concept_uuid = "095be123-23ed-4c24-8aea-d6ec16ec21fa"  # An unreviewed mapping

        # Testing the case of a no map
        no_map_mapping: app.concept_maps.models.Mapping = source_concept_uuid_to_mapping[no_map_source_concept_uuid][0]
        no_map_source: app.concept_maps.models.SourceConcept = no_map_mapping.source

        # Verify the structure and content of the Code inside the SourceConcept
        self.assertEqual("6dd14cf0-e77b-45df-8b55-7a8ec676e8e5", str(no_map_source.code.custom_terminology_code_uuid))
        self.assertEqual(app.models.codes.RoninCodeSchemas.codeable_concept, no_map_source.code.code_schema)
        self.assertEqual(app.models.codes.FHIRCodeableConcept, type(no_map_source.code.code))
        self.assertEqual("FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - STAGE PREFIX", no_map_source.code.code.text)
        self.assertEqual(1, len(no_map_source.code.code.coding))
        self.assertEqual("FINDINGS - PHYSICAL EXAM - ONCOLOGY - STAGING - STAGE PREFIX", no_map_source.code.display)
        self.assertEqual("848fac05-bdb3-43b0-b0a7-9062e94b320a", str(no_map_source.code.terminology_version.uuid))

        # Verify other data from concept_maps.source_concept
        self.assertEqual("", no_map_source.comments)
        self.assertEqual("ready for review", no_map_source.map_status)
        self.assertEqual("3534855d-e9f5-4514-9446-5a5d7b30edb4", str(no_map_source.concept_map_version_uuid))
        self.assertTrue(isinstance(no_map_source.assigned_mapper, app.concept_maps.models.ContentCreator))
        self.assertEqual("Katelin Brown", str(no_map_source.assigned_mapper.first_last_name))
        self.assertIsNone(no_map_source.assigned_reviewer)
        self.assertTrue(no_map_source.no_map)
        self.assertEqual("Not in target code system", no_map_source.reason_for_no_map)
        self.assertIsNone(no_map_source.mapping_group)
        self.assertIsNone(no_map_source.previous_version_context)
        self.assertFalse(no_map_source.save_for_discussion)

        # Verify the relationship
        self.assertEqual("dca7c556-82d9-4433-8971-0b7edb9c9661", str(no_map_mapping.relationship.uuid))

        # Verify the target
        self.assertEqual("No map", no_map_mapping.target.code)
        self.assertEqual("No matching concept", no_map_mapping.target.display)
        self.assertEqual("93ec9286-17cf-4837-a4dc-218ce3015de6", str(no_map_mapping.target.terminology_version.uuid))

        # Verify the metadata
        self.assertTrue(isinstance(no_map_mapping.mapped_by, app.concept_maps.models.ContentCreator))
        self.assertEqual("Unknown User (for migration)", no_map_mapping.mapped_by.first_last_name)
        self.assertEqual("70b5405d-b2ab-481b-85a5-d5b305164851", str(no_map_mapping.mapped_by.uuid))
        self.assertEqual(2023, no_map_mapping.mapped_date_time.year)
        self.assertEqual(2, no_map_mapping.mapped_date_time.day)
        self.assertEqual(11, no_map_mapping.mapped_date_time.month)
        self.assertEqual(51, no_map_mapping.mapped_date_time.minute)
        self.assertEqual(17, no_map_mapping.mapped_date_time.hour)
        self.assertEqual(25, no_map_mapping.mapped_date_time.second)
        self.assertIsNone(no_map_mapping.reviewed_by)
        self.assertIsNone(no_map_mapping.reviewed_date_time)
        self.assertEqual("reviewed", no_map_mapping.review_status)
        self.assertIsNone(no_map_mapping.review_comments)
        self.assertIsNone(no_map_mapping.map_program_date_time)
        self.assertIsNone(no_map_mapping.map_program_version)
        self.assertIsNone(no_map_mapping.map_program_prediction_id)
        self.assertIsNone(no_map_mapping.map_program_confidence_score)
        self.assertIsNone(no_map_mapping.deleted_date_time)
        self.assertIsNone(no_map_mapping.deleted_by)

