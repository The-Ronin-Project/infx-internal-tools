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
        self.app.config.update({
            "DISABLE_ROLLBACK_AFTER_REQUEST": False
        })
        self.app.config.update({
            "DISABLE_CLOSE_AFTER_REQUEST": False
        })
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

    def test_create_mapping(self):
        """
        In this test, we will map `Ascending colon cancer` to `Primary malignant neoplasm of ascending colon (disorder)`

        This will occur in the `Test ONLY: Condition Codeable Concepts to SNOMED` concept map version 1
        """
        test_codeable_concepts_v1_version_uuid = "1e7a9753-375c-4e6f-b4be-446946fcc143"

        ascending_colon_cancer_source_concept_uuid = "a55142ba-9192-4bec-b370-43c58ee2f4ce"

        # Configure so we can make a request and leave the database connection open
        self.app.config.update({
            "DISABLE_ROLLBACK_AFTER_REQUEST": True
        })
        self.app.config.update({
            "DISABLE_CLOSE_AFTER_REQUEST": True
        })

        # POST to /mappings/
        self.client.post(
            "/mappings/",
            json={
                "source_concept_uuids": [ascending_colon_cancer_source_concept_uuid],
                "relationship_code_uuid": "f2a20235-bd9d-4f6a-8e78-b3f41f97d07f",  # Equivalent
                "target_concept_code": "93683002",
                "target_concept_display": "Primary malignant neoplasm of ascending colon (disorder)",
                "target_concept_terminology_version_uuid": "dd33a8e7-c2d5-4ca6-8c74-b857c94d9ed9",
                "mapping_comments": "Test mapping comments",
                "mapped_by_uuid": "951d32b4-06a1-4b86-842e-57b8bef9bcd8",  # Rey Johnson
                "review_status": "reviewed"  # todo: maybe use a subsequent call to mark it reviewed for accuracy
            }
        )

        # Verify the mapping we just made
        cm_version = app.concept_maps.models.ConceptMapVersion(test_codeable_concepts_v1_version_uuid)

        source_concept_uuid_to_mapping = dict()
        for source_concept, mapping in cm_version.mappings.items():
            source_concept_uuid_to_mapping[str(source_concept.uuid)] = mapping

        # Testing the case of a no map
        new_mapping: app.concept_maps.models.Mapping = source_concept_uuid_to_mapping[ascending_colon_cancer_source_concept_uuid][0]
        new_mapping_source: app.concept_maps.models.SourceConcept = new_mapping.source

        # Verify the structure and content of the Code inside the SourceConcept
        self.assertEqual("5f48c020-1bcb-44bd-90af-251b168bd1c4", str(new_mapping_source.code.custom_terminology_code_uuid))
        self.assertEqual(app.models.codes.RoninCodeSchemas.codeable_concept, new_mapping_source.code.code_schema)
        self.assertEqual(app.models.codes.FHIRCodeableConcept, type(new_mapping_source.code.code))
        self.assertEqual("Ascending colon cancer", new_mapping_source.code.code.text)
        self.assertEqual(1, len(new_mapping_source.code.code.coding))
        self.assertEqual("Ascending colon cancer", new_mapping_source.code.display)
        self.assertEqual("7bfa2bdb-6632-47c8-9bab-aad107bc31bf", str(new_mapping_source.code.terminology_version.uuid))

        # Verify other data from concept_maps.source_concept
        self.assertEqual("pending", new_mapping_source.map_status)  # todo: this should be "ready for review"
        # self.assertEqual("3534855d-e9f5-4514-9446-5a5d7b30edb4", str(new_mapping_source.concept_map_version_uuid))
        self.assertTrue(isinstance(new_mapping_source.assigned_mapper, app.concept_maps.models.ContentCreator))
        self.assertEqual("Automap", str(new_mapping_source.assigned_mapper.first_last_name))
        self.assertIsNone(new_mapping_source.assigned_reviewer)
        self.assertFalse(new_mapping_source.no_map)
        self.assertIsNone(new_mapping_source.reason_for_no_map)
        self.assertIsNone(new_mapping_source.mapping_group)
        self.assertIsNone(new_mapping_source.previous_version_context)
        self.assertFalse(new_mapping_source.save_for_discussion)

        # Verify the relationship
        self.assertEqual("f2a20235-bd9d-4f6a-8e78-b3f41f97d07f", str(new_mapping.relationship.uuid))

        # Verify the target
        self.assertEqual("93683002", new_mapping.target.code)
        self.assertEqual("Primary malignant neoplasm of ascending colon (disorder)", new_mapping.target.display)
        self.assertEqual("dd33a8e7-c2d5-4ca6-8c74-b857c94d9ed9", str(new_mapping.target.terminology_version.uuid))

        # Verify the metadata
        self.assertTrue(isinstance(new_mapping.mapped_by, app.concept_maps.models.ContentCreator))
        self.assertEqual("Rey Johnson", new_mapping.mapped_by.first_last_name)
        self.assertEqual("951d32b4-06a1-4b86-842e-57b8bef9bcd8", str(new_mapping.mapped_by.uuid))
        self.assertIsNotNone(new_mapping.mapped_date_time)
        self.assertIsNone(new_mapping.reviewed_by)
        self.assertIsNone(new_mapping.reviewed_date_time)
        self.assertEqual("reviewed", new_mapping.review_status)
        self.assertIsNone(new_mapping.review_comments)
        self.assertIsNone(new_mapping.map_program_date_time)
        self.assertIsNone(new_mapping.map_program_version)
        self.assertIsNone(new_mapping.map_program_prediction_id)
        self.assertIsNone(new_mapping.map_program_confidence_score)
        self.assertIsNone(new_mapping.deleted_date_time)
        self.assertIsNone(new_mapping.deleted_by)

