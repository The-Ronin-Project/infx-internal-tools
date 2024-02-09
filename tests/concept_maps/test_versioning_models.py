import unittest
from app.app import create_app
from app.database import get_db
from app.concept_maps.versioning_models import ConceptMapVersionCreator
from app.concept_maps.models import ConceptMapVersion, ConceptMap


class ConceptMapVersioningModels(unittest.TestCase):
    """

    ```
    concept_map_uuid =
    ```
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

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    def test_new_version_from_previous_same_source_same_target(self):
        """
        Given: The source and target value set versions are the same for both previous version and new version
        When: new_version_from_previous is called
        Then: The previous version of the concept map and the new version of the concept map are identical

        """

        # Create params, get data, etc...
        previous_version_uuid = "24f3917c-94b2-48ab-9936-a63ca24c673a"
        new_source_value_set_version_uuid = "94160026-646c-4c44-9252-3c6c84b133ac"
        new_target_value_set_version_uuid = "1c9455af-8ced-4d26-a223-1df4e0c49a16"
        require_review_for_non_equivalent_relationships = False
        require_review_no_maps_not_in_target = False

        # Create class
        concept_map_version_creator = ConceptMapVersionCreator()
        # Call method and store the new_version_uuid
        new_version_uuid = concept_map_version_creator.new_version_from_previous(
            previous_version_uuid,
            new_source_value_set_version_uuid,
            new_target_value_set_version_uuid,
            require_review_for_non_equivalent_relationships,
            require_review_no_maps_not_in_target,
        )
        # Load the previous ConceptMapVersion and new ConceptMapVersion
        previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        previous_concept_map_version.load_data()
        new_concept_map_version = ConceptMapVersion(new_version_uuid)
        new_concept_map_version.load_data()

        # Get the concept_map_uuid and version numbers for both previous and new versions
        concept_map_uuid = previous_concept_map_version.concept_map.uuid
        previous_version_number = previous_concept_map_version.version
        new_version_number = new_concept_map_version.version

        # Compare the previous version and the new version using diff_mappings_and_metadata
        diff_result = ConceptMap.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version_number,
            new_version_number,
        )

        # Add assertions here to compare the relevant properties of both versions
        # In this case, we expect no added, removed or modified codes since the source and target value set versions are the same
        self.assertEqual(diff_result["added_count"], 0)
        self.assertEqual(diff_result["removed_count"], 0)
        self.assertEqual(diff_result["modified_count"], 0)
        self.assertEqual(diff_result["previous_total"], diff_result["new_total"])

    def test_new_version_from_previous_new_source_same_target(self):
        """
        Given: The source value set for the new concept map version is new and the target value set version is the same.
        When: new_version_from_previous is called
        Then: The new version of the concept map will have unmapped concepts (equal to the number of new concepts in the new source value set, one in this case)

        """

        # Create params, get data, etc...
        previous_version_uuid = "24f3917c-94b2-48ab-9936-a63ca24c673a"
        new_source_value_set_version_uuid = "1c9455af-8ced-4d26-a223-1df4e0c49a16"
        new_target_value_set_version_uuid = "1c9455af-8ced-4d26-a223-1df4e0c49a16"
        require_review_for_non_equivalent_relationships = False
        require_review_no_maps_not_in_target = False

        # Create class
        concept_map_version_creator = ConceptMapVersionCreator()
        # Call method and store the new_version_uuid
        new_version_uuid = concept_map_version_creator.new_version_from_previous(
            previous_version_uuid,
            new_source_value_set_version_uuid,
            new_target_value_set_version_uuid,
            require_review_for_non_equivalent_relationships,
            require_review_no_maps_not_in_target,
        )

        # Load the previous ConceptMapVersion and new ConceptMapVersion
        previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        previous_concept_map_version.load_data()
        new_concept_map_version = ConceptMapVersion(new_version_uuid)
        new_concept_map_version.load_data()

        # Get the concept_map_uuid and version numbers for both previous and new versions
        concept_map_uuid = previous_concept_map_version.concept_map.uuid
        previous_version_number = previous_concept_map_version.version
        new_version_number = new_concept_map_version.version

        # Compare the previous version and the new version using diff_mappings_and_metadata
        diff_result = ConceptMap.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version_number,
            new_version_number,
        )

        # Add assertions here to compare the relevant properties of both versions
        # In this case, we expect an added code, but no removed or modified codes since the source value set version is new (once code more) and the target value set version is the same
        self.assertEqual(diff_result["added_count"], 58)
        self.assertEqual(diff_result["removed_count"], 0)
        self.assertEqual(diff_result["modified_count"], 0)
        self.assertEqual(diff_result["previous_total"] + 1, diff_result["new_total"])

    def test_new_version_from_previous_same_source_new_target(self):
        """
        Given: The source value set version is the same and the target value set version is the new.
        When: new_version_from_previous is called
        Then: The new version of the concept map will have an unmapped concept with previous mapping context (the new target value set version will have one less concept(so it must be used in the previous))

        """

        # Create params, get data, etc...
        previous_version_uuid = "24f3917c-94b2-48ab-9936-a63ca24c673a"
        new_source_value_set_version_uuid = "94160026-646c-4c44-9252-3c6c84b133ac"
        new_target_value_set_version_uuid = "94160026-646c-4c44-9252-3c6c84b133ac"
        require_review_for_non_equivalent_relationships = False
        require_review_no_maps_not_in_target = False

        # Create class
        concept_map_version_creator = ConceptMapVersionCreator()
        # Call method and store the new_version_uuid
        new_version_uuid = concept_map_version_creator.new_version_from_previous(
            previous_version_uuid,
            new_source_value_set_version_uuid,
            new_target_value_set_version_uuid,
            require_review_for_non_equivalent_relationships,
            require_review_no_maps_not_in_target,
        )

        # Load the previous ConceptMapVersion and new ConceptMapVersion
        previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        previous_concept_map_version.load_data()
        new_concept_map_version = ConceptMapVersion(new_version_uuid)
        new_concept_map_version.load_data()

        # Get the concept_map_uuid and version numbers for both previous and new versions
        concept_map_uuid = previous_concept_map_version.concept_map.uuid
        previous_version_number = previous_concept_map_version.version
        new_version_number = new_concept_map_version.version

        # Compare the previous version and the new version using diff_mappings_and_metadata
        diff_result = ConceptMap.diff_mappings_and_metadata(
            concept_map_uuid,
            previous_version_number,
            new_version_number,
        )

        # Add assertions here to compare the relevant properties of both versions
        # In this case, we expect a removed code and a modified count (via previous_mapping_context) but no added codes since the source value set version is the same and the target value set version is the new (one code less)
        self.assertEqual(diff_result["added_count"], 0)
        self.assertEqual(diff_result["removed_count"], 57)
        self.assertEqual(diff_result["modified_count"], 57)
        self.assertEqual(diff_result["previous_total"] - 57, diff_result["new_total"])
