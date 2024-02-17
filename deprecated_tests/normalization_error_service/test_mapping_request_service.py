import unittest
import uuid

import app.helpers.format_helper
from app.database import get_db

import app.util.mapping_request_service
import app.models.models


class NormalizationErrorServiceIntegrationTests(unittest.TestCase):
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

        concept_map = app.util.mapping_request_service.lookup_concept_map_version_for_data_element(
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

        concept_map = app.util.mapping_request_service.lookup_concept_map_version_for_data_element(
            data_element="Observation.code",
            organization=psj_prod_organization,
        )
        print(concept_map)
        assert 1 == 0


class NormalizationErrorServiceUnitTests(unittest.TestCase):
    def test_filter_issues_by_type(self):
        cerner_sandbox_organization = app.models.models.Organization(id="ejh3j95h")

        resource = app.util.mapping_request_service.ErrorServiceResource(
            id=uuid.UUID("22139011-0c6f-4f3e-8e5c-164a2830b367"),
            organization=cerner_sandbox_organization,
            resource_type=app.util.mapping_request_service.ResourceType.CONDITION,
            resource="N/A",
            status="REPORTED",
            severity="FAILED",
            create_dt_tm=app.helpers.format_helper.convert_string_to_datetime_or_none("2023-03-16T21:45:10.173667Z"),
            update_dt_tm=None,
            reprocess_dt_tm=None,
            reprocessed_by=None,
            token=None
        )

        issue_1 = app.util.mapping_request_service.ErrorServiceIssue(
            id=uuid.UUID("9e15647a-7a5c-4cb0-989d-e2b3809ca3f0"),
            severity="FAILED",
            type="NOV_CONMAP_LOOKUP",
            description="Sample description",
            status="REPORTED",
            create_dt_tm=app.helpers.format_helper.convert_string_to_datetime_or_none(
                "2023-03-16T21:45:10.316956Z"
            ),
            location="Condition.code",
            update_dt_tm=None,
            metadata=None
        )
        resource.issues.append(issue_1)

        issue_2 = app.util.mapping_request_service.ErrorServiceIssue(
            id=uuid.UUID("a8469e52-a0e9-4864-b2e8-c5b0caecbc10"),
            severity="FAILED",
            type="SAMPLE_ERROR_TYPE",
            description="Sample description",
            status="REPORTED",
            create_dt_tm=app.helpers.format_helper.convert_string_to_datetime_or_none(
                "2023-07-18T21:45:10.316956Z"
            ),
            location="Condition.code",
            update_dt_tm=None,
            metadata=None
        )
        resource.issues.append(issue_2)

        filtered_issues = resource.filter_issues_by_type("NOV_CONMAP_LOOKUP")
        self.assertEqual(1, len(filtered_issues))


