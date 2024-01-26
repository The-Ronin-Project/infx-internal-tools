import datetime
import uuid
import unittest

import app.models.mapping_request_service


class MappingRequestServiceTests(unittest.TestCase):
    def test_extract_issue_location(self):
        mapping_request_service = app.models.mapping_request_service.MappingRequestService()

        sample_issue_2 = app.models.mapping_request_service.ErrorServiceIssue(
            create_dt_tm=datetime.datetime.now(),
            description="Tenant source value '260385009' has no target defined in any Observation.valueCodeableConcept concept map for tenant 'mdaoc'",
            id=uuid.UUID('ea088929-a8ba-472c-af6b-e6df5cd0c248'),
            location='Observation.component[0].valueCodeableConcept',
            severity='FAILED',
            status='REPORTED',
            type='NOV_CONMAP_LOOKUP',
            update_dt_tm=None,
            metadata=None
        )

        location, element, index = mapping_request_service.extract_issue_location(sample_issue_2)

        self.assertEqual(location, 'Observation.component[0].valueCodeableConcept')
        self.assertEqual(element, 'Observation.component.valueCodeableConcept')
        self.assertEqual(index, 0)