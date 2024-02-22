import hashlib
import json
import unittest
from unittest import skip
from unittest.mock import patch, Mock
from uuid import UUID

from pytest import raises
from sqlalchemy import text

import app.value_sets.models
import app.terminologies.models
import app.models.codes
from app.database import get_db
from app.app import create_app
from app.errors import NotFoundException, BadRequestWithCode
from app.value_sets.models import ValueSetVersion, ValueSet
from app.terminologies.models import Terminology
from app.database import get_db

class ValueSetTerminologyTests(unittest.TestCase):
    """
    Safe ValueSet UUIDs, see ValueSetTests class doc
    Safe Terminology UUIDs, see CodeTests class doc,
    """
    def setUp(self) -> None:
        self.conn = get_db()
        self.app = create_app()
        self.app.config.update({
            "TESTING": True,
        })
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.conn.rollback()
        self.conn.close()

    # UUID values: value_sets.value_set
    # version and status may change over time; before using in a test, double-check with a query: see class doc above
    # expected status is active:
    safe_value_set_uuid_obsrv_mirth = "ca75b03c-1763-44fd-9bfa-4fe015ff809c"
    safe_value_set_uuid_nomap_diab = "c7c37780-e727-42f6-9d1b-d823d75171ad"
    safe_value_set_uuid_cond_load = "50ead103-a8c9-4aae-b5f0-f1e51b264323"
    safe_value_set_uuid_obsrv_load = "ccba9765-66ee-4742-a656-4e37d0811958"
    safe_value_set_uuid_code_systems = "477195c0-8a91-11ec-ac15-073d0cb083df"
    # expected status is obsolete:
    safe_value_set_uuid_code_diffs = "b5f97703-abf3-4fc0-aa49-f8851a3fced4"
    safe_value_set_uuid_descrip = "236b88af-40c2-4d59-b319-a5e68865afdc"
    # expected status is in progress:
    safe_value_set_uuid_auto_tool = "fc82ec39-7b9f-4d74-9a34-adf86db1a50f"

    # UUID values: (as needed) value_sets.value_set_version
    safe_value_set_uuid_auto_tool_version = "58e792d9-1264-4f18-b16e-6292cb7ca597"

    # UUID values: (as needed) value_sets.expansion
    safe_value_set_uuid_auto_tool_expansion = "640e5226-79a6-11ee-93aa-b2cb39228ed3"

    # UUID values: public.terminology_versions (documentation see test_codes.py and test_terminologies.py)
    safe_term_uuid_fake = "d2ae0de5-0168-4f54-924a-1f79cf658939"
    safe_term_uuid_old = "3c9ed300-0cb8-47af-8c04-a06352a14b8d"
    safe_term_uuid_dupl = "d14cbd3a-aabe-4b26-b754-5ae2fbd20949"

    def test_perform_terminology_update_missing_input(self):
        """
        ValueSet.perform_terminology_update() is the helper function for perform_terminology_update_for_value_sets().
        Both support the API endpoint POST /TerminologyUpdate/ValueSets/actions/perform_update - these functions
        are noted as needing revision or replacement, but they exist today and offer several cases worth testing.
        """
        value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
        old_terminology_version_uuid = self.safe_term_uuid_old
        new_terminology_version_uuid = self.safe_term_uuid_fake
        with raises(TypeError) as e:
            value_set.perform_terminology_update(old_terminology_version_uuid, new_terminology_version_uuid)
        expected = "perform_terminology_update() missing 3 required positional arguments: 'effective_start', 'effective_end', and 'description'"
        assert expected in str(e.value)

    def test_perform_terminology_update_old_is_null(self):
        value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
        new_terminology_version_uuid = self.safe_term_uuid_fake
        with raises(NotFoundException) as e:
            value_set.perform_terminology_update(
                old_terminology_version_uuid=None,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
        result = e.value
        assert result.message == f"Unable to compare Terminology with UUID: None to Terminology with UUID: {new_terminology_version_uuid}"

    def test_perform_terminology_update_new_is_null(self):
        value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
        old_terminology_version_uuid = self.safe_term_uuid_old
        with raises(NotFoundException) as e:
            value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=None,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
        result = e.value
        assert result.message == f"Unable to compare Terminology with UUID: {old_terminology_version_uuid} to Terminology with UUID: None"

    def test_perform_terminology_update_no_recent_version(self):
        """
        Value set exists but has no versions
        """
        with patch(target='app.value_sets.models.ValueSet.load_version_metadata', return_value=[]):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_descrip)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            with raises(NotFoundException) as e:
                value_set.perform_terminology_update(
                    old_terminology_version_uuid=old_terminology_version_uuid,
                    new_terminology_version_uuid=new_terminology_version_uuid,
                    effective_start="2023-01-01",
                    effective_end="2023-12-31",
                    description="Test ONLY"
                )
            result = e.value
            assert result.message == f"No versions found for Value Set with UUID: {self.safe_value_set_uuid_descrip}"

    def test_perform_terminology_update_already_updated(self):
        """
        Value set does not need an update
        """
        with patch(target='app.value_sets.models.ValueSetVersion.contains_content_from_terminology', return_value=True):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_descrip)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                    old_terminology_version_uuid=old_terminology_version_uuid,
                    new_terminology_version_uuid=new_terminology_version_uuid,
                    effective_start="2023-01-01",
                    effective_end="2023-12-31",
                    description="Test ONLY"
                )
            assert report_text == "already_updated"

    def test_perform_terminology_update_not_active(self):
        """
        Value set needs an update but its most recent version is not active
        """
        with patch(target='app.value_sets.models.ValueSetVersion.contains_content_from_terminology', return_value=False):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_descrip)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "latest_version_not_active"

    def test_perform_terminology_update_fail_create(self):
        """
        An exception blocked creating a new value set
        """
        mock_error = Mock(side_effect=ValueError("Mocked error"))
        with patch('app.value_sets.models.ValueSet.create_new_version_from_previous', mock_error):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "failed_to_create_new: ValueError"

    def test_perform_terminology_update_fail_rules(self):
        """
        An exception blocked updating rules for the terminology
        """
        mock_error = Mock(side_effect=ValueError("Mocked error"))
        with patch('app.value_sets.models.ValueSetVersion.update_rules_for_terminology', mock_error):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "failed_to_update_rules: ValueError"

    def test_perform_terminology_update_fail_diff(self):
        """
        An exception blocked comparing versions for the terminology
        """
        mock_error = Mock(side_effect=ValueError("Mocked error"))
        with patch('app.value_sets.models.ValueSetVersion.diff_for_removed_and_added_codes', mock_error):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "failed_to_diff_versions: ValueError"

    def test_perform_terminology_update_reviewed(self):
        """
        Value set needs an update and its status is reviewed (diff shows 0 new or added codes)
        """
        with patch('app.value_sets.models.ValueSetVersion.contains_content_from_terminology', return_value=False):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "reviewed"

    def test_perform_terminology_update_pending(self):
        """
        Value set needs an update and its status is pending (diff shows >0 new or added codes)
        """
        diff_codes = {
            "removed_codes": [
                {
                    "code": "a",
                    "display": "a",
                    "system": "a",
                }
            ],
            "added_codes": [
                {
                    "code": "b",
                    "display": "b",
                    "system": "b",
                }
            ]
        }
        with patch('app.value_sets.models.ValueSetVersion.diff_for_removed_and_added_codes', return_value=diff_codes):
            value_set = app.value_sets.models.ValueSet.load(self.safe_value_set_uuid_obsrv_mirth)
            old_terminology_version_uuid = self.safe_term_uuid_old
            new_terminology_version_uuid = self.safe_term_uuid_fake
            report_text = value_set.perform_terminology_update(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
                effective_start="2023-01-01",
                effective_end="2023-12-31",
                description="Test ONLY"
            )
            assert report_text == "pending"

    @skip("Performance is too slow for daily use: 270.39s (0:04:30). Test locally only.")
    def test_value_sets_terminology_update_report_happy(self):
        """
        The function being tested, value_sets_terminology_update_report, might be refactored or replaced.
        See the @skip annotation. To run this test, comment out the @skip locally and run it locally.

        Happy path for a terminology update report (this terminology has 4 versions) - no version is excluded
        """
        safe_term_uuid_fhir_uri = "http://projectronin.io/fhir/CodeSystem/mock/condition"
        result = app.value_sets.models.value_sets_terminology_update_report(
                terminology_fhir_uri=safe_term_uuid_fhir_uri,
                exclude_version=None
        )
        assert result == {
            'ready_for_update': [
                {
                    'value_set_uuid': UUID('50ead103-a8c9-4aae-b5f0-f1e51b264323'),
                    'name': 'testconditionincrementalloadsourcevalueset',
                    'title': 'Test ONLY: Test Condition Incremental Load Source Value Set'
                }
            ],
            'latest_version_not_active': [],
            'already_updated': []
        }

    @skip("Performance is too slow for daily use: 251.45s (0:04:11). Test locally only.")
    def test_value_sets_terminology_update_report_exclude_version_4(self):
        """
        The function being tested, value_sets_terminology_update_report, might be refactored or replaced.
        See the @skip annotation. To run this test, comment out the @skip locally and run it locally.

        Happy path while excluding a version (this terminology has 4 versions) - version 4 is excluded
        """
        safe_term_uuid_fhir_uri = "http://projectronin.io/fhir/CodeSystem/mock/condition"
        result = app.value_sets.models.value_sets_terminology_update_report(
            terminology_fhir_uri=safe_term_uuid_fhir_uri,
            exclude_version="4"
        )
        assert result == {
            'ready_for_update': [],
            'latest_version_not_active': [],
            'already_updated': [
                {
                    'value_set_uuid': UUID('50ead103-a8c9-4aae-b5f0-f1e51b264323'),
                    'name': 'testconditionincrementalloadsourcevalueset',
                    'title': 'Test ONLY: Test Condition Incremental Load Source Value Set',
                    'most_recent_version_status': 'active'
                }
            ]
        }

    def test_value_sets_terminology_update_report_null_term(self):
        """
        Input a null terminology_fhir_uri
        """
        with raises(BadRequestWithCode) as e:
            app.value_sets.models.value_sets_terminology_update_report(
                terminology_fhir_uri=None,
                exclude_version=None
            )
        result = e.value
        assert result.http_status_code == 400
        assert result.code == "ValueSet.value_sets_terminology_update_report.no_term_input"
        assert result.description == "No terminology URI was input"

    @skip("Performance is too slow for daily use: 247.32s (0:04:07). Test locally only.")
    def test_value_sets_terminology_update_report_bad_term_uri(self):
        """
        The function being tested, value_sets_terminology_update_report, might be refactored or replaced.
        See the @skip annotation. To run this test, comment out the @skip locally and run it locally.

        Input a non-existent terminology URI, so that includes_terminology is False for all value sets
        """
        bad_term_uuid_fake_fhir_uri = "does_not_exist"
        result = app.value_sets.models.value_sets_terminology_update_report(
                terminology_fhir_uri=bad_term_uuid_fake_fhir_uri,
                exclude_version=None
        )
        assert result == {
            "ready_for_update": [],
            "latest_version_not_active": [],
            "already_updated": [],
        }

    def test_value_sets_terminology_update_report_no_metadata(self):
        """
        Value sets return bad metadata which cannot be processed to make a report
        - mocked to fail every value set, so it fails on the first in UUID order
        """
        with patch(target='app.value_sets.models.ValueSet.load_all_value_set_metadata', return_value=[]):
            safe_term_uuid_fake_fhir_uri = "fake/fhir_uri"
            result = app.value_sets.models.value_sets_terminology_update_report(
                    terminology_fhir_uri=safe_term_uuid_fake_fhir_uri,
                    exclude_version=None
            )
            assert result == {
                "ready_for_update": [],
                "latest_version_not_active": [],
                "already_updated": [],
            }

    @skip("Performance is too slow for daily use: 123.78s (0:02:03). Test locally only.")
    def test_value_sets_terminology_update_report_no_recent_version(self):
        """
        The function being tested, load_version_metadata, might be refactored or replaced.
        See the @skip annotation. To run this test, comment out the @skip locally and run it locally.
        
        Value sets exist but have no versions
        - mocked to fail every value set, so it fails on the first value set UUID discovered, in random order
        """
        with patch(target='app.value_sets.models.ValueSet.load_version_metadata', return_value=[]):
            safe_term_uuid_fake_fhir_uri = "fake/fhir_uri"
            with raises(NotFoundException) as e:
                app.value_sets.models.value_sets_terminology_update_report(
                    terminology_fhir_uri=safe_term_uuid_fake_fhir_uri,
                    exclude_version=None
                )
            result = e.value
            assert "No versions found for Value Set with UUID: " in str(result.message)

    def test_lookup_terminologies_in_value_set_version(self):
        """
        Legacy unit test from the value_sets folder.
        """
        # todo: verify content and update post-migration
        loinc_2_74 = Terminology.load("554805c6-4ad1-4504-b8c7-3bab4e5196fd")  # LOINC 2.74

        value_set_version = ValueSetVersion.load(
            "2441d5b7-9c64-4cac-b274-b70001f05e3f")  # todo: replace w/ dedicated value set for automated tests
        value_set_version.expand()
        terminologies_in_vs = value_set_version.lookup_terminologies_in_value_set_version()

        assert terminologies_in_vs == [loinc_2_74]


if __name__ == '__main__':
    unittest.main()
