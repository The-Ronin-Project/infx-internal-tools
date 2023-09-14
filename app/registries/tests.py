import json
import uuid

import pytest

from app.registries.models import Registry

LABS_REGISTRY_TEST_LIST = [
    # "2680d404-eca1-474c-8212-d8b7e53a4c94",  # test (groups but no members)
    "df7f8893-3672-4b12-b6aa-e3e8e8463870",  # test (has a group with many members)
    # "bb921ad0-0b86-4070-89ea-7b5152d3da70"   # Product: SMT-MVP
]
VITALS_REGISTRY_TEST_LIST = [
    # "45248e7a-4c83-4692-9062-df957506f953",  # test
    # "24458a61-db21-42b7-95b1-9d3b166dff2c"  # Product: SMT-MVP
]
DOCUMENTS_REGISTRY_TEST_LIST = [
    # "029f929a-3edc-4d50-a3be-f201d1440859",  # test
    # "187c87fa-c3ac-4f22-9718-b728791f0723",  # Product: Timeline
    # "765b49c5-dff6-4fd9-9809-4c03fd9beb3a"   # Product: Slideout
]


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_all_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    test_labs_registry_publish_csv()
    test_vitals_registry_publish_csv()
    test_documents_registry_publish_csv()


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_labs_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # test each registry
    labs_registry_uuid = LABS_REGISTRY_TEST_LIST
    for uuid in labs_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(registry_uuid=uuid, environment="dev,stage,prod")

        # dev convenience (comment for commit)
        print(f"published labs registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,minimumPanelMembers,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the files


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_vitals_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # test each registry
    vitals_registry_uuid = VITALS_REGISTRY_TEST_LIST
    for uuid in vitals_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(registry_uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published vitals registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,ucumRefUnits,refRangeLow,refRangeHigh,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the file


@pytest.mark.skip(reason="this writes to OCI product folders so we must mock OCI instead")
def test_documents_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # test each registry
    documents_registry_uuid = DOCUMENTS_REGISTRY_TEST_LIST
    for uuid in documents_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(registry_uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published documents registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the file


def test_all_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    test_labs_registry_export_csv()
    test_vitals_registry_export_csv()
    test_documents_registry_export_csv()


def test_labs_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    labs_registry_uuid = LABS_REGISTRY_TEST_LIST
    for uuid in labs_registry_uuid:
        # export
        output = Registry.export(uuid)

        # dev convenience (comment for commit)
        print(f"labs registry {uuid} csv:")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,minimumPanelMembers,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1


def test_vitals_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    vitals_registry_uuid = VITALS_REGISTRY_TEST_LIST
    for uuid in vitals_registry_uuid:
        # export
        output = Registry.export(uuid)

        # dev convenience (comment for commit)
        print(f"vitals registry {uuid} csv:")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,ucumRefUnits,refRangeLow,refRangeHigh,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion")
        assert output.count("\n") >= 1


def test_documents_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    documents_registry_uuid = DOCUMENTS_REGISTRY_TEST_LIST
    for uuid in documents_registry_uuid:
        # export
        output = Registry.export(uuid)

        # dev convenience (comment for commit)
        print(f"documents registry {uuid} csv:")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1


def test_all_registry_export_json():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    test_labs_registry_export_json()
    test_vitals_registry_export_json()
    test_documents_registry_export_json()


def test_labs_registry_export_json():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    labs_registry_uuid = LABS_REGISTRY_TEST_LIST
    for uuid in labs_registry_uuid:
        # export
        output = Registry.export(uuid, "pending", "json")

        # dev convenience (comment for commit)
        print(f"labs registry {uuid} json:")
        print(output)

        # assertions
        row_count = len(output)
        output_string = json.dumps(output)
        assert output_string.count("productGroupLabel") == row_count
        assert output_string.count("productItemLabel") == row_count
        assert output_string.count("minimumPanelMembers") == row_count
        assert output_string.count("sequence") >= row_count  # this word could be in data
        assert output_string.count("valueSetUuid") == row_count
        assert output_string.count("valueSetDisplayTitle") == row_count
        assert output_string.count("valueSetCodeName") == row_count
        assert output_string.count("valueSetVersion") == row_count


def test_vitals_registry_export_json():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    vitals_registry_uuid = VITALS_REGISTRY_TEST_LIST
    for uuid in vitals_registry_uuid:
        # export
        output = Registry.export(uuid, "pending", "json")

        # dev convenience (comment for commit)
        print(f"vitals registry {uuid} json:")
        print(output)

        # assertions
        row_count = len(output)
        output_string = json.dumps(output)
        assert output_string.count("productGroupLabel") == row_count
        assert output_string.count("productItemLabel") == row_count
        assert output_string.count("ucumRefUnits") == row_count
        assert output_string.count("refRangeLow") == row_count
        assert output_string.count("refRangeHigh") == row_count
        assert output_string.count("sequence") >= row_count  # this word could be in data
        assert output_string.count("valueSetUuid") == row_count
        assert output_string.count("valueSetDisplayTitle") == row_count
        assert output_string.count("valueSetCodeName") == row_count
        assert output_string.count("valueSetVersion") == row_count


def test_documents_registry_export_json():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # test each registry
    documents_registry_uuid = DOCUMENTS_REGISTRY_TEST_LIST
    for uuid in documents_registry_uuid:
        # export
        output = Registry.export(uuid, "pending", "json")

        # dev convenience (comment for commit)
        print(f"documents registry {uuid} json:")
        print(output)

        # assertions
        row_count = len(output)
        output_string = json.dumps(output)
        assert output_string.count("productGroupLabel") == row_count
        assert output_string.count("productItemLabel") == row_count
        assert output_string.count("sequence") >= row_count  # this word could be in data
        assert output_string.count("valueSetUuid") == row_count
        assert output_string.count("valueSetDisplayTitle") == row_count
        assert output_string.count("valueSetCodeName") == row_count
        assert output_string.count("valueSetVersion") == row_count


@pytest.mark.skip(reason="Not a test. For local dev only. Helpful when tidying or populating data in dbAdmin.")
def test_get_new_uuid():
    print(uuid.uuid4())
