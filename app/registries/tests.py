from app.registries.models import Registry

LABS_REGISTRY_TEST_LIST = [
    "2680d404-eca1-474c-8212-d8b7e53a4c94",  # test (groups but no members)
    "df7f8893-3672-4b12-b6aa-e3e8e8463870",  # test (has a group with many members)
    # "bb921ad0-0b86-4070-89ea-7b5152d3da70"   # Product: SMT-MVP
]
VITALS_REGISTRY_TEST_LIST = [
    "45248e7a-4c83-4692-9062-df957506f953",  # test
    # "24458a61-db21-42b7-95b1-9d3b166dff2c"  # Product: SMT-MVP
]
DOCUMENTS_REGISTRY_TEST_LIST = [
    "029f929a-3edc-4d50-a3be-f201d1440859",  # test
    # "187c87fa-c3ac-4f22-9718-b728791f0723",  # Product: Timeline
    # "765b49c5-dff6-4fd9-9809-4c03fd9beb3a"   # Product: Slideout
]


def test_all_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    test_labs_registry_publish_csv()
    test_vitals_registry_publish_csv()
    test_documents_registry_publish_csv()


def test_labs_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    """
    # test each registry
    labs_registry_uuid = LABS_REGISTRY_TEST_LIST
    for uuid in labs_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(registry_uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published labs registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,minimumPanelMembers,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the files


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
        output = Registry.export_csv(uuid)

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
        output = Registry.export_csv(uuid)

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
        output = Registry.export_csv(uuid)

        # dev convenience (comment for commit)
        print(f"documents registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1

# def test_get_new_uuid():
#     """
#     For convenience, not a real test at present. Helpful when tidying or populating data in dbAdmin, for tests.
#     """
#     print(uuid.uuid4())
