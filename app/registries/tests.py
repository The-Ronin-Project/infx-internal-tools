from app.database import get_db
from app.registries.models import Registry


def test_labs_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    conn = get_db()
    labs_registry_uuid = [
        "df7f8893-3672-4b12-b6aa-e3e8e8463870",  # test
        "6acfc51a-b69a-4409-b0c6-354ea4f5b14e",  # test
        "2680d404-eca1-474c-8212-d8b7e53a4c94",
        "bb921ad0-0b86-4070-89ea-7b5152d3da70"
    ]

    # test each registry
    for uuid in labs_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published labs registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,minimumPanelMembers,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the file

        conn.rollback()
        conn.close()


def test_vitals_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # look in OCI and delete the files by uuid (or just let them be overwritten)

    # start the test
    conn = get_db()
    vitals_registry_uuid = [
        "6b071576-2063-484f-9221-43ca79613b91",  # test
        "5fe807d6-e248-4f21-8cb0-e6ee39c42423",  # test
        "24458a61-db21-42b7-95b1-9d3b166dff2c"
    ]

    # test each registry
    for uuid in vitals_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published vitals registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,ucumRefUnits,refRangeLow,refRangeHigh,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion"
        )
        assert output.count("\n") >= 1
        # look in OCI to see the file

        conn.rollback()
        conn.close()

def test_documents_registry_publish_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    # look in OCI and delete the files by uuid (or just let them be overwritten)

    # start the test
    conn = get_db()
    documents_registry_uuid = [
        "0dc18f3d-520b-41a6-b75a-548ca3b4d08a",  # test
        "550a2a6d-5bde-4ee4-b7a9-247d6b5165d7",  # test
        "187c87fa-c3ac-4f22-9718-b728791f0723",
        "765b49c5-dff6-4fd9-9809-4c03fd9beb3a",
        "abe979d1-443d-4d2c-bb66-c6cd6e760947"
    ]

    # test each registry
    for uuid in documents_registry_uuid:
        # publish
        output = Registry.publish_to_object_store(uuid=uuid, environment="dev")

        # dev convenience (comment for commit)
        print(f"published documents registry {uuid}")
        print(output)

        # assertions
        assert output.startswith(
            "productGroupLabel,productItemLabel,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion")
        assert output.count("\n") >= 1
        # look in OCI to see the file

    conn.rollback()
    conn.close()

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
    conn = get_db()
    labs_registry_uuid = [
        "df7f8893-3672-4b12-b6aa-e3e8e8463870",  # test
        "6acfc51a-b69a-4409-b0c6-354ea4f5b14e",  # test
        "2680d404-eca1-474c-8212-d8b7e53a4c94",
        "bb921ad0-0b86-4070-89ea-7b5152d3da70"
    ]

    # test each registry
    for uuid in labs_registry_uuid:
        # export
        output = Registry.export_csv(uuid)

        # dev convenience (comment for commit)
        print(f"labs registry {uuid} csv:")
        print(output)

        # assertions
        assert output.startswith("productGroupLabel,productItemLabel,minimumPanelMembers,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion")
        assert output.count("\n") >= 1

    conn.rollback()
    conn.close()

def test_vitals_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    conn = get_db()
    vitals_registry_uuid = [
        "6b071576-2063-484f-9221-43ca79613b91",  # test
        "5fe807d6-e248-4f21-8cb0-e6ee39c42423",  # test
        "24458a61-db21-42b7-95b1-9d3b166dff2c"
    ]

    # test each registry
    for uuid in vitals_registry_uuid:
        # export
        output = Registry.export_csv(uuid)

        # dev convenience (comment for commit)
        print(f"vitals registry {uuid} csv:")
        print(output)

        # assertions
        assert output.startswith("productGroupLabel,productItemLabel,ucumRefUnits,refRangeLow,refRangeHigh,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion")
        assert output.count("\n") >= 1

    conn.rollback()
    conn.close()

def test_documents_registry_export_csv():
    """
    Rough test for developers, not a unit test or codecov test (yet).
    Reuse existing test registries, since we do not want to expose a Registry.delete() for easy cleanup.
    """
    conn = get_db()
    documents_registry_uuid = [
        "0dc18f3d-520b-41a6-b75a-548ca3b4d08a",  # test
        "550a2a6d-5bde-4ee4-b7a9-247d6b5165d7",  # test
        "187c87fa-c3ac-4f22-9718-b728791f0723",
        "765b49c5-dff6-4fd9-9809-4c03fd9beb3a",
        "abe979d1-443d-4d2c-bb66-c6cd6e760947"
    ]

    # test each registry
    for uuid in documents_registry_uuid:
        # export
        output = Registry.export_csv(uuid)

        # dev convenience (comment for commit)
        print(f"documents registry {uuid}")
        print(output)

        # assertions
        assert output.startswith("productGroupLabel,productItemLabel,sequence,valueSetUuid,valueSetDisplayTitle,valueSetCodeName,valueSetVersion")
        assert output.count("\n") >= 1

    conn.rollback()
    conn.close()
