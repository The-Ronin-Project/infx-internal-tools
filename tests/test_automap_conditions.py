import app.automapping.conditions
import app.models.codes


def test_exact_match():
    input_codeable_concept = app.models.codes.CodeableConcept(
        coding=[
            app.models.codes.Code(
                code="254837009",
                display="Malignant neoplasm of breast (disorder)",
                system="http://snomed.info/sct",
                version=None,
            )
        ],
        text="Malignant neoplasm of breast (disorder)",
    )

    output = app.automapping.conditions.automap_condition(input_codeable_concept)
    print(output)
    assert output.target_code.code == "254837009"
    assert output.target_code.display == "Malignant neoplasm of breast (disorder)"
    assert output.target_code.system == "http://snomed.info/sct"
    assert output.target_code.version == "2023-03-01"

    assert output.reason == "EXACT"


def test_synonym_match():
    input_codeable_concept = app.models.codes.CodeableConcept(
        coding=[
            app.models.codes.Code(
                code="254837009",
                display="Breast cancer",
                system="http://snomed.info/sct",
                version=None,
            )
        ],
        text="Breast cancer",
    )

    output = app.automapping.conditions.automap_condition(input_codeable_concept)
    print(output)
    assert output.target_code.code == "254837009"
    assert output.target_code.display == "Malignant neoplasm of breast (disorder)"
    assert output.target_code.system == "http://snomed.info/sct"
    assert output.target_code.version == "2023-03-01"

    assert output.reason == "SYNONYM"


def test_normalized_match():
    input_codeable_concept = app.models.codes.CodeableConcept(
        coding=[
            app.models.codes.Code(
                code="254837009",
                display="Malignant tumor of breast, NOS",
                system="http://snomed.info/sct",
                version=None,
            )
        ],
        text="Malignant tumor of breast, NOS",
    )

    output = app.automapping.conditions.automap_condition(input_codeable_concept)
    print(output)
    assert output.target_code.code == "254837009"
    assert output.target_code.display == "Malignant neoplasm of breast (disorder)"
    assert output.target_code.system == "http://snomed.info/sct"
    assert output.target_code.version == "2023-03-01"

    assert output.reason == "NORMALIZED DESCRIPTION"


def test_inactive_match():
    input_codeable_concept = app.models.codes.CodeableConcept(
        coding=[
            app.models.codes.Code(
                code="366980001",
                display="Suspected breast cancer (situation)",
                system="http://snomed.info/sct",
                version=None,
            )
        ],
        text="Suspected breast cancer (situation)",
    )

    output = app.automapping.conditions.automap_condition(input_codeable_concept)
    print(output)
    assert output.target_code.code == "134405005"
    assert output.target_code.display == "Suspected breast cancer (situation)"
    assert output.target_code.system == "http://snomed.info/sct"
    assert output.target_code.version == "2023-03-01"

    assert output.reason == "REPLACED_INACTIVE"
