import app.automapping.conditions
import app.models.codes


def test_exact_match():
    input_codeable_concept = app.models.codes.CodeableConcept(
        coding=[
            app.models.codes.Code(
                code="254837009",
                display="Malignant neoplasm of breast (disorder)",
                system="http://snomed.info/sct",
                version=None
            )
        ],
        text="Malignant neoplasm of breast (disorder)"
    )

    output = app.automapping.conditions.automap_condition(input_codeable_concept)
    assert output.code == "254837009"
    assert output.display == "Malignant neoplasm of breast (disorder)"
    assert output.system == "http://snomed.info/sct"
    assert output.version == "2023-03-01"