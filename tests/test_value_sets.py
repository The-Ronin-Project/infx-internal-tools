import app.value_sets.models
import app.terminologies.models
import app.models.codes


def test_value_set_expand():
    """
    Expand the 'Automated Testing Value Set' value set and verify the outputs
    """
    value_set_version_uuid = '58e792d9-1264-4f18-b16e-6292cb7ca597'
    value_set_version = app.value_sets.models.ValueSetVersion.load(value_set_version_uuid)
    value_set_version.expand(force_new=True)

    assert len(value_set_version.expansion) == 91


def test_loinc_rule():
    terminology_version = app.terminologies.models.Terminology.load('554805c6-4ad1-4504-b8c7-3bab4e5196fd')
    rule = app.value_sets.models.LOINCRule(
        uuid=None,
        position=None,
        description=None,
        prop='component',
        operator='=',
        value='{"Complete blood count W Auto Differential panel"}',
        include=True,
        value_set_version=None,
        fhir_system='http://loinc.org',
        terminology_version=terminology_version,
    )
    rule.execute()

    assert len(rule.results) == 1

    first_item = list(rule.results)[0]

    assert first_item.code == '57021-8'
    assert first_item.display == 'CBC W Auto Differential panel - Blood'
    assert first_item.system == 'http://loinc.org'
    assert first_item.version == '2.74'
