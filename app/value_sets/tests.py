from app.value_sets.models import ValueSetVersion
from app.terminologies.models import Terminology
from app.database import get_db


def test_lookup_terminologies_in_value_set_version():
    conn = get_db()

    loinc_2_74 = Terminology.load('554805c6-4ad1-4504-b8c7-3bab4e5196fd')  # LOINC 2.74

    value_set_version = ValueSetVersion.load('2441d5b7-9c64-4cac-b274-b70001f05e3f') #todo: replace w/ dedicated value set for automated tests
    value_set_version.expand()
    terminologies_in_vs = value_set_version.lookup_terminologies_in_value_set_version()

    assert terminologies_in_vs == [loinc_2_74]

    conn.rollback()
    conn.close()
