from sqlalchemy import text
from app.database import get_db

class Terminology:
    def __init__(self, uuid, name, version, effective_start, effective_end, fhir_uri, fhir_terminology):
        self.uuid = uuid
        self.name = name
        self.version = version
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.fhir_uri = fhir_uri
        self.fhir_terminology = fhir_terminology

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, Terminology):
            if other.uuid == self.uuid:
                return True
        return False

    @classmethod
    def load_terminologies_for_value_set_version(cls, vs_version_uuid):
        conn = get_db()
        term_data = conn.execute(text(
            """
            select * 
            from terminology_versions
            where uuid in 
            (select terminology_version
            from value_sets.value_set_rule
            where value_set_version=:vs_version)
            """
            ), {
            'vs_version': vs_version_uuid
            }
        )
        terminologies = {x.uuid: Terminology(
            x.uuid,
            x.terminology,
            x.version,
            x.effective_start,
            x.effective_end,
            x.fhir_uri,
            x.fhir_terminology
        ) for x in term_data}

        return terminologies
