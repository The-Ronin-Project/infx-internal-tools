from webbrowser import get
from sqlalchemy import text
from app.database import get_db
import app.models.codes

class Terminology:
    def __init__(self, uuid, name, version, effective_start, effective_end, fhir_uri, fhir_terminology):
        self.uuid = uuid
        self.name = name
        self.version = version
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.fhir_uri = fhir_uri
        self.fhir_terminology = fhir_terminology
        self.codes = []

    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if isinstance(other, Terminology):
            if other.uuid == self.uuid:
                return True
        return False

    @classmethod
    def load(cls, terminology_version_uuid):
        conn = get_db()
        term_data = conn.execute(
            text(
                """
                select * from terminology_versions
                where uuid=:terminology_version_uuid
                """
            ), {
                'terminology_version_uuid': terminology_version_uuid
            }
        ).first()

        return cls(
            uuid=term_data.uuid, 
            name=term_data.terminology, 
            version=term_data.version, 
            effective_start=term_data.effective_start, 
            effective_end=term_data.effective_end, 
            fhir_uri=term_data.fhir_uri, 
            fhir_terminology=term_data.fhir_terminology
        )

    def load_content(self):
        if self.fhir_terminology is True:
            conn = get_db()
            content_data = conn.execute(
                text(
                    """
                    select * from fhir_defined_terminologies.code_systems_new
                    where terminology_version_uuid =:terminology_version_uuid
                    """
                ), {
                    'terminology_version_uuid': self.uuid
                }
            )
        else:
            raise NotImplementedError('Loading content for non-FHIR terminologies is not supported.')

        for item in content_data:
            self.codes.append(
                app.models.codes.Code(
                    system=self.fhir_uri,
                    version=self.version,
                    code=item.code,
                    display=item.display,
                    uuid=item.uuid,
                    system_name=self.name,
                    terminology_version=self
                )
            )

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
