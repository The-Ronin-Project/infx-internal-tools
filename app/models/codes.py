from app.database import get_db
from sqlalchemy import text
from app.models.terminologies import Terminology


class Code:
    def __init__(self, system, version, code, display, uuid=None, system_name=None, terminology_version=None):
        self.system = system
        self.version = version
        self.code = code
        self.display = display
        self.uuid = uuid
        self.system_name = system_name
        self.terminology_version = terminology_version

        if self.terminology_version is not None and self.system is None and self.version is None:
            terminology = Terminology.load(self.terminology_version)
            self.system = terminology.fhir_uri
            self.version = terminology.version

    def __repr__(self):
        return f"Code({self.code}, {self.display}, {self.system}, {self.version})"

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Code):
            return (self.code == other.code) and (self.display == other.display) and (self.system == other.system) and (self.version == other.version)
        return False

    @classmethod
    def load_from_custom_terminology(cls, code_uuid):
        conn = get_db()
        code_data = conn.execute(
            text(
                """
                select code.uuid, code.code, display, tv.fhir_uri as system_url, tv.version, tv.terminology as system_name 
                from custom_terminologies.code
                join terminology_versions tv
                on code.terminology_version = tv.uuid
                where code.uuid=:code_uuid
                """
            ),
            {
                'code_uuid': code_uuid
            }
        ).first()

        return cls(
            system = code_data.system_url,
            version = code_data.version,
            code = code_data.code,
            display = code_data.display,
            system_name = code_data.system_name,
            uuid=code_uuid
        )

    @classmethod
    def load_concept_map_source_concept(cls, source_code_uuid):
        conn = get_db()

        source_data = conn.execute(
            text(
                """
                select system as terminology_version_uuid, * from concept_maps.source_concept
                where uuid=:source_concept_uuid
                """
            ), {
                'source_concept_uuid': source_code_uuid
            }
        ).first()

        return cls(
            uuid=source_data.uuid,
            system=None,
            version=None,
            code=source_data.code,
            display=source_data.display,
            terminology_version=source_data.terminology_version_uuid
        )
    def serialize(self, with_system_and_version=True, with_system_name=False):
        serialized = {
            "system": self.system,
            "version": self.version,
            "code": self.code,
            "display": self.display
        }

        if with_system_and_version is False:
            serialized.pop('system')
            serialized.pop('version')

        if self.system_name is not None and with_system_name is True:
            serialized['system_name'] = self.system_name

        return serialized