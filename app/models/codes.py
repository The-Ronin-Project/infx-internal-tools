import datetime
import uuid
import json
from app.database import get_db
from sqlalchemy import text
from app.models.terminologies import Terminology
from werkzeug.exceptions import BadRequest


class Code:
    def __init__(
        self,
        system,
        version,
        code,
        display,
        additional_data=None,
        uuid=None,
        system_name=None,
        terminology_version=None,
        terminology_version_uuid=None,
    ):
        self.system = system
        self.version = version
        self.code = code
        self.display = display
        self.additional_data = additional_data
        self.uuid = uuid
        self.system_name = system_name
        self.terminology_version = terminology_version
        self.terminology_version_uuid = terminology_version_uuid

        if (
            self.terminology_version is not None
            and self.system is None
            and self.version is None
        ):
            terminology = Terminology.load(self.terminology_version)
            self.system = terminology.fhir_uri
            self.version = terminology.version

    def __repr__(self):
        return f"Code({self.code}, {self.display}, {self.system}, {self.version})"

    def __hash__(self) -> int:
        return hash(self.__repr__())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Code):
            return (
                (self.code == other.code)
                and (self.display == other.display)
                and (self.system == other.system)
                and (self.version == other.version)
            )
        return False

    @classmethod
    def load_from_custom_terminology(cls, code_uuid):
        conn = get_db()
        code_data = conn.execute(
            text(
                """
                select code.uuid, code.code, display, tv.fhir_uri as system_url, tv.version, tv.terminology as system_name, tv.uuid as terminology_version_uuid
                from custom_terminologies.code
                join terminology_versions tv
                on code.terminology_version_uuid = tv.uuid
                where code.uuid=:code_uuid
                """
            ),
            {"code_uuid": code_uuid},
        ).first()

        return cls(
            system=code_data.system_url,
            version=code_data.version,
            code=code_data.code,
            display=code_data.display,
            system_name=code_data.system_name,
            terminology_version_uuid=code_data.terminology_version_uuid,
            uuid=code_uuid,
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
            ),
            {"source_concept_uuid": source_code_uuid},
        ).first()

        return cls(
            uuid=source_data.uuid,
            system=None,
            version=None,
            code=source_data.code,
            display=source_data.display,
            terminology_version=source_data.terminology_version_uuid,
        )

    def serialize(self, with_system_and_version=True, with_system_name=False):

        serialized = {
            "system": self.system,
            "version": self.version,
            "code": self.code,
            "display": self.display,
        }

        if with_system_and_version is False:
            serialized.pop("system")
            serialized.pop("version")

        if self.system_name is not None and with_system_name is True:
            serialized["system_name"] = self.system_name

        return serialized

    @classmethod
    def add_new_code_to_terminology(cls, data):
        """
        This method will insert new codes into a custom terminology. The input is a list of codes to be created.
        """
        conn = get_db()
        terminology_version_uuid_list = set(
            [x.get("terminology_version_uuid") for x in data]
        )
        # This will validate the terminology version is still within its effective date range.
        for terminology_version_uuid in terminology_version_uuid_list:
            terminology_metadata = conn.execute(
                text(
                    """
                    select effective_end from public.terminology_versions
                    where uuid = :terminology_version_uuid
                    """
                ),
                {"terminology_version_uuid": terminology_version_uuid},
            ).first()
            effective_end = terminology_metadata.effective_end
            if effective_end < datetime.date.today():
                raise BadRequest(
                    f"The terminology effective end date for {terminology_version_uuid} has passed, a new terminology version must be created."
                )
        new_uuids = []

        # This will insert the new codes into a custom terminology.
        for x in data:
            new_code_uuid = uuid.uuid4()
            new_uuids.append(new_code_uuid)
            new_additional_data = x.get('additional_data')
            if new_additional_data:
                new_additional_data = json.dumps(new_additional_data)
            conn.execute(
                text(
                    """
                    Insert into custom_terminologies.code(uuid, code, display, terminology_version_uuid, additional_data)
                    Values (:uuid, :code, :display, :terminology_version_uuid, :additional_data)
                    """
                ),
                {
                    "uuid": new_code_uuid,
                    "code": x["code"],
                    "display": x["display"],
                    "terminology_version_uuid": x["terminology_version_uuid"],
                    "additional_data": new_additional_data,
                },
            )
        new_codes = []
        for new_uuid in new_uuids:
            new_code = cls.load_from_custom_terminology(new_uuid)
            new_codes.append(new_code)
        return new_codes
