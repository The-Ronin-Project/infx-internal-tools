import datetime
import uuid
import json
from app.database import get_db
from sqlalchemy import text
from app.models.terminologies import (
    Terminology,
    load_terminology_version_with_cache,
    terminology_version_uuid_lookup,
)
from app.errors import BadRequestWithCode
from werkzeug.exceptions import BadRequest

# INTERNAL_TOOLS_BASE_URL = "https://infx-internal.prod.projectronin.io"


class Code:
    """
    This class represents a code object used for encoding and maintaining information about a specific concept or term within a coding system or terminology. It provides a way to manage various attributes related to the code and the coding system it belongs to.

    Attributes:
    system (str): The identifier of the coding system that the code is a part of.
    version (str): The version of the coding system.
    code (str): The code representing the specific concept or term.
    display (str): The human-readable display text for the code.
    additional_data (dict, optional): Any additional data associated with the code.
    uuid (str, optional): The unique identifier for the code.
    system_name (str, optional): The name of the coding system.
    terminology_version (str, optional): The identifier of the specific terminology version the code belongs to.
    terminology_version_uuid (str, optional): The unique identifier for the terminology version.

    Methods:
    init: Initializes a new instance of the Code class and sets its attributes.
    """

    def __init__(
        self,
        system,
        version,
        code,
        display,
        additional_data=None,
        uuid=None,
        system_name=None,
        terminology_version: Terminology = None,
        terminology_version_uuid=None,
    ):
        self.system = system
        self.version = version
        self.code = code
        self.display = display
        self.additional_data = additional_data
        self.uuid = uuid
        self.system_name = system_name
        self.terminology_version: Terminology = terminology_version
        self.terminology_version_uuid: uuid.UUID = terminology_version_uuid

        if (
            self.terminology_version is not None
            and self.system is None
            and self.version is None
        ):
            self.system = self.terminology_version.fhir_uri
            self.version = self.terminology_version.version

        if (
            self.terminology_version_uuid is None
            and self.system is not None
            and self.version is not None
        ):
            self.terminology_version_uuid = terminology_version_uuid_lookup(
                system, version
            )
            self.terminology_version = load_terminology_version_with_cache(
                self.terminology_version_uuid
            )

        if (
            self.terminology_version_uuid is None
            and self.terminology_version is not None
        ):
            self.terminology_version_uuid = self.terminology_version.uuid

    def __repr__(self):
        """
        This method returns a human-readable representation of the Code instance. It overrides the default representation method for the Code class.

        Returns:
        str: A string representation of the Code instance in the format "Code(code, display, system, version)".

        Usage:
        To get the string representation of a Code instance, use the following syntax:
        repr_string = repr(code)
        """
        return f"Code({self.code}, {self.display}, {self.system}, {self.version})"

    def __hash__(self) -> int:
        """
        This method computes a hash value for the Code instance based on its string representation. It overrides the default hash method for the Code class.

        Returns:
        int: A hash value computed from the string representation of the Code instance.

        Usage:
        To compute the hash value of a Code instance, use the following syntax:
        code_hash = hash(code)
        """
        return hash(self.__repr__())

    def __eq__(self, other: object) -> bool:
        """
        This method checks if two Code instances are equal by comparing their code, display, system, and version attributes. It overrides the default equality operator for the Code class.

        Args:
        other (object): The other object to compare with the current instance.

        Returns:
        bool: True if the two Code instances have the same code, display, system, and version attributes, otherwise False.

        Usage:
        To compare two Code instances for equality, use the following syntax:
        are_equal = code1 == code2
        """
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
        """
        This class method is used to load a code object from a custom terminology using its unique identifier (UUID). It retrieves the code data from the database and initializes a new instance of the Code class with the fetched data.

        Args:
        code_uuid (str): The unique identifier (UUID) of the code to be loaded.

        Returns:
        Code: An instance of the Code class, initialized with the data fetched from the database.

        Usage:
        To load a code object from a custom terminology by its UUID, use the following syntax:
        code = Code.load_from_custom_terminology(code_uuid)
        """
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
        """
        This class method is used to load a source concept from a concept map using its unique identifier (UUID). It retrieves the source concept data from the database and initializes a new instance of the Code class with the fetched data.

        Args:
        source_code_uuid (str): The unique identifier (UUID) of the source concept to be loaded.

        Returns:
        Code: An instance of the Code class, initialized with the data fetched from the database.

        Usage:
        To load a source concept from a concept map by its UUID, use the following syntax:
        source_concept = Code.load_concept_map_source_concept(source_code_uuid)
        """
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
            terminology_version_uuid=source_data.terminology_version_uuid,
        )

    def serialize(self, with_system_and_version=True, with_system_name=False):
        """
        This method serializes the Code instance into a dictionary format, including the system, version, code, and display attributes. It provides options to include or exclude the system and version attributes and to include the system_name attribute.

        Args:
        with_system_and_version (bool, optional): Whether to include the system and version attributes in the serialized output. Defaults to True.
        with_system_name (bool, optional): Whether to include the system_name attribute in the serialized output. Defaults to False.

        Returns:
        dict: A dictionary containing the serialized attributes of the Code instance.

        Usage:
        To serialize a Code instance, use the following syntax:
        serialized_code = code.serialize(with_system_and_version=True, with_system_name=False)
        """

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
        This class method adds new codes to a custom terminology. It takes a list of code data dictionaries as input and validates if the terminology can be edited, and if there are no conflicts with existing codes. If all validations pass, the new codes are inserted into the custom terminology, and the newly created code instances are returned.

        Args:
        data (List[dict]): A list of dictionaries containing the data for the new codes to be added. Each dictionary must include the following keys:
        - code (str): The code value.
        - display (str): The human-readable display text for the code.
        - terminology_version_uuid (str): The unique identifier (UUID) for the terminology version the code belongs to.
        - additional_data (dict, optional): Any additional data associated with the code.

        Returns:
        List[Code]: A list of newly created Code instances.

        Raises:
        BadRequestWithCode: If the terminology is a standard or FHIR terminology, has no effective end date, or if the effective end date has passed.
        BadRequestWithCode: If the code-display pair already exists in the custom terminology.

        Usage:
        To add new codes to a custom terminology, use the following syntax:
        new_codes = Code.add_new_code_to_terminology(data)
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
                    select is_standard, fhir_terminology, effective_end from public.terminology_versions
                    where uuid = :terminology_version_uuid
                    """
                ),
                {"terminology_version_uuid": terminology_version_uuid},
            ).first()
            effective_end = terminology_metadata.effective_end
            is_standard_boolean = terminology_metadata.is_standard
            if is_standard_boolean:
                raise BadRequestWithCode(
                    code="TerminologyIsStandard",
                    description="This is a standard terminology and cannot be edited using this method",
                )
            fhir_terminology_boolean = terminology_metadata.fhir_terminology
            if fhir_terminology_boolean:
                raise BadRequestWithCode(
                    code="TerminologyIsFHIR",
                    description="This is a FHIR terminology and cannot be edited using this method",
                )
            if effective_end is None:
                raise BadRequestWithCode(
                    code="Terminology.EffectiveEndNull",
                    description="This terminology does not have an effective end date",
                )
            else:
                if effective_end < datetime.date.today():
                    raise BadRequestWithCode(
                        code="Terminology.EffectiveEndExpired",
                        description="The terminology cannot have more codes added after the effective end has passed. You must first create a new version of the terminology before you can add additional codes to it.",
                    )
                # Trigger creating a new version of terminology API endpoint
                # requests.post(f{INTERNAL_TOOLS_BASE_URL}"/terminology/new_version_from_previous", json={

                # })

        new_uuids = []
        # This will insert the new codes into a custom terminology.
        for x in data:
            new_code_uuid = uuid.uuid4()
            new_uuids.append(new_code_uuid)
            new_additional_data = json.dumps(x["additional_data"])
            result = conn.execute(
                text(
                    """
                    Select count(*) as conflict_count from custom_terminologies.code
                    where code = :code_value
                    and display = :display_value
                    and terminology_version_uuid = :terminology_version_uuid
                    """
                ),
                {
                    "code_value": x["code"],
                    "display_value": x["display"],
                    "terminology_version_uuid": x["terminology_version_uuid"],
                },
            ).one()
            # Check if the code display pair already appears in the custom terminology.
            if result.conflict_count > 0:
                raise BadRequestWithCode(
                    code="CodeDisplayPairDuplicated",
                    description="This code display pair already exists in the terminology.",
                )
            else:
                conn.execute(
                    text(
                        """
                        Insert into custom_terminologies.code(uuid, code, display, terminology_version_uuid, additional_data, depends_on_property, depends_on_system, depends_on_value, depends_on_display)
                        Values (:uuid, :code, :display, :terminology_version_uuid, :additional_data, :depends_on_property, :depends_on_system, :depends_on_value, :depends_on_display)
                        """
                    ),
                    {
                        "uuid": new_code_uuid,
                        "code": x["code"],
                        "display": x["display"],
                        "terminology_version_uuid": x["terminology_version_uuid"],
                        "additional_data": new_additional_data,
                        "depends_on_property": x["depends_on_property"],
                        "depends_on_system": x["depends_on_system"],
                        "depends_on_value": x["depends_on_value"],
                        "depends_on_display": x["depends_on_display"],
                    },
                )
        new_codes = []
        for new_uuid in new_uuids:
            new_code = cls.load_from_custom_terminology(new_uuid)
            new_codes.append(new_code)
        return new_codes
