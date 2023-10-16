import datetime
import uuid
import json
from app.database import get_db
from sqlalchemy import text
import app.terminologies.models
from typing import List
from uuid import UUID
from app.errors import BadRequestWithCode


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
        terminology_version: app.terminologies.models.Terminology = None,
        terminology_version_uuid=None,
        depends_on_property: str = None,
        depends_on_system: str = None,
        depends_on_value: str = None,
        depends_on_display: str = None,
        custom_terminology_code_uuid: uuid.UUID = None,
    ):
        self.system = system
        self.version = version
        self.code = code
        self.display = display
        self.additional_data = additional_data
        self.uuid = uuid
        self.system_name = system_name
        self.terminology_version: app.terminologies.models.Terminology = (
            terminology_version
        )
        self.terminology_version_uuid: uuid.UUID = terminology_version_uuid
        self.custom_terminology_code_uuid = custom_terminology_code_uuid

        self.depends_on_property = depends_on_property
        self.depends_on_system = depends_on_system
        self.depends_on_value = depends_on_value
        self.depends_on_display = depends_on_display

        if (
            self.terminology_version is not None
            and self.system is None
            and self.version is None
        ):
            self.system = self.terminology_version.fhir_uri
            self.version = self.terminology_version.version
            self.terminology_version_uuid = self.terminology_version.uuid

        if (
            self.terminology_version_uuid is None
            and self.system is not None
            and self.version is not None
        ):
            self.terminology_version_uuid = (
                app.terminologies.models.terminology_version_uuid_lookup(
                    system, version
                )
            )
            if self.terminology_version_uuid is not None:
                self.terminology_version = (
                    app.terminologies.models.load_terminology_version_with_cache(
                        self.terminology_version_uuid
                    )
                )

        if (
            self.terminology_version_uuid is None
            and self.terminology_version is not None
        ):
            self.terminology_version_uuid = self.terminology_version.uuid

    def __repr__(self, include_additional_data=True):
        """
        This method returns a human-readable representation of the Code instance. It overrides the default representation method for the Code class.

        Returns:
        str: A string representation of the Code instance in the format "Code(code, display, system, version, depends_on_property, depends_on_system, depends_on_value, depends_on_display)".

        Usage:
        To get the string representation of a Code instance, use the following syntax:
        repr_string = repr(code)
        """
        repr_string = f"Code({self.code}, {self.display}, {self.system}, {self.version}"
        depends_on_parts = []

        if self.depends_on_property:
            depends_on_parts.append(f"depends_on_property={self.depends_on_property}")
        if self.depends_on_system:
            depends_on_parts.append(f"depends_on_system={self.depends_on_system}")
        if self.depends_on_value:
            depends_on_parts.append(f"depends_on_value={self.depends_on_value}")
        if self.depends_on_display:
            depends_on_parts.append(f"depends_on_display={self.depends_on_display}")

        if depends_on_parts:
            repr_string += ", " + ", ".join(depends_on_parts)

        if include_additional_data:
            repr_string += f", additional_data={self.additional_data}"

        return repr_string + ")"

    def __hash__(self) -> int:
        """
        This method computes a hash value for the Code instance based on its string representation. It overrides the default hash method for the Code class.

        Returns:
        int: A hash value computed from the string representation of the Code instance.

        Usage:
        To compute the hash value of a Code instance, use the following syntax:
        code_hash = hash(code)
        """
        return hash(self.__repr__(include_additional_data=False))

    def __eq__(self, other: object) -> bool:
        """
        This method checks if two Code instances are equal by comparing their code, display, system, version and the 'depends on' attributes. It overrides the default equality operator for the Code class.

        Args:
        other (object): The other object to compare with the current instance.

        Returns:
        bool: True if the two Code instances have the same code, display, system, version and 'depends on' attributes, otherwise False.

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
                and (self.depends_on_property == other.depends_on_property)
                and (self.depends_on_system == other.depends_on_system)
                and (self.depends_on_value == other.depends_on_value)
                and (self.depends_on_display == other.depends_on_display)
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
                select code.uuid, code.code, code.depends_on_value, code.depends_on_display, 
                code.depends_on_property, code.depends_on_system, display, tv.fhir_uri as system_url, 
                tv.version, tv.terminology as system_name, tv.uuid as terminology_version_uuid
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
            depends_on_value=code_data.depends_on_value,
            depends_on_display=code_data.depends_on_display,
            depends_on_property=code_data.depends_on_property,
            depends_on_system=code_data.depends_on_system,
        )

    # todo: likely deprecated, verify and remove
    # @classmethod
    # def load_concept_map_source_concept(cls, source_code_uuid):
    #     """
    #     This class method is used to load a source concept from a concept map using its unique identifier (UUID). It retrieves the source concept data from the database and initializes a new instance of the Code class with the fetched data.
    #
    #     Args:
    #     source_code_uuid (str): The unique identifier (UUID) of the source concept to be loaded.
    #
    #     Returns:
    #     Code: An instance of the Code class, initialized with the data fetched from the database.
    #
    #     Usage:
    #     To load a source concept from a concept map by its UUID, use the following syntax:
    #     source_concept = Code.load_concept_map_source_concept(source_code_uuid)
    #     """
    #     conn = get_db()
    #
    #     source_data = conn.execute(
    #         text(
    #             """
    #             select system as terminology_version_uuid, * from concept_maps.source_concept
    #             where uuid=:source_concept_uuid
    #             """
    #         ),
    #         {"source_concept_uuid": source_code_uuid},
    #     ).first()
    #
    #     return cls(
    #         uuid=source_data.uuid,
    #         system=None,
    #         version=None,
    #         code=source_data.code,
    #         display=source_data.display,
    #         terminology_version_uuid=source_data.terminology_version_uuid,
    #     )

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

    def add_examples_to_additional_data(
        self,
        unit=None,
        value=None,
        reference_range=None,
        value_quantity=None,
        value_boolean=None,
        value_string=None,
        value_date_time=None,
        value_codeable_concept=None,
    ):
        """
        When we're loading data from the error service to load into concept maps, we need to bring along some additional
        pieces of data which are useful context to help the mapper. These include unit, value, value[x], reference_range

        This method will store these in the additional_data
        """
        # todo: we should structure these and store them in their own dataclass, then serialize to additional_data
        if (
            unit is None
            and value is None
            and reference_range is None
            and value_quantity is None
            and value_boolean is None
            and value_string is None
            and value_date_time is None
            and value_codeable_concept is None
        ):
            return

        if self.additional_data is None:
            self.additional_data = {}

        self.add_example_to_additional_data("example_unit", unit)
        self.add_example_to_additional_data("example_value", value)
        self.add_example_to_additional_data("example_reference_range", reference_range)
        self.add_example_to_additional_data("example_value_quantity", value_quantity)
        self.add_example_to_additional_data("example_value_boolean", value_boolean)
        self.add_example_to_additional_data("example_value_string", value_string)
        self.add_example_to_additional_data("example_value_date_time", value_date_time)
        self.add_example_to_additional_data("example_value_codeable_concept", value_codeable_concept)


    def add_example_to_additional_data(self, key: str, example):
        """
        Per-example helper for add_examples_to_additional_data()
        """
        if example is not None:
            if key not in self.additional_data:
                self.additional_data[key] = []
            if isinstance(example, list):
                self.additional_data[key].extend(example)
            else:
                self.additional_data[key].append(example)

            json_list = [json.dumps(x) for x in self.additional_data[key]]
            deduplicated_list = list(set(json_list))
            unjsoned_list = [json.loads(x) for x in deduplicated_list]
            self.additional_data[key] = unjsoned_list[:5]