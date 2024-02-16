import datetime
import uuid
import json
import logging
from dataclasses import dataclass
from typing import List, Union, Optional
from enum import Enum

from sqlalchemy import text

import app.terminologies.models
from app.database import get_db
from app.errors import BadRequestWithCode, NotFoundException


class RoninCodeSchemas(Enum):
    code = "code"
    codeable_concept = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept"


@dataclass
class FHIRCoding:
    """
    This class represents a Coding in FHIR/RCDM.
    This class should directly hold data; it should NOT access our database
    or have to be aware of our database schema.

    This should only be used within FHIRCodeableConcept and NOT as a standalone.
    """
    code: Optional[str]
    display: Optional[str]
    system: Optional[str]
    version: Optional[str]

    @classmethod
    def deserialize(cls, json_input_raw) -> 'FHIRCoding':
        if type(json_input_raw) == str:
            json_input = json.loads(json_input_raw)
        elif type(json_input_raw) == dict:
            json_input = json_input_raw
        else:
            raise NotImplementedError(f"FHIRCoding.deserialize not implemented for input type: {type(json_input_raw)}")

        instance = cls(
            code=json_input.get('code'),
            display=json_input.get('display'),
            system=json_input.get('system'),
            version=json_input.get('version')
        )

        # See if any unrecognized keys were present in the initial JSON
        expected_keys = {"code", "display", "system", "version", "userSelected"}
        unrecognized_keys = set(json_input.keys()) - expected_keys
        if unrecognized_keys:
            raise ValueError(f"Unrecognized keys in JSON input for FHIRCoding.deserialize: {unrecognized_keys}")

        return instance

    def serialize(self):
        """
        Only return attributes which are not None so that whatever we de-serialized
        gets serialized the same way without adding other attributes.
        """
        serialized = {}
        if self.code:
            serialized["code"] = self.code
        if self.display:
            serialized["display"] = self.display
        if self.system:
            serialized["system"] = self.system
        if self.version:
            serialized["version"] = self.version
        return serialized


@dataclass
class FHIRCodeableConcept:
    """
    This class represents a CodeableConcept in FHIR/RCDM.
    This class should directly hold data; it should NOT access our database
    or have to be aware of our database schema.
    """
    coding: List[FHIRCoding]
    text: str

    @classmethod
    def deserialize(cls, json_input_raw) -> 'FHIRCodeableConcept':
        if type(json_input_raw) == str:
            json_input = json.loads(json_input_raw)
        elif type(json_input_raw) == dict:
            json_input = json_input_raw
        else:
            raise NotImplementedError(f"FHIRCodeableConcept.deserialize not implemented for input type: {type(json_input_raw)}")

        fhir_text = json_input.get('text')
        coding_array = json_input.get('coding')

        instance = cls(
            coding=[FHIRCoding.deserialize(coding) for coding in coding_array],
            text=fhir_text
        )

        # See if any unrecognized keys were present in the initial JSON
        expected_keys = {"coding", "text"}
        unrecognized_keys = set(json_input.keys()) - expected_keys
        if unrecognized_keys:
            raise ValueError(f"Unrecognized keys in JSON input for FHIRCoding.deserialize: {unrecognized_keys}")

        return instance

    def serialize(self):
        """ Serialize to a dict appropriate for json.dumps """
        serialized = {}
        if self.text:
            serialized["text"] = self.text
        if self.coding:
            serialized["coding"] = [coding.serialize() for coding in self.coding]
        return serialized



class Code:
    """
    This class represents a code object used for encoding and maintaining information about a specific
    concept or term within a coding system or terminology. While previously, it was analagous to a FHIR Coding,
    this class is now intended to support both Coding (code, display, system, version) and entire
    CodeableConcepts (or RCDM SourceCodeableConcept). It provides a way to manage various
    attributes related to the code and the coding system it belongs to.

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
        terminology_version: 'app.terminologies.models.Terminology' = None,
        terminology_version_uuid=None,
        depends_on_property: str = None,
        depends_on_system: str = None,
        depends_on_value: str = None,
        depends_on_display: str = None,
        custom_terminology_code_uuid: Optional[uuid.UUID] = None,
        fhir_terminology_code_uuid: Optional[uuid.UUID] = None,
        code_object: FHIRCodeableConcept = None,
        code_schema: RoninCodeSchemas = RoninCodeSchemas.code,
        from_custom_terminology: Optional[bool] = None,
        from_fhir_terminology: Optional[bool] = None,
        saved_to_db: bool = True,
    ):
        """
        Initializes a new instance of the Code class with specified attributes for managing codes
        within a coding system or terminology. This method sets up a flexible structure that can represent
        not just a single coding (with attributes like code, display, system, and version) but also more
        complex structures such as CodeableConcepts. It is capable of handling codes from custom terminologies,
        FHIR terminologies, and standard terminologies.

        ------

        To properly initialize a Code instance, one must provide identification for the coding system:
        - By specifying both `system` and `version` for known coding systems. MUST be registered in public.terminology_versions table.
        - By providing a `terminology_version_uuid` directly if the version's unique identifier is known.
        - By supplying a `terminology_version` object that represents the specific version of the terminology in use.

        This ensures that each code is accurately linked to its terminology version.

        ------

        This constructor enforces the requirement
        that a code_object must be supplied if the code_schema indicates a CodeableConcept. Conversely, if the code_schema
        indicates a simple code, then code and display must be supplied, and code_object must not be supplied or should be None.

        Note that RoninCodeSchema.codeable_concept is only supported for custom terminologies, which must provide a custom_terminology_uuid.

        - If code_schema is RoninCodeSchemas.codeable_concept, a code_object must be provided, and code and display parameters should not be set (or should be None).
        - If code_schema is RoninCodeSchemas.code, code and display parameters must be provided, and code_object must be None.

        This ensures that the Code instance is correctly initialized according to the specified schema, either as a simple code or a more complex CodeableConcept.

        ------

        Parameters:
        - system (str): The identifier of the coding system the code belongs to. This is a FHIR URI, but named systems for backwards compatibility.
        - version (str): The version of the coding system.
        - code (str): The code representing the specific concept or term.
        - display (str): The human-readable display text for the code.
        - additional_data (dict, optional): Any additional data associated with the code.
        - terminology_version ('app.terminologies.models.Terminology', optional): The specific terminology version instance the code belongs to.
        - terminology_version_uuid (str, optional): The unique identifier for the terminology version.
        - depends_on_property (str, optional):
        - depends_on_system (str, optional):
        - depends_on_value (str, optional):
        - depends_on_display (str, optional):
        - custom_terminology_code_uuid (uuid.UUID, optional): The UUID for this code in the custom terminology table.
        - fhir_terminology_code_uuid (uuid.UUID, optional): The UUID for this code in FHIR terminology table.
        - code_object (FHIRCodeableConcept, optional): An instance of FHIR CodeableConcept.
        - code_schema (RoninCodeSchemas, optional): Schema indicating whether this code is a simple code or a codeable concept.
        - from_custom_terminology (bool, optional): Indicates if this code originates from a custom terminology.
        - from_fhir_terminology (bool, optional): Indicates if this code originates from a FHIR terminology.

        Raises:
        - Raises ValueError if the conditions for usage (see above) are not met, ensuring proper usage of the constructor.
        """
        # Validate code_schema against provided parameters
        if code_schema == RoninCodeSchemas.codeable_concept:
            if not code_object or code or display:
                raise ValueError(
                    "For CodeableConcept schema, code_object must be provided, and code and display must be None or not set."
                )
        elif code_schema == RoninCodeSchemas.code:
            if code_object or not code or not display:
                raise ValueError(
                    "For code schema, code and display must be provided, and code_object must be None or not set.")

        self._code = code
        self._display = display

        self.code_object = code_object
        self.code_schema = code_schema

        # `custom_terminology_code_uuid` is a specifically assigned uuid for this code
        # it serves as the primary key in the custom_terminologies.code table
        # Only applies if this Code represents something loaded from the custom_terminologies.code table
        self.custom_terminology_code_uuid = custom_terminology_code_uuid

        # Only applies if loaded from the fhir terminologies system
        self.fhir_terminology_code_uuid = fhir_terminology_code_uuid

        # Set up self.terminology_version
        self.terminology_version = None
        if terminology_version is not None:
            if type(terminology_version) != app.terminologies.models.Terminology:
                raise ValueError(
                    "terminology_version parameter must be instance of app.terminologies.models.Terminology or None"
                )
            self.terminology_version: app.terminologies.models.Terminology = terminology_version

        if terminology_version is None and terminology_version_uuid is None:
            if system is None or version is None:
                raise ValueError(
                    "Either terminology_version, terminology_version_uuid, or system AND version must be provided to instantiate Code"
                )
            else:
                self.terminology_version = app.terminologies.models.Terminology.load_by_fhir_uri_and_version_from_cache(
                    fhir_uri=system,
                    version=version
                )

        if terminology_version_uuid is not None:
            self.terminology_version = app.terminologies.models.Terminology.load_from_cache(terminology_version_uuid)

        if from_custom_terminology is None and from_fhir_terminology is None:
            # If these weren't explicitly provided, we can still look them up
            from_custom_terminology = self.terminology_version.is_custom_terminology
            from_fhir_terminology = self.terminology_version.fhir_terminology

        # Designate as custom terminology code or FHIR terminology code if applicable
        self.from_custom_terminology = from_custom_terminology
        self.from_fhir_terminology = from_fhir_terminology

        # Validate inputs for custom terminology requirements
        if code_schema == RoninCodeSchemas.codeable_concept:
            if self.from_custom_terminology is None or self.from_custom_terminology is False:
                raise ValueError("Codeable concepts are only supported in custom terminologies")
        if from_custom_terminology and custom_terminology_code_uuid is None:
            raise ValueError(
                "If initializing from a custom terminology, the custom_terminology_uuid must be provided")

        # Other set up
        self.additional_data = additional_data

        self.depends_on_property = depends_on_property
        self.depends_on_system = depends_on_system
        self.depends_on_value = depends_on_value
        self.depends_on_display = depends_on_display

        self._saved_to_db = saved_to_db

    @classmethod
    def new_code(
            cls,
            code,
            display,
            system: Optional[str] = None,
            version: Optional[str] = None,
            additional_data=None,
            terminology_version: 'app.terminologies.models.Terminology' = None,
            terminology_version_uuid=None,
            depends_on_property: str = None,
            depends_on_system: str = None,
            depends_on_value: str = None,
            depends_on_display: str = None,
        ):
        """
        Instantiates a new code (not loaded from database).
        Be sure to call save() on it.
        """
        return cls(
            system=system,
            version=version,
            code=code,
            display=display,
            additional_data=additional_data,
            terminology_version=terminology_version,
            terminology_version_uuid=terminology_version_uuid,
            depends_on_property=depends_on_property,
            depends_on_system=depends_on_system,
            depends_on_value=depends_on_value,
            depends_on_display=depends_on_display,
            custom_terminology_code_uuid=uuid.uuid4(),
            code_schema=RoninCodeSchemas.code,
            from_custom_terminology=True,
            from_fhir_terminology=False,
            saved_to_db=False,
        )

    @classmethod
    def new_codeable_concept(
            cls,
            code_object: FHIRCodeableConcept,
            custom_terminology_code_uuid: Optional[uuid.UUID] = None,
            additional_data=None,
            terminology_version: 'app.terminologies.models.Terminology' = None,
            terminology_version_uuid=None,
            system: Optional[str] = None,
            version: Optional[str] = None,
            depends_on_property: str = None,
            depends_on_system: str = None,
            depends_on_value: str = None,
            depends_on_display: str = None,
    ):
        custom_terminology_code_uuid = custom_terminology_code_uuid
        if custom_terminology_code_uuid is None:
            custom_terminology_code_uuid = uuid.uuid4()

        return cls(
                system=system,
                version=version,
                code=None,
                display=None,
                additional_data=additional_data,
                terminology_version=terminology_version,
                terminology_version_uuid=terminology_version_uuid,
                depends_on_property=depends_on_property,
                depends_on_system=depends_on_system,
                depends_on_value=depends_on_value,
                depends_on_display=depends_on_display,
                custom_terminology_code_uuid=custom_terminology_code_uuid,
                fhir_terminology_code_uuid=None,
                code_object=code_object,
                code_schema=RoninCodeSchemas.codeable_concept,
                from_custom_terminology=True,
                from_fhir_terminology=False,
                saved_to_db=False,
        )

    @property
    def uuid(self):
        if self.from_custom_terminology is True:
            return self.custom_terminology_code_uuid
        elif self.from_fhir_terminology is True:
            return self.fhir_terminology_code_uuid
        else:
            raise NotImplementedError(
                "No uuid for Code objects that are not declared as from a custom terminology or fhir terminology"
            )

    @property
    def code(self):
        if self.code_schema == RoninCodeSchemas.codeable_concept:
            return self.code_object
        return self._code

    @property
    def display(self):
        if self.code_schema == RoninCodeSchemas.codeable_concept:
            return self.code_object.text
        return self._display

    @property
    def system(self):
        # For legacy compatibility
        return self.terminology_version.fhir_uri

    @property
    def version(self):
        # For legacy compatibility
        return self.terminology_version.version

    @property
    def terminology_version_uuid(self):
        return self.terminology_version.uuid

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

    def __hash__(self) -> int:  # todo: make it very clear why this is different from code_id and if it should be
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

    # @classmethod
    # def save_many(
    #         cls,
    #         codes: List['app.models.codes.Code'],
    #         on_conflict_do_nothing: bool = False
    # ):
    #     # todo: implement as future optimization
    #     pass

    def generate_deduplicate_hash(self):
        return ""  # todo: obviously, change this for the hash

    def save(self,
             on_conflict_do_nothing: bool = False
             ):
        if self._saved_to_db is True:
            # todo: should this raise an exception or just return?
            raise Exception("Code object is already saved; cannot save again")

        conn = get_db()
        query_text = """
                        INSERT INTO custom_terminologies.code_poc 
                        (
                            uuid, 
                            old_uuid,
                            display, 
                            code_schema,
                            code_simple,
                            code_jsonb,
                            code_id,
                            terminology_version_uuid, 
                            additional_data
                        )
                        VALUES 
                        (
                            :uuid,
                            :old_uuid, 
                            :display, 
                            :code_schema,
                            :code_simple,
                            :code_jsonb,
                            :code_id,
                            :terminology_version_uuid, 
                            :additional_data
                        )
                    """
        if on_conflict_do_nothing:
            query_text += """ on conflict do nothing
                                """
        query_text += """ returning uuid"""

        code_schema_to_save = None
        code_simple = None
        code_jsonb = None

        if self.code_schema == RoninCodeSchemas.code:
            code_schema_to_save = RoninCodeSchemas.code.value
            code_simple = self.code
        elif self.code_schema == RoninCodeSchemas.codeable_concept:
            code_schema_to_save = RoninCodeSchemas.codeable_concept.value
            code_jsonb = json.dumps(self.code.serialize())
        else:
            raise NotImplementedError("Save only implemented for code and codeable concepts")

        try:
            custom_terminology_code_uuid = self.custom_terminology_code_uuid if self.custom_terminology_code_uuid is not None else uuid.uuid4()
            result = conn.execute(
                text(query_text),
                {
                    "uuid": custom_terminology_code_uuid,
                    "old_uuid": custom_terminology_code_uuid,
                    "display": self.display,
                    "code_schema": code_schema_to_save,
                    "code_simple": code_simple,
                    "code_jsonb": code_jsonb,
                    "code_id": self.generate_deduplicate_hash(),
                    "terminology_version_uuid": self.terminology_version_uuid,
                    "additional_data": json.dumps(self.additional_data),
                },
            ).fetchall()
        except Exception as e:
            conn.rollback()
            raise e

        actually_inserted = True if len(result) > 0 else False
        if actually_inserted:
            self._saved_to_db = True

        return actually_inserted

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
                select code.uuid, code.code_schema, code.code_simple, code.code_jsonb, 
                code.display, 
                tv.fhir_uri as system_url, tv.version, tv.terminology as system_name, tv.uuid as terminology_version_uuid
                from custom_terminologies.code_poc as code
                join terminology_versions tv
                on code.terminology_version_uuid = tv.uuid
                where code.old_uuid=:code_uuid
                """  # todo: switch from old_uuid to uuid when appropriate
            ),
            {"code_uuid": code_uuid},
        ).first()

        if code_data is None:
            raise NotFoundException(f"No custom terminology code found with UUID: {code_uuid}")

        code_schema_raw = code_data.code_schema
        code_schema = RoninCodeSchemas(code_schema_raw)
        code = None
        display = None

        code_object = None
        if code_schema == RoninCodeSchemas.codeable_concept:
            code_object = FHIRCodeableConcept.deserialize(code_data.code_jsonb)
        elif code_schema == RoninCodeSchemas.code:
            code = code_data.code_simple
            display = code_data.display

        return cls(
            system=code_data.system_url,
            version=code_data.version,
            code=code,
            display=display,
            terminology_version_uuid=code_data.terminology_version_uuid,
            custom_terminology_code_uuid=code_uuid,
            from_custom_terminology=True,
            code_object=code_object,
            code_schema=code_schema,
            saved_to_db=True
        )

    def serialize(self, with_system_and_version=True):
        # todo: this will need to support multiple types (string, code, CodeableConcept)
        """
        This method serializes the Code instance into a dictionary format, including the system, version, code, and display attributes. It provides options to include or exclude the system and version attributes and to include the system_name attribute.

        Args:
        with_system_and_version (bool, optional): Whether to include the system and version attributes in the serialized output. Defaults to True.

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

            json_list = []
            for x in self.additional_data[key]:
                if x is not None:
                    json_list.append(json.dumps(x))
            deduplicated_list = list(set(json_list))
            unjsoned_list = [json.loads(x) for x in deduplicated_list]
            self.additional_data[key] = unjsoned_list[:5]
