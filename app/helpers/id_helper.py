from typing import Optional

import app.helpers.data_helper
import app.helpers.format_helper
import app.helpers.message_helper


def generate_code_id(
    code_string: str,
    display_string: Optional[str] = None,
    # todo: align with changes: now> 4 str, soon> 1 str: correctly ordered, serialized, joined DependsOnData list items
    # todo: during v4>v5 depends_on_value_string may contain the ordered, serialized, joined DependsOnData list data
    depends_on_value_string: Optional[str] = None,
    depends_on_property: Optional[str] = None,
    depends_on_system: Optional[str] = None,
    depends_on_display: Optional[str] = None
) -> str:
    """
    The standard code_id creation function. Offered as the backend function for a Mapping Service API endpoint.

    Allows JSON string input, but loads and re-serializes the JSON, using standard INFX Systems functions to normalize
    the JSON to the RCDM model, apply consistent JSON string format, force consistent JSON key and list element order on
    the JSON string, then inputting the result to standard INFX Systems functions to hash the value.

    All parameters except code_string are optional.
    @param code_string is required. It may be a serialized JSON string for a FHIR resource like CodeableConcept, or a
        string value, fpr a FHIR code. It may be any FHIR data type that RCDM allows for a source code in a ConceptMap.
    @param display_string is expected when the code is simple string and not a CodeableConcept, but it is not required.
    @param depends_on_value_string is an optional argument, but is REQUIRED if the code has associated dependsOn data.
        It may be a serialized JSON string for a FHIR resource, or a string value, for a FHIR string.
        It may be the serialized value of any FHIR data type that RCDM allows for a depends_on value in a ConceptMap.
        v4: It contains the DependsOnData.value or (for v4 > v5) MAY contain a join of all 4 DependsOnData properties.
            v4 data contains depends_on_value and depends_on_property only, no depends_on_system or depends_on_display.
        v5: It MUST contain ALL the DependsOnData list items correctly ordered, serialized, and joined.
            prepare_depends_on_for_storage() returns this value.
    @param depends_on_property (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_system (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_display (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @raise BadDataError if the code_string is serialized JSON that is not an RCDM supported object type for codes.
        ValueError if the input code_string is empty or None.
    @return the unique code_id
    """
    return hash_for_code_id(
        prepare_code_string_for_code_id(code_string),
        display_string,
        # todo: align: now> 4 str, soon> 1 str: correctly ordered, serialized, joined DependsOnData list items
        depends_on_value_string,
        depends_on_property,
        depends_on_system,
        depends_on_display
    )


def generate_mapping_id_with_source_code_id(
        source_code_id: str,
        relationship_code: str,
        target_concept_code: str,
        target_concept_display: Optional[str] = None,
        target_concept_system: Optional[str] = None,
) -> str:
    """
    One of 2 standard mapping_id creation functions offered as backend functions for Mapping Service API endpoints.
    The other is generate_mapping_id_with_source_code_values().

    Concatenates the inputs in a specific order. Returns
    a hash of the concatenated string. Does not modify input strings in ANY way before concatenating and hashing.

    @param source_code_id MUST be a correctly generated, accurate, unique code_id value. Other params are strings.
    @param relationship_code: Required source-to-target relationship code
    @param target_concept_code: Required target code
    @param target_concept_display: Optional target display
    @param target_concept_system: Optional target system
    @raise ValueError if any of source_code_id, relationship_code, and/or target_concept_code are empty or None.
    @return: the unique mapping_id
    """
    return hash_for_mapping_id(
        source_code_id,
        relationship_code,
        target_concept_code,
        target_concept_display,
        target_concept_system
    )


def generate_mapping_id_with_source_code_values(
    source_code_string: str,
    display_string: Optional[str],
    relationship_code: str,
    target_concept_code: str,
    target_concept_display: Optional[str] = None,
    target_concept_system: Optional[str] = None,
    # todo: align with changes: now> 4 str, soon> 1 str: correctly ordered, serialized, joined DependsOnData list items
    depends_on_value_string: Optional[str] = None,
    depends_on_property: Optional[str] = None,
    depends_on_system: Optional[str] = None,
    depends_on_display: Optional[str] = None,
) -> str:
    """
    One of 2 standard mapping_id creation functions offered as backend functions for Mapping Service API endpoints.
    The alternative is generate_mapping_id_with_source_code_id().

    Concatenates the inputs in a specific order. Returns
    a hash of the concatenated string. Does not modify input strings in ANY way before concatenating and hashing.

    All parameters except code_string are optional.
    @param source_code_string is required. It may be a serialized JSON string for a FHIR resource like CodeableConcept,
        or a string value, for a FHIR code. It may be any data type that RCDM allows for a source code in a ConceptMap.
    @param display_string is expected when the code is simple string and not a CodeableConcept, but it is not required.
    @param relationship_code: Required source-to-target relationship code
    @param target_concept_code: Required target code
    @param target_concept_display: Optional target display
    @param target_concept_system: Optional target system
    @param depends_on_value_string is an optional argument, but is REQUIRED if the code has associated dependsOn data.
        It may be a serialized JSON string for a FHIR resource, or a string value, for a FHIR string.
        It may be the serialized value of any FHIR data type that RCDM allows for a depends_on value in a ConceptMap.
        v4: It contains the DependsOnData.value or (for v4 > v5) MAY contain a join of all 4 DependsOnData properties.
            v4 data contains depends_on_value and depends_on_property only, no depends_on_system or depends_on_display.
        v5: It MUST contain ALL the DependsOnData list items correctly ordered, serialized, and joined.
            prepare_depends_on_for_storage() returns this value.
    @param depends_on_property (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_system (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_display (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @raise ValueError if any of source_code_string, relationship_code, and/or target_concept_code are empty or None.
    @return: the unique mapping_id
    """
    return generate_mapping_id_with_source_code_id(
        generate_code_id(
            source_code_string,
            display_string,
            # todo: align: now> 4 str, soon> 1 str: correctly ordered, serialized, joined DependsOnData list items
            depends_on_value_string,
            depends_on_property,
            depends_on_system,
            depends_on_display
        ),
        relationship_code,
        target_concept_code,
        target_concept_display,
        target_concept_system
    )


def prepare_code_string_for_code_id(code_string: str) -> str:
    """
    Internal INFX Systems use only. NOT intended to be exposed via any external API such as the Mapping Service.
    NOTE: This is NOT the standard code_id creation function. This function is a helper to that function.

    Returns a INFX Systems serialized JSON string for the input, if it is serialized JSON for a supported object type.
    If the input code_string is a numeric code value, converts it to a string containing digit characters.
    
    @param code_string - a simple string, or serialized JSON using any valid syntax for JSON and keys/lists in any order
    @return - either the input code_string (if it was a simple string), or the serialized JSON from the code_string but:
        normalized to RCDM (sorts list order, removes unwanted attributes), and re-serialized (sorts keys, strips space)
    @raise BadDataError if the code_string is serialized JSON that is not an RCDM supported object type for codes.
    @raise ValueError if code_string is "" (empty) or None
    """
    if code_string is None or code_string == "":
        raise ValueError("Cannot use an empty code_string for a code_id")
    try:
        # an all-digits simple code value may be read as an int
        if code_string.isnumeric():
           return f"{code_string}"

        # an object value of a known schema, such as FHIR CodeableConcept
        json_object = app.helpers.data_helper.load_json_string(code_string)

        # CodeableConcept
        try:
            rcdm_object = app.helpers.data_helper.normalized_source_codeable_concept(json_object)
            rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
            return rcdm_string
        except Exception as e:
            raise e

    except Exception as e:
        name = app.helpers.message_helper.message_exception_classname(e)
        if name == "JSONDecodeError":
            # a string value, such as FHIR code
            return code_string
        else:
            raise e


def hash_for_code_id(
    code_string: str,
    display_string: Optional[str] = None,
    # todo: align with changes: now> 4 str, soon> 1 str: correctly ordered, serialized, joined DependsOnData list items
    depends_on_value_string: Optional[str] = None,
    depends_on_property: Optional[str] = None,
    depends_on_system: Optional[str] = None,
    depends_on_display: Optional[str] = None,
) -> str:
    """
    Internal INFX Systems use only. NOT intended to be exposed via any external API such as the Mapping Service.
    NOTE: This is NOT the standard code_id creation function. This function is a helper to that function.

    Concatenates the input string values in a specific order. Returns a hash of the concatenated string. Does not modify
    input strings in ANY way before concatenating and hashing. BEFORE calling this function, caller NUST apply measures
    to ensure consistent hashing of FHIR objects from JSON binary data, regardless of list order or key order in JSON.

    That is, when code_string and depends_on_value_string are not simple string values, but are serialized JSON objects,
    they MUST receive the following preparation before being input to this function as strings:
    - Systems functions must be used to force a consistent order on list entries at storage time.
    - Systems functions for JSON serialization to string must be used force a consistent JSON key order on the string.
      See prepare_code_string_for_code_id().

    All parameters except code_string are optional.
    @param code_string is required. It may be a serialized JSON string for a FHIR resource like CodeableConcept,
        or a string value, for a FHIR code. It may be any data type that RCDM allows for a source code in a ConceptMap.
    @param display_string is expected when the code is simple string and not a CodeableConcept, but it is not required.
    @param depends_on_value_string is an optional argument, but is REQUIRED if the code has associated dependsOn data.
        It may be a serialized JSON string for a FHIR resource, or a string value, for a FHIR string.
        It may be the serialized value of any FHIR data type that RCDM allows for a depends_on value in a ConceptMap.
        v4: It contains the DependsOnData.value or (for v4 > v5) MAY contain a join of all 4 DependsOnData properties.
            v4 data contains depends_on_value and depends_on_property only, no depends_on_system or depends_on_display.
        v5: It MUST contain ALL the DependsOnData list items correctly ordered, serialized, and joined.
            prepare_depends_on_for_storage() returns this value.
    @param depends_on_property (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_system (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    @param depends_on_display (if there is one, and if it is not included in the depends_on_value_string). Deprecated.
    """
    if code_string is None or code_string == "":
        raise ValueError("Cannot use an empty code_string for a code_id")
    if depends_on_property is not None and depends_on_property != "":
        if depends_on_value_string is None or depends_on_value_string == "":
            raise ValueError("If there is a depends_on_property there must be a depends_on_value")
    if depends_on_system is not None and depends_on_system != "":
        if depends_on_value_string is None or depends_on_value_string == "":
            raise ValueError("If there is a depends_on_system there must be a depends_on_value")
    if depends_on_display is not None and depends_on_display != "":
        if depends_on_value_string is None or depends_on_value_string == "":
            raise ValueError("If there is a depends_on_display there must be a depends_on_value")
    concatenated = (
        code_string
        + "|" + (display_string if display_string is not None else "")
        + "|" + app.helpers.format_helper.prepare_depends_on_attributes_for_code_id_migration(
            depends_on_value_string,
            depends_on_property,
            depends_on_system,
            depends_on_display
        )
    )
    hashed = app.helpers.data_helper.hash_string(concatenated)
    return hashed


def hash_for_mapping_id(
    source_code_id: str,
    relationship_code: str,
    target_concept_code: str,
    target_concept_display: Optional[str] = None,
    target_concept_system: Optional[str] = None,
) -> str:
    """
    Internal INFX Systems use only. NOT intended to be exposed via any external API such as the Mapping Service.
    NOTE: This is NOT the standard mapping_id creation function. This function is a helper to that function.

    Concatenates the input string values in a specific order. Returns a hash of the concatenated string.
    Does not modify input strings in ANY way before concatenating and hashing.
    @param source_code_id MUST be a correctly generated, accurate, unique code_id value. Other params are strings.
    @param relationship_code: Required source-to-target relationship code
    @param target_concept_code: Required target code
    @param target_concept_display: Optional target display
    @param target_concept_system: Optional target system
    """
    if source_code_id is None or source_code_id == "":
        raise ValueError("Cannot create mapping_id without a source_code_id")
    elif relationship_code is None or relationship_code == "":
        raise ValueError("Cannot create mapping_id without a relationship_code")
    elif target_concept_code is None or target_concept_code == "":
        raise ValueError("Cannot create mapping_id without a target_concept_code")

    concatenated = (
        source_code_id
        + "|" + relationship_code
        + "|" + target_concept_code
        + "^" + (target_concept_display if target_concept_display is not None else "")
        + "^" + (target_concept_system if target_concept_system is not None else "")
    )

    hashed = app.helpers.data_helper.hash_string(concatenated)
    return hashed
