import datetime
import json
import re
from enum import Enum
from typing import Optional

import app.models.codes
import app.helpers.data_helper


class DataExtensionUrl(Enum):
    """
    Supported DataExtensionUrls for ConceptMap dynamic values, from the RCDM specification
    """
    SOURCE_CODEABLE_CONCEPT = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept"
    SOURCE_RATIO = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceRatio"
    NEEDS_EXAMPLE_DATA = "http://projectronin.io/fhir/StructureDefinition/Extension/needsExampleData"


class FHIRPrimitiveName(Enum):
    """
    Supported FHIR primitive data types for ConceptMap dynamic values, from the RCDM specification
    """
    CODE = "code"


class IssuePrefix(Enum):
    """
    Prefix for messages reporting format issues.
    # todo: delete this Enum after v5 migration is complete
    """
    COLUMN_VALUE_FORMAT = "format issue: "


def convert_string_to_datetime_or_none(input_string):
    """
    This is the standard, top-level function to prepare a datetime value in Informatics desired format, or return None.
    """
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")


def prepare_code_and_display_for_storage(raw_code = None, raw_display: str = None) -> (str, str, str, str, str):
    """
    This is the standard, top-level function to prepare 1 source concept object for input to 3 columns of a table.

    There is NO reason to validate results from this function, since it does ALL required validation internally.

    @param raw_code any object or string: may be a FHIR attribute from raw data, or a JSON binary or string column value
    @param raw_display optional, may appear with simple codes; for CodeableConcept will be overwritten with text value

    JSON binary column values (such as values from code_jsonb and depends_on_value_jsonb columns) are objects.
    If, during earlier processing logic, you read them values from tables into objects in your Python,
    and now you need to write these objects into tables, call this function to prepare the object values for storage.

    When receiving raw attributes values from tenant objects, they may be strings or objects.
    Call this function to prepare these string or object values for storage. It accepts any data type as raw_code.

    clinical-content tables that use this 3-column convention for source concepts are custom_terminologies.code_data,
    concept_maps.source_concept_data, and concept_maps.concept_relationship_data.
    Compare/contrast with prepare_depends_on_for_storage() which does not overlap with these use cases.
    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive  - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        display_string (string) - in the case of code being CodeableConcept, normalize display to the code.text value
    """
    normalized_code = normalized_codeable_concept_string(raw_code)
    normalized_display = raw_code.get("text")
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage(normalized_code, normalized_display)
    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_code_and_display_for_storage_migration(raw_code: str = None, raw_display: str = None) -> (str, str, str, str, str):
    """
    # todo: delete this function after v5 migration is complete
    Low-level helper function. DO NOT call this function directly.
    For reading old, inconsistently serialized code column values from v4 tables and preparing them for storage in v5 tables.
    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive  - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        display_string (string) - in the case of code being CodeableConcept, normalize display to the code.text value
    """
    # prepare_dynamic_value_for_storage() ensures all values are correct
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage(raw_code, raw_display)
    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_depends_on_for_storage(depends_on: list = None) -> (str, list):
    """
    # todo: NOT USED during v5 migration. Fast-follow-on: Top-level function for n-member dependsOn lists
    This is the standard, top-level function to prepare a DependsOnData list for custom_terminologies.code_depends_on.
    Compare/contrast with prepare_code_and_display_for_storage() which does not overlap with this use case.
    @param depends_on - list of DependsOnData objects - see the following notes on FHIR ConceptMap dependsOn as a list.

    There is NO reason to validate results from this function, since it does ALL required validation internally.

    FHIR defines the dependsOn attribute of a ConceptMap as a list because a mapping can depend on multiple factors.
    Even when there is only one dependsOn factor for a code, ConceptMap dependsOn is still a list (a 1-member list).

    This function expects that, as FHIR data is ingested from tenants, for each FHIR attribute that Ronin maps, whenever
    we need to collect a dependsOn list for that attribute we will write a purpose-built helper function for extracting
    the needed DependsOnData and creating a list of DependsOnData rows to store in custom_terminologies.code_depends_on
    for that source code. Examples of purpose-built FHIR attribute helper functions today:

        - normalized_codeable_concept_depends_on() is used on Observation.code when Observation.category is "SmartData".
            It is a general-purpose function for using 1 CodeableConcept value as a dependsOn (could serve other cases).
        - normalized_medication_ingredient_strength_depends_on() is used on Medication.ingredient (a list on Medication)
            It is a purpose-built function to walk the Medication.ingredient list structure to get the data we need.

    @return
        depends_on_value_string (str) - contains ALL the DependsOnData list items correctly ordered, serialized, and joined
            - ready for the caller to input to generate_code_id() as the depends_on_value_string.
        code_depends_on_insert_list - Each list entry is a tuple of several values as follows:
            sequence (int) - value to put in the sequence column
            value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
            value_simple (str) - column for dependsOn.value - old_code if primitive - else None
            value_jsonb (str) - column for dependsOn.value - None if primitive - else, serialized object string
                prepared for SQL (Note: SQLAlchemy can do SQL escaping but does not cover all the cases in our data)
            property (str) - value to put in the property column
            system (str) - dependsOn.system - usually not in data - the system column
            display (str) - dependsOn.display - usually not in data - the display column
    """
    sequence = 0
    depends_on_value_string = ""
    depends_on_list = []
    for row in depends_on:
        # prepare_depends_on_columns_for_storage() ensures all values are correct
        (
            value_schema,
            value_simple,
            value_jsonb,
            value_string,
            property
        ) = prepare_depends_on_columns_for_storage(row)
        sequence += 1
        depends_on_value_string += prepare_depends_on_attributes_for_code_id_migration(
            value_string,
            property,
            row.depends_on_system,
            row.depends_on_display
        )
        depends_on_list.append(
            (
                sequence,
                value_schema,
                value_simple,
                value_jsonb,
                property,
                row.depends_on_system,
                row.depends_on_display
            )
        )

    return depends_on_value_string, depends_on_list


def prepare_depends_on_attributes_for_code_id(input_object: 'app.models.codes.DependsOnData') -> str:
    """
    # todo: NOT USED during v5 migration. Fast-follow-on: replaces prepare_depends_on_attributes_for_code_id_migration()
    Low-level helper to correctly combine the 4 DependsOnData string values into one string as input for a code_id.
    """
    return prepare_depends_on_attributes_for_code_id_migration(
        input_object.depends_on_value,
        input_object.depends_on_property,
        input_object.depends_on_system,
        input_object.depends_on_display,
)


def prepare_depends_on_attributes_for_code_id_migration(
        depends_on_value_string: str,
        depends_on_property: Optional[str] = None,
        depends_on_system: Optional[str] = None,
        depends_on_display: Optional[str] = None,
) -> str:
    """
    # todo: Top-level for v5 migration. Fast-follow-on: replace with prepare_depends_on_attributes_for_code_id()
    Low-level helper to correctly combine the 4 DependsOnData string values into one string as input for a code_id.
    """
    if depends_on_value_string is None or depends_on_value_string == "":
        return ""
    return (depends_on_value_string
        + (depends_on_property if depends_on_property is not None else "")
        + (depends_on_system if depends_on_system is not None else "")
        + (depends_on_display if depends_on_display is not None else "")
    )


def prepare_depends_on_columns_for_storage(depends_on_row: 'app.models.codes.DependsOnData' = None) -> (str, str, str, str, str):
    """
    # todo: NOT USED during v5 migration. Fast-follow-on: helper for prepare_depends_on_for_storage()
    Helper function. DO NOT call this function directly.

    There is NO reason to validate results from this function, since it does ALL required validation internally.

    Low-level helper to prepare several values in DependsOnData for columns in custom_terminologies.code_depends_on.

    Does call prepare_depends_on_value_for_storage() to prepare 1 depends_on_value for 3 columns in code_depends_on.
    Does not process or supply a sequence value for the row. So, since FHIR dependsOn is always an ordered list, in
    specific sequence, do not call this function directly. Instead, call prepare_depends_on_for_storage() which provides
    detailed doc with examples. A summary of these details:
        1. Call (or write) the purpose-built function for this FHIR attribute's dependsOn data;
            this function will create a correct list of DependsOnData objects. Examples exist in current codebase.
        2. Input this list to prepare_depends_on_for_storage();
            this function orders the list in proper sequence and returns more values you need for the row inserts.
    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive  - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to contribute to generate_code_id().
        property_string (str) - returned property_string is ready for the caller to contribute to generate_code_id().
    """
    # prepare_depends_on_value_for_storage() ensures all values are correct
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string,
        property_string
    ) = prepare_depends_on_value_for_storage(depends_on_row.depends_on_value, depends_on_row.depends_on_property)
    return value_schema, value_simple, value_jsonb, value_string, property_string


def prepare_depends_on_value_for_storage(raw_depends_on_value: str = None, raw_depends_on_property: str = None) -> (str, str, str, str, str):
    """
    # todo: Top-level for v5 migration. Fast-follow-on: replace with prepare_depends_on_for_storage()
    Helper function. DO NOT call this function directly.

    There is NO reason to validate results from this function, since it does ALL required validation internally.

    Lowest-level helper to prepare 1 depends_on_value for 3 columns in the custom_terminologies.code_depends_on table.

    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive  - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        property_string (str) - returned property string is ready for the caller to contribute to generate_code_id().
        IF the inputs do not match the v4 legacy case this function returns 5 None values
    """
    # prepare_dynamic_value_for_storage() ensures all values are correctly;
    # discard display_string from prepare_dynamic_value_for_storage() and correct any legacy v4 property_string value
    property_string = raw_depends_on_property
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage(raw_depends_on_value, None)
    if value_schema is None:
        # not usable, return 5 None values
        property_string = None
    elif value_schema == FHIRPrimitiveName.CODE.value:
        # if the value was a string (legacy v4; not supported in v5) FHIR primitive data type named "code" was assigned
        (
            value_schema,
            value_simple,
            value_jsonb,
            value_string,
            property_string
        ) = convert_string_depends_on_to_codeable_concept(
            depends_on_value=value_simple,
            depends_on_property=raw_depends_on_property
        )
    return value_schema, value_simple, value_jsonb, value_string, property_string


def convert_string_depends_on_to_codeable_concept(depends_on_value: str, depends_on_property: str) -> (str, str, str, str, str):
    """
    # todo: a migration-only function for handling v4 legacy staging data
    @return
        value_schema (str) - DataExtensionUrls.SOURCE_CODEABLE_CONCEPT.value
        value_simple (str) - None
        value_jsonb (str) - CodeableConcept object serialized string prepared for SQL
            - depends_on_value converted to a CodeableConcept with a text attribute that holds the string value
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (str) - value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        property_string (str) - returned property_string is ready for the caller to contribute to generate_code_id()
            - depends_on_property with ".text" removed from the end of the string
        IF the inputs do not match the v4 legacy case this function returns 5 None values
    """
    false_result = None, None, None, None, None
    if depends_on_value is None or depends_on_property is None:
        return false_result
    if depends_on_value == "" or (depends_on_property is not None and ".text" not in depends_on_property[-5:]):
        return false_result
    # convert the legacy v4 string value to a v5 CodeableConcept
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string
    ) = prepare_object_source_code_for_storage({"text": depends_on_value})
    property_string = depends_on_property[:-5]
    return value_schema, value_simple, value_jsonb, value_string, property_string


def prepare_dynamic_value_for_storage(old_value: str = None, old_display: str = None) -> (str, str, str, str, str):
    """
    Helper function. DO NOT call this function directly.

    # todo: this function was written for migration. A future function would want an object as the first argument.
    # To avoid disrupting the migration while it is in progress, the top-level format helper functions do accept
    # objects as arguments, and then carefully (behind the curtain) ensure they call a standard normalization
    # function on the input object before sending the resulting serialized string into this function as old_value.

    For each FHIR element type we need, we write a wrapper function, which must be called instead of this function.
    For examples, see doc in prepare_code_and_display_for_storage() and prepare_depends_on_value_for_storage().

    For inserting dynamic value columns into a table that has already been migrated to the ConceptMap v5 schema.
    the input string in old_value may be a string or a serialized object. The end result is to prepare and return
    4 values the caller needs: 3 column values for storage, and 1 string to contribute to later hash calculations.

    Calls the helper function prepare_dynamic_value_for_storage_report_issue() to process the inputs.

    @param old_value - may be a string, or may be a FHIR code which as data is a string value
    @param old_display - this value is optional and might be present when the dynamic value is for a source code

    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive  - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        display_string (string) - in the case of code being CodeableConcept, normalize display to the code.text value
    """
    (
        value_schema, 
        value_simple, 
        value_jsonb, 
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage_report_issue(old_value, old_display)
    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_dynamic_value_for_storage_report_issue(old_value: str = None, old_display: str = None) -> (str, str, str, str, str):
    """
    Helper function does the work for prepare_dynamic_value_for_storage(). DO NOT call this function directly.

    # todo: this function was written for migration. A future function would want an object as the first argument.
    # To avoid disrupting the migration while it is in progress, the top-level format helper functions do accept
    # objects as arguments, and then carefully (behind the curtain) ensure they call a standard normalization
    # function on the input object before sending the resulting serialized string into this function as old_value.

    @param old_value - may be a string, or may be a FHIR code or serialized FHIR object.
    @param old_display - (Optional) the old_display value that came with the old_code value, if available.

    Returns 5 values the caller needs for storage:
    - 3 column values for storing the value,
    - 1 string to contribute to calculating an identity hash, and
    - (if old_display is input) 1 display value, normalized to CodeableConcept.text if the old_value was CodeableConcept

    @return
        value_schema (str) - FHIR primitive name or DataExtensionUrls value - if None, inputs could not be processed
        value_simple (str) - old_code if primitive - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
        display_string (string) - in the case of code being CodeableConcept, normalize display to the "text" value
    """

    # null depends_on
    if old_display is None and (old_value is None or old_value == ""):
        return None, None, None, None, None

    # init
    value_schema = None
    value_simple = None
    value_jsonb = None
    value_string = None

    # CodeableConcept will overwrite this with the CodeableConcept.text value
    display_string = old_display

    # an all-digits simple value may be read as an int
    if old_value is not None and old_value.isnumeric():
        old_value = f"{old_value}"

    # unwrap any wrappers in the JSON
    # todo: delete this legacy check after v5 migration is complete
    if old_value is not None and '"valueCodeableConcept"' in old_value:
        # code
        old_value = remove_deprecated_wrapper(old_value)

    # detect issue cases:
    # todo: after v5 migration is complete, this if/elif/else begins at is_json_format, others to be discarded as shown
    # handle None (bad)
    # todo: delete this legacy check after v5 migration is complete
    if old_value is None and old_display is not None:
        # code: report format issue
        (value_schema, value_simple, value_jsonb, value_string) = prepare_code_none_issue_for_storage(
            issue="value is None",
            id="value is null"
        )

    # handle '' (bad)
    # todo: delete this legacy check after v5 migration is complete
    elif old_value == "" and old_display is not None:
        # code: report format issue
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_storage(
            issue="value is ''",
            code_string=old_value
        )

    # handle Ronin cancer staging pattern as spark (good)
    # todo: delete this legacy check after v5 migration is complete
    elif old_display is not None and re.search(r"\{Line \d}", old_value):
        # code: convert to json
        json_string = convert_source_concept_text_only_spark_export_string_to_json_string(old_value)
        (
            value_schema,
            value_simple,
            value_jsonb,
            value_string,
            display_string
        ) = prepare_json_format_for_storage(json_string, old_display)

    # handle '[null]' variants (bad)
    # todo: delete this legacy check after v5 migration is complete
    elif old_value == "[null]" or re.search(r"\[null, ", old_value):
        # depends_on: correct to null
        if old_display is None:
            (value_schema, value_simple, value_jsonb, value_string) = (None, None, None, None)
        # code: report format issue
        else:
            (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_storage(
                issue="value is '[null]' or '[null, null]' or '[null, null, null]'",
                code_string=old_value
            )

    # handle spark case (bad)
    # todo: delete this legacy check after v5 migration is complete
    elif app.helpers.data_helper.is_spark_format(old_value):
        # 2 steps to final: convert spark to JSON, then process as in the JSON case
        # try to convert to json
        json_string = convert_source_concept_spark_export_string_to_json_string_unordered(old_value)
        # failed to convert to json
        if json_string == old_value:
            # depends_on: already reported by the filter_unsafe_depends_on_value function
            # code: report format issue
            if old_display is not None:
                (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_storage(
                    issue="spark",
                    code_string=old_value
                )
        # converted to json
        else:
            # code or depends_on
            (
                value_schema,
                value_simple,
                value_jsonb,
                value_string,
                display_string
            ) = prepare_json_format_for_storage(json_string, old_display)

    # json
    # todo: after v5 migration is complete, the if/elif/else should begin here; cases above here should be discarded
    elif app.helpers.data_helper.is_json_format(old_value):
        (
            value_schema,
            value_simple,
            value_jsonb,
            value_string,
            display_string
        ) = prepare_json_format_for_storage(old_value, old_display)

    # string
    else:
        value_schema = FHIRPrimitiveName.CODE.value
        value_simple = old_value
        value_string = old_value

    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_json_format_for_storage(json_string: str, display_string: str = None) -> (str, str, str, str, str):
    """
    Low-level helper function for prepare_dynamic_value_for_storage(). DO NOT call this function directly.

    There is NO reason to validate results from this function, since it does ALL required validation internally.

    # todo: this function was written for migration. A future function would want an object as the first argument.
    # To avoid disrupting the migration while it is in progress, the top-level format helper functions do accept
    # objects as arguments, and then carefully (behind the curtain) ensure they call a standard normalization
    # function on the input object before sending the resulting serialized string into this function as old_value.

    Normalizes the JSON string to an expected format with double quotes and no wasted space - calls helper functions
    in the correct order: load the object, normalize to RCDM (sorts lists), serialize (sorts keys, strips space).
    @param json_string - serialized JSON string for an object to be processed
    @param display_string (Optional) - if a display value was supplied along with the object value
    @return
        code_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        code_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        value_jsonb (str) - None if primitive - else, object with that value_schema - string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        code_string (string) - either code_simple, or the code_jsonb binary JSON value correctly serialized to a string
            - this returned code_string value is ready for the caller to input to the hash function generate_code_id().
        display_string (string) - if code is CodeableConcept, the code.text value, otherwise the input value
        Note: We no longer encounter format issues we cannot resolve. Just in case, if that happens return all 5 None.
    """
    false_result = None, None, None, None, None
    try:
        json_object = app.helpers.data_helper.load_json_string(json_string)

        # Ratio
        if '"numerator"' in json_string or '"denominator"' in json_string:
            # todo: prepare_object_source_ratio_for_storage(ratio_object=json_object) - we do not yet have data to test
            return false_result

        # CodeableConcept
        elif '"text"' in json_string or (
                '"coding"' in json_string and (
                '"code"' in json_string
                or '"display"' in json_string
                or '"system"' in json_string
                or '"version"' in json_string
        )
        ):
            try:
                (code_schema, code_simple, code_jsonb, code_string) = prepare_object_source_code_for_storage(
                    code_object=json_object
                )
                display_string = json_object.get("text")
            except Exception as e:
                return false_result

        # unsupported FHIR resource type, or an unexpected data dictionary
        else:
            return false_result

    except Exception as e:
        return false_result

    return code_schema, code_simple, code_jsonb, code_string, display_string


def prepare_binding_and_value_for_jsonb_insert(insert_column_name: str, value_schema: str, jsonb_column_value) -> (str, str):
    """
    This is the top-level, standard function for preparing JSON Binary values for use in INFX insert queries.

    Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.

    @param insert_column_name (str) - column name in the table for the INSERT or UPDATE
    @param value_schema (str) - valid value for a code_schema or depends_on_schema column in INFX clinical-content.
    @param jsonb_column_value (JSON Binary object)

    @return (insert_binding_string, insert_none_value) means ("for the INSERT/UPDATE list", "for the VALUES list")
        The insert_none_value is a dictionary entry to be appended to the values being input to conn.execute().
        To prep before your query do this: if insert_none_value is not None: values_for_query.append(insert_none_value)
        There are 3 possible pairs of return value from this function:
        - EITHER: jsonb_column_value is None, returns: ( f" :{insert_column_name}, ", {f"{insert_column_name}": None} )
        - OR: jsonb_column_value is a JSON Binary object, returns: ( f" '{jsonb_sql_escaped}'::jsonb, None )
        - If value_schema is not a valid schema for a JSON Binary object in INFX clinical-content, returns (None, None)
    """
    # Call (or write) the appropriate helper function for each supported schema type - current examples are shown below
    if value_schema == DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value:
        normalized_json_string = normalized_codeable_concept_string(jsonb_column_value)
    elif value_schema == DataExtensionUrl.SOURCE_RATIO.value:
        normalized_json_string = normalized_ratio_string(jsonb_column_value)
    else:
        normalized_json_string = None
    return prepare_binding_and_value_for_jsonb_insert_migration(insert_column_name, normalized_json_string)


def prepare_binding_and_value_for_jsonb_insert_migration(insert_column_name: str, normalized_json_string: str) -> (str, str):
    """
    Helper function does the work for prepare_binding_and_value_for_jsonb_insert(). DO NOT call this function directly.

    Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
    For examples of use see data_migration.py.

    @param insert_column_name (str) - column name in the table for the INSERT or UPDATE
    @param normalized_json_string (str) - normalized, serialized JSON string produced by the appropriate helper function
        for the schema type for the data, for example normalized_codeable_concept_string() or normalized_ratio_string()

    @return (insert_binding_string, insert_none_value) means ("for the INSERT/UPDATE list", "for the VALUES list")
        The insert_none_value is a dictionary entry to be appended to the values being input to conn.execute().
        To prep before your query do this: if insert_none_value is not None: values_for_query.append(insert_none_value)
        There are 3 possible pairs of return value from this function:
        - EITHER: jsonb_column_value is None, returns: ( f" :{insert_column_name}, ", {f"{insert_column_name}": None} )
        - OR: jsonb_column_value is a JSON Binary object, returns: ( f" '{jsonb_sql_escaped}'::jsonb, None )
        - If normalized_json_string is None, returns (None, None)
    """
    # todo: Remove after migration is complete.
    if normalized_json_string is None:
        return None, None
    jsonb_sql_escaped = app.helpers.data_helper.escape_sql_input_value(normalized_json_string)
    if jsonb_sql_escaped is None:
        insert_binding_string = f" :{insert_column_name}, "
        insert_none_value = {f"{insert_column_name}": None}
    else:
        insert_binding_string = f" '{jsonb_sql_escaped}'::jsonb, "
        insert_none_value = None
    return (insert_binding_string, insert_none_value)


def normalized_medication_ingredient_strength_depends_on(ingredient_list_object) -> list:
    """
    Purpose-built function for creating a list of DependsOnData from a FHIR Medication.ingredient list object.

    @param ingredient_list_object is a FHIR Medication.ingredient list object, which may have 0 or more members,
        although at least 1 and usually more than 1 are expected. Caller is responsible for inputting an object
        (not a serialized string). Object must be FHIR but is raw upon ingestion, so need not conform to RCDM profiles.
    @return a list of DependsOnData objects. In each DependsOnData, the depends_on_value is a str that is a FHIR Ratio
        object normalized to RCDM Ratio and serialized. Each DependsOnData gives the Medication.ingredient.strength
        value for a member of the input Medication.ingredient list, in the same list order as the Medication.ingredient
        list. The depends_on_property for each DependsOnData object is the same: "Medication.ingredient.strength".
        If the input object is None, or if the input object is an empty list, function returns None: no dependsOn data.
    """
    pass  # todo: this is a stub to be implemented


def prepare_object_source_ratio_for_storage(ratio_object) -> (str, str, str, str):
    """
    @raise BadDataError if the value does not match the sourceRatio schema
    """
    rcdm_object = app.helpers.data_helper.normalized_source_ratio(ratio_object)
    rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_RATIO.value,
        None,
        app.helpers.data_helper.escape_sql_input_value(rcdm_string),
        rcdm_string
    )


def normalized_ratio_string(code_object) -> str:
    """
    @raise BadDataError if the value is not a Ratio
    @return (str) serialized RCDM Ratio normalized for: JSON format, JSON key order, coding list member order
    """
    rcdm_object = app.helpers.data_helper.normalized_source_ratio(code_object)
    rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
    return rcdm_string


def prepare_object_source_code_for_storage(code_object) -> (str, str, str, str):
    """
    @raise BadDataError if the value does not match the sourceCodeableConcept schema
    @return
        value_schema (str) - DataExtensionUrls.SOURCE_CODEABLE_CONCEPT.value
        value_simple (str) - None
        value_jsonb (str) - CodeableConcept object serialized string prepared for SQL
            Note: SQLAlchemy can do SQL escaping, but does not cover all cases in our data. We supplement as needed.
        value_string (string) - value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to the hash function generate_code_id().
    """
    rcdm_object = app.helpers.data_helper.normalized_source_codeable_concept(code_object)
    rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value,
        None,
        app.helpers.data_helper.escape_sql_input_value(rcdm_string),
        rcdm_string
    )


def normalized_codeable_concept_string(code_object) -> str:
    """
    @raise BadDataError if the value is not a CodeableConcept
    @return (str) serialized RCDM CodeableConcept normalized for: JSON format, JSON key order, coding list member order
    """
    rcdm_object = app.helpers.data_helper.normalized_source_codeable_concept(code_object)
    rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
    return rcdm_string


def normalized_codeable_concept_and_display(
    raw_code: dict = None
) -> (str, str):
    """
    Top-level, standard function.

    Prepares raw, new, ingested attributes from tenant FHIR data for safe entry into in Informatics custom terminologies
    - for use in Mapping Request and all successor services to ensure data format correction at the FIRST point of entry
    """
    if raw_code is None or len(raw_code) == 0:
        return None
    if "text" not in raw_code and "coding" not in raw_code:
        return None
    normalized_code = normalized_codeable_concept_string(raw_code)
    normalized_display = raw_code.get("text")
    return (normalized_code, normalized_display)


def normalized_primitive_code_and_display(
        raw_data: dict = None,
        attribute: str = None,
        raw_display: Optional[str] = None
) -> (str, str):
    """
    Top-level, standard function.

    Prepares raw, new, ingested attributes from tenant FHIR data for safe entry into in Informatics custom terminologies
    - for use in Mapping Request and all successor services to ensure data format correction at the FIRST point of entry
    """
    if attribute is None or raw_data is None or attribute not in raw_data:
        return None
    raw_code = raw_data[attribute]
    return (raw_code, raw_display)


def normalized_codeable_concept_depends_on(
        codeable_concept_object,
        depends_on_property: str,
        depends_on_system: Optional[str] = None,
        depends_on_display: Optional[str] = None
):
    """
    Top-level, standard function.

    Prepares raw, new, ingested attributes from tenant FHIR data for safe entry into in Informatics custom terminologies
    - for use in Mapping Request and all successor services to ensure data format correction at the FIRST point of entry

    Purpose-built function for creating a list of DependsOnData from any FHIR attribute that is a 0..1 CodeableConcept.
    This function does NOT accept a LIST of CodeableConcept as input; see normalized_codeable_concept_list_depends_on().

    Given a CodeableConcept value, return DependsOnData to use as input into the creation of a new Terminology Code.
    @raise BadDataError if the value is not a CodeableConcept
    @return - (see todo)
    # todo: align with changes: now> @return DependsOnData object, soon> @return 1-member list of DependsOnData
    """
    return app.models.codes.DependsOnData(
        depends_on_value=codeable_concept_object,
        depends_on_value_schema=app.models.codes.DependsOnSchemas.CODEABLE_CONCEPT,
        depends_on_property=depends_on_property
    )


def normalized_codeable_concept_list_depends_on(
        list_object,
        depends_on_property: str,
        depends_on_system: Optional[str] = None,
        depends_on_display: Optional[str] = None
) -> list:
    """
    Top-level, standard function.

    Prepares raw, new, ingested attributes from tenant FHIR data for safe entry into in Informatics custom terminologies
    - for use in Mapping Request and all successor services to ensure data format correction at the FIRST point of entry

    Purpose-built function for creating a list of DependsOnData from any FHIR attribute that is a CodeableConcept LIST.
    This function does NOT accept a single CodeableConcept as input; see normalized_codeable_concept_depends_on().

    Given a CodeableConcept value, return DependsOnData to use as input into the creation of a new Terminology Code.
    @raise BadDataError if the value is not a CodeableConcept
    @return - list of DependsOnData
    """
    result_list = []
    for codeable_concept_object in list_object:
        result_list.append(
            app.models.codes.DependsOnData(
                depends_on_value=normalized_codeable_concept_string(codeable_concept_object),
                depends_on_property=depends_on_property,
                depends_on_system=depends_on_system,
                depends_on_display=depends_on_display,
            )
        )
    return result_list


def prepare_additional_data_for_storage(additional_data: str, rejected_depends_on_value: str = None):
    """
    @param additional_data (str) - serialized data dictionary for internal INFX Systems and Content use
    @param rejected_depends_on_value (str) - for legacy issues this may contain a string value that is unsafe to keep
    """

    # todo: This block is intended for use only during database migration to v5. After migration it should be deleted.
    if rejected_depends_on_value is not None:
        if additional_data is None or additional_data == "" or additional_data == "null":
            additional_data_dict = {}
        else:
            try:
                additional_data_dict = app.helpers.data_helper.load_json_string(additional_data)
            except Exception as e:
                # for bad old data, simply discard what was there and add the necessary info
                additional_data_dict = {}
        warning = [
            "The depends_on data format provided with this code is NOT SAFE for clinical use",
            "Do not attempt to interpret or use these values",
            "Each may have ANY ORDER and ANY MEANING"
        ]
        additional_data_dict.update(
            {
                "warning_unsafe_rejected_data": " - ".join(warning),
                "rejected_data_do_not_use": rejected_depends_on_value
            }
        )

    # todo: After migration, remove all of the above, rewrite this function, & REMOVE the rejected_depends_on_value arg


def prepare_data_dictionary_for_storage(info_dict):
    """
    Serialize a python data dictionary for a string valued column - like additional_data for INFX internal use only
    @return the string, or None if the dictionary was empty
    """
    if info_dict is None or len(info_dict) == 0:
        return None
    info_string = json.dumps(info_dict)
    clean_info = app.helpers.data_helper.cleanup_json_string(info_string)
    sql_escaped = app.helpers.data_helper.escape_sql_input_value(clean_info)
    if len(sql_escaped) == 0:
        return None
    return sql_escaped


def normalized_data_dictionary_string(info_dict):
    """
    Serialize a python data dictionary for a string valued column - like additional_data for INFX internal use only
    @return the string, or None if the dictionary was empty
    """
    if info_dict is None or len(info_dict) == 0:
        return None
    info_string = json.dumps(info_dict)
    clean_info = app.helpers.data_helper.cleanup_json_string(info_string)
    return clean_info


def prepare_code_none_issue_for_storage(issue: str, id: str) -> (str, str, str, str):
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        None,
        None,
        id
    )


def prepare_string_format_issue_for_storage(issue: str, code_string: str) -> (str, str, str, str):
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        code_string,
        None,
        code_string
    )


def append_string_format_issue_for_storage(issue: str, schema: str) -> str:
    if schema is None or len(schema) == 0:
        return f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}"
    else:
        return schema + f", {issue}"


def prepare_object_format_issue_for_storage(issue: str, code_string: str) -> (str, str, str, str):
    code_clean = app.helpers.data_helper.cleanup_json_string(code_string)
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        code_clean,
        None,
        code_clean
    )


def remove_deprecated_wrapper(input_string: str) -> str:
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    deprecated wrapper convention uses keys "component", "valueCodeableConcept", or both - remove if present
    @return (bool: whether the wrapper was found and removed, str: the string to use after unwrapping)
    """
    output_string = input_string
    if app.helpers.data_helper.is_json_format(input_string):
        try:
            json_object = app.helpers.data_helper.load_json_string(input_string)
            component = json_object.get("component")
            if component is None:
                codeable_concept = json_object.get("valueCodeableConcept")
                if codeable_concept is not None:
                    output_string = app.helpers.data_helper.serialize_json_object(codeable_concept)
            else:
                if isinstance(component, list):
                    # It is only safe to grab the valueCodeableConcept for this repair if it is single
                    if len(component) == 1:
                        for concept in component:
                            codeable_concept = concept.get("valueCodeableConcept")
                            if codeable_concept is not None:
                                output_string = app.helpers.data_helper.serialize_json_object(codeable_concept)
                else:
                    codeable_concept = component.get("valueCodeableConcept")
                    if codeable_concept is not None:
                        output_string = app.helpers.data_helper.serialize_json_object(codeable_concept)
        except Exception as e:
            pass
    return output_string


def filter_unsafe_depends_on_value(input_string: str) -> (str, str):
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    @return
        depends_on_value (str) - text value, or serialized object string
        rejected_depends_on_value (str) - the input_string value is returned here if too malformed to use
    """
    if input_string is None or input_string == "":
        return None, None
    if app.helpers.data_helper.is_json_format(input_string):
        try:
            # Handle cases based on the real data: a list of unsafe strength data from spark, a CodeableConcept, or text
            json_object = app.helpers.data_helper.load_json_string(input_string)
            if isinstance(json_object, list):
                # Came from a spark list of unsafe Medication.ingredient.strength values with mixed orders of num/denom
                return None, input_string
            else:
                # A single CodeableConcept value as a serialized string, not a list
                return input_string, None
        except Exception as e:
            pass
    if app.helpers.data_helper.is_spark_format:
        # All spark in real data are unsafe Medication.ingredient.strength values with some mixed orders of num/denom
        return None, input_string
    else:
        # All remaining cases in real data are a text value, not a list
        return input_string, None


def is_code_display_reversed(code_string: str, display_string: str):
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    good-enough check for likelihood that this is a case where code and display columns were reversed by a load function
    """
    # In valid code/display pairs it is unlikely that code and display have the same value or that either is empty
    if code_string is None:
        if display_string is None:
            return False
        else:
            return True
    if code_string == display_string:
        return False

    # Check length 0
    code_len = len(code_string)
    if display_string is None:
        display_len = 0
    else:
        display_len = len(display_string)
    if code_len == 0 and display_len > 0:
        return True
    if display_len == 0 and code_len > 0:
        return False

    code_alpha_count = sum(1 for char in code_string if char.isalpha())
    code_digit_count = sum(1 for char in code_string if char.isdigit())
    code_hyphen_count = sum(1 for char in code_string if char == "-")
    code_alnum_count = code_alpha_count + code_digit_count
    expected_code_len = code_alnum_count + code_hyphen_count

    display_alpha_count = sum(1 for char in display_string if char.isalpha())
    display_digit_count = sum(1 for char in display_string if char.isdigit())
    display_hyphen_count = sum(1 for char in display_string if char == "-")
    display_alnum_count = display_alpha_count + display_digit_count
    expected_display_code_len = display_alnum_count + display_hyphen_count

    # There are non-typical values in the code value, while at the same time, display looks like a code value
    if code_len > expected_code_len and display_len == expected_display_code_len:
        return True
    # There are more alphabetic values in the code than in the display
    if code_alpha_count > display_alpha_count:
        return True
    # There are more numeric values in the display than in the code
    if code_digit_count < display_digit_count:
        return True
    # There are too many numeric values in the display for it to be human-readable
    if display_digit_count == display_len or (display_digit_count >= (display_len / 2)):
        return True
    # If there are only alnum or hyphen characters in the code, and it is reasonably short, it is probably fine
    if code_len == expected_code_len and code_len <= display_len:
        return False
    # In FHIR code/display value sets, display has (title case) + spaces, while code is (lowercase or digits) + hyphens
    code_clean = "".join(code_string.split("-"))
    display_clean = "".join(display_string.split(" "))
    if (
        len(code_clean) == len(display_clean)
            and code_clean[0].isupper()
            and display_clean[0].islower()
            and code_clean[0].lower() == display_clean[0]
    ):
        return True
    # We could not identify any issues with this code/display pair
    return False


def convert_source_concept_text_only_spark_export_string_to_json_string(spark_export_string: str) -> str:
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    These strings in format {Line n} were created during early days of cancer staging work.
    Many examples of the more desired JSON format, {"text":"Line n"} exist in other data in the same table.
    The value of n does not matter in this case but is usually 1. There are no other cases of spark export data being
    text-only CodeableConcepts like this, so all cases of this spark format in current data are in this form: {Line n}
    @param spark_export_string is a string in the format {anything}. The string inside the braces is not examined.
    @return The string inside the pair of braces (in this case, anything) in this format: {"text":"anything"}
    @raise exception if parsing hits any snags, which it should not
    """
    json_string = "{" + f'"text":"{spark_export_string[1:-1]}"' + "}"
    return json_string


def convert_source_concept_spark_export_string_to_json_string_normalized_ordered(spark_export_string: str) -> str:
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    The standard function for converting spark export format data to an ordered, RCDM compliant sourceCodeableConcept.
    Always call is_spark_format(spark_export_string) first to ensure this function will work. See @return notes below.

    @param spark_export_string is an artifact of a legacy issue during an ETL operation, details omitted here.
        Possible values are a malformed FHIR Medication.ingredient.strength Ratio for a target dependsOn (do not
        process, return the input string unchanged) or a CodeableConcept (process and return a serialized JSON string).
    @return JSON serialized string for a FHIR CodeableConcept, if successful. Otherwise, returns the input string.
        This function calls normalized_source_codeable_concept() and serialize_json_object() to return an RCDM model
        compliant sourceCodeableConcept serialized as a string with correctly ordered JSON keys and list members.
        This output is suitable as input to id_helper.py functions as a code_string to create a code_id or mapping_id.
    """
    code_string = convert_source_concept_spark_export_string_to_json_string_unordered(spark_export_string)
    code_object = app.helpers.data_helper.load_json_string(code_string)
    rcdm_object = app.helpers.data_helper.normalized_source_codeable_concept(code_object)
    rcdm_string = app.helpers.data_helper.serialize_json_object(rcdm_object)
    return rcdm_string


def convert_source_concept_spark_export_string_to_json_string_unordered(spark_export_string: str) -> str:
    """
    This function is for v5 data migration only. The issue being fixed was a one-time issue from an old ETL script.
    Always call is_spark_format(spark_export_string) first to ensure this good-enough function will work.
    @param spark_export_string is an artifact of a legacy issue during an ETL operation, details omitted here.
        Possible values are a malformed FHIR Medication.ingredient.strength Ratio for a target dependsOn (do not
        process, return the input string unchanged) or a CodeableConcept (process and return a serialized JSON string).
    @return JSON serialized string for a FHIR CodeableConcept, if successful. Otherwise, returns the input string.
    """
    # reject a malformed Medication.ingredient in spark format
    if re.search("http://unitsofmeasure.org", spark_export_string):
        return spark_export_string

    # init
    json_object: dict = {}
    coding_list = []
    value_list = re.split(r'], ', spark_export_string)

    # coding
    if len(value_list) > 0:
        coding_string = value_list[0][2:]
        coding_value_list = re.split(r'}, \{', coding_string)
        if len(coding_value_list) > 0:
            coding_value_list[0] = coding_value_list[0][1:]
            coding_value_list[-1] = coding_value_list[-1][:-1]
            for coding_value in coding_value_list:
                attribute = re.split(r', ', coding_value)
                coding_object: dict = {}
                if len(attribute) >= 3:
                    first = attribute[0]
                    middle = ", ".join(attribute[1:-1])
                    last = attribute[-1]
                    if first != "null":
                        coding_object.update({"code": first})
                    if middle != "null":
                        coding_object.update({"display": middle})
                    if last != "null":
                        coding_object.update({"system": last})
                elif len(attribute) == 2:
                    first = attribute[0]
                    last = attribute[-1]
                    if first != "null":
                        coding_object.update({"code": first})
                    if last != "null":
                        coding_object.update({"system": last})
                else:
                    return spark_export_string
                if len(coding_object) > 0:
                    coding_list.append(coding_object)
            if len(coding_list) > 0:
                json_object.update({"coding": coding_list})
        else:
            return spark_export_string
    else:
        return spark_export_string

    # text
    if len(value_list) > 1:
        text_value = value_list[1][:-1]
        if text_value != "null":
            json_object.update({"text": text_value})

    # done
    return app.helpers.data_helper.serialize_json_object(json_object)
