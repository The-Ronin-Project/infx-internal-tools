import datetime
import json
import re
from enum import Enum

from app.helpers.data_helper import is_uuid4_format, is_spark_format, is_json_format, load_json_string, \
    normalize_source_ratio, serialize_json_object, escape_sql_input_value, normalize_source_codeable_concept, \
    cleanup_json_string
from app.helpers.message_helper import message_exception_classname


class DataExtensionUrl(Enum):
    """
    DataExtensionUrls for ConceptMaps from the RCDM specification
    """
    SOURCE_CODEABLE_CONCEPT = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept"
    SOURCE_RATIO = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceRatio"
    NEEDS_EXAMPLE_DATA = "http://projectronin.io/fhir/StructureDefinition/Extension/needsExampleData"


class IssuePrefix(Enum):
    """
    Prefix for messages reporting format issues
    """
    COLUMN_VALUE_FORMAT = "format issue: "


def convert_string_to_datetime_or_none(input_string):
    if input_string is None:
        return None
    else:
        try:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return datetime.datetime.strptime(input_string, "%Y-%m-%dT%H:%M:%SZ")


def prepare_code_and_display_for_storage(raw_code: str = None, raw_display: str = None) -> (str, str, str, str, str):
    return prepare_dynamic_value_for_storage(raw_code, raw_display)


def prepare_depends_on_value_for_storage(raw_depends_on_value: str = None) -> (str, str, str, str):
    (
        value_schema,
        value_simple,
        value_jsonb,
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage(raw_depends_on_value, None)
    return value_schema, value_simple, value_jsonb, value_string

    
def prepare_dynamic_value_for_storage(old_value: str = None, old_display: str = None) -> (str, str, str, str, str):
    """
    To ensure clarity for callers, wrapper functions prepare_code_and_display_for_storage() and
    prepare_depends_on_value_for_storage() are provided and should be used instead of this function.

    For inserting code or dependsOn columns into a table that has already been migrated to the ConceptMap v5 schema.
    the input string in old_value may be a string or a serialized object. The end result is to prepare and return
    4 values the caller needs: 3 column values for storage, and 1 string to contribute to later hash calculations.

    Calls the helper function prepare_dynamic_value_for_storage_report_issue() to process the inputs.

    @param old_value - may be a string, or may be a FHIR code which as data is a string value
    @param old_display - IMPORTANT - caller must supply this value if the old_value is a FHIR code - used for some tests

    @return
        value_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        value_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        value_jsonb (string) - None if primitive or old_value has "format issue:" - else, object with that value_schema
            - note: because this function prepares these values for SQL INSERT query formation, this value is a string -
            all JSON uses double quotes, and if there is a single quote value in the JSON, it is '' for SQL escaping
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to hash_string to create the code_id
        display_string (string) - in the case of code being CodeableConcept, normalize display to the code.text value
    """
    (
        value_schema, 
        value_simple, 
        value_jsonb, 
        value_string,
        display_string
    ) = prepare_dynamic_value_for_storage_report_issue(old_value, old_display)
    
    # todo: This block is intended for use only after database migration is complete. During migration we need issues!
    # @raise ValueError if the old_value cannot be correctly stored, so the caller should not insert it in the database.
    # if value_schema is None:
    #     raise ValueError("Value cannot be stored: format issues found")
    # if IssuePrefix.COLUMN_VALUE_FORMAT.value in value_schema:
    #     raise ValueError(value_schema)
    
    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_dynamic_value_for_storage_report_issue(old_value: str = None, old_display: str = None) -> (str, str, str, str, str):
    """
    Helper function does the work for prepare_dynamic_value_for_storage().
    To ensure clarity for callers, wrapper functions prepare_code_and_display_for_storage() and
    prepare_depends_on_value_for_storage() are provided and should be used.

    If old_display is supplied by the caller, the old_value is treated as a code value; otherwise as a depends_on value.

    The input string in old_value may be a string or a serialized object. The result is to prepare and return
    4 values the caller needs: 3 column values for storage, and 1 string to contribute to later hash calculations.
    This function is intended to support multiple data types for FHIR dynamic values: code, CodeableConcept, Ratio, etc.
    For CodeableConcept a 5th value is returned giving a display value, normalized to be equal to CodeableConcept.text

    @param old_value - may be a string, or may be a FHIR code or serialized FHIR object.
    @param old_display - (Optional) the old_display value that came with the old_code value, if available.

    @return
        value_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        value_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        value_jsonb (string) - None if primitive or old_value has "format issue:" - else, object with that value_schema
            - note: because this function prepares these values for SQL INSERT query formation, this value is a string -
            all JSON uses double quotes, and if there is a single quote value in the JSON, it is '' for SQL escaping
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the caller to input to hash_string to create the code_id
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
    if old_value.isnumeric():
        old_value = f"{old_value}"

    # unwrap any wrappers in the JSON
    if old_value is not None and '"valueCodeableConcept"' in old_value:
        # code
        old_value = remove_deprecated_wrapper(old_value)

    # begin: detect code and depends_on issue cases
    # handle None (bad)
    if old_value is None and old_display is not None:
        # code: report format issue
        (value_schema, value_simple, value_jsonb, value_string) = prepare_code_none_issue_for_storage(
            issue="value is None",
            id="value is null"
        )

    # handle '' (bad)
    elif old_value == "" and old_display is not None:
        # code: report format issue
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_storage(
            issue="value is ''",
            code_string=old_value
        )

    # handle uuid case (bad)
    elif old_display is not None and is_uuid4_format(old_value):
        # code: report format issue
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_storage(
            issue="uuid",
            code_string=old_value
        )

    # handle Ronin cancer staging pattern as spark (good)
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
    elif is_spark_format(old_value):
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

    # handle json case (good or bad)
    elif is_json_format(old_value):
        (
            value_schema,
            value_simple,
            value_jsonb,
            value_string,
            display_string
        ) = prepare_json_format_for_storage(old_value, old_display)

    # (removed) an issue with code and display columns reversed as a result of an old ETL script is no longer present

    # depends on: string (good)
    elif old_display is None:
        # depends_on
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_depends_on_for_storage(
            code_string=old_value
        )

    # code: string (good)
    else:
        # code
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_source_code_for_storage(
            code_string=old_value
        )
    # end: detect code and depends_on issue cases

    return value_schema, value_simple, value_jsonb, value_string, display_string


def prepare_json_format_for_storage(json_string: str, display_string: str = None) -> (str, str, str, str, str):
    """
    Normalizes the JSON string to an expected format with double quotes and no wasted space - calls helper functions
    in the correct order: load the object, normalize to RCDM (sorts lists), serialize (sorts keys, strips space)
    @param json_string - serialized JSON string for an object to be processed
    @param display_string (Optional) - if a display value was supplied along with the object value
    @return
        code_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        code_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        code_jsonb (string) - None if primitive or old_code has "format issue:" - else, the object with that code_schema
            - note: because this function prepares these values for SQL INSERT query formation, this value is a string -
            all JSON uses double quotes, and if there is a single quote value in the JSON, it is '' for SQL escaping
        code_string (string) - either code_simple, or the code_jsonb binary JSON value correctly serialized to a string
        display_string (string) - if code is CodeableConcept, the code.text value, otherwise the input value
    """
    try:
        json_object = load_json_string(json_string)

        # Ratio
        if '"numerator"' in json_string or '"denominator"' in json_string:
            # todo: prepare_object_source_ratio_for_storage(ratio_object=json_object) - we do not yet have data to test
            (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_storage(
                issue="Ratio needs to be supported",
                code_string=json_string
            )

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
                name = message_exception_classname(e)
                if name == "BadDataError":
                    name = "invalid JSON for CodeableConcept"
                (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_storage(
                    issue=name,
                    code_string=json_string
                )

        # unsupported FHIR resource type, or an unexpected data dictionary
        else:
            (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_storage(
                issue="JSON is an unsupported FHIR resource type, or an unexpected data dictionary",
                code_string=json_string
            )

    except Exception as e:
        name = message_exception_classname(e)
        if name == "JSONDecodeError":
            name = "invalid JSON for json.loads()"
        (code_schema, code_simple, code_jsonb, code_string) = prepare_string_format_issue_for_storage(
            issue=name,
            code_string=json_string
        )

    return code_schema, code_simple, code_jsonb, code_string, display_string


def prepare_object_source_ratio_for_storage(code_object) -> (str, str, str, str):
    """
    @throws BadDataError if the value does not match the sourceRatio schema
    """
    rcdm_object = normalize_source_ratio(code_object)
    rcdm_string = serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_RATIO.value,
        None,
        escape_sql_input_value(rcdm_string),
        rcdm_string
    )


def prepare_object_source_code_for_storage(code_object) -> (str, str, str, str):
    """
    @raise BadDataError if the value does not match the sourceCodeableConcept schema
    """
    rcdm_object = normalize_source_codeable_concept(code_object)
    rcdm_string = serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value,
        None,
        escape_sql_input_value(rcdm_string),
        rcdm_string
    )


def prepare_additional_data_for_storage(additional_data, rejected_depends_on_value: str = None):

    # todo: This block is intended for use only during database migration to v5. After migration it should be deleted.
    if additional_data is None or len(additional_data) == 0 or additional_data == "{}" or additional_data == "null":
        return None
    if rejected_depends_on_value is not None:
        warning = [
            "The depends_on data format provided with this code is NOT SAFE for clinical use",
            "Do not attempt to interpret or use these values",
            "Each may have ANY ORDER and ANY MEANING"
        ]
        additional_data.update(
            {
                "warning_unsafe_rejected_data": " - ".join(warning),
                "rejected_data_do_not_use": rejected_depends_on_value
            }
        )

    # The line below is the only line needed in prepare_additional_data_for_storage() after migration to v5 is complete.
    # todo: After migration, keep the line below, and REMOVE the rejected_depends_on_value argument from this function.
    return prepare_data_dictionary_for_storage(additional_data)


def prepare_data_dictionary_for_storage(info_dict):
    """
    Serialize a python data dictionary for a string valued column - like additional_data for INFX internal use only
    @return the string, or None if the dictionary was empty
    """
    if info_dict is None or len(info_dict) == 0:
        return None
    info_string = json.dumps(info_dict)
    clean_info = cleanup_json_string(info_string)
    sql_escaped = escape_sql_input_value(clean_info)
    if len(sql_escaped) == 0:
        return None
    return sql_escaped


def prepare_string_depends_on_for_storage(code_string: str) -> (str, str, str, str):
    if code_string is None or code_string == "":
        return (
            None,
            None,
            None,
            None
        )
    else:
        return (
            "string",
            code_string,
            None,
            code_string
        )


def prepare_string_source_code_for_storage(code_string: str) -> (str, str, str, str):
    return (
        "code",
        code_string,
        None,
        code_string
    )


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
    code_clean = cleanup_json_string(code_string)
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
    if is_json_format(input_string):
        try:
            json_object = load_json_string(input_string)
            component = json_object.get("component")
            if component is None:
                codeable_concept = json_object.get("valueCodeableConcept")
                if codeable_concept is not None:
                    output_string = serialize_json_object(codeable_concept)
            else:
                if isinstance(component, list):
                    # It is only safe to grab the valueCodeableConcept for this repair if it is single
                    if len(component) == 1:
                        for concept in component:
                            codeable_concept = concept.get("valueCodeableConcept")
                            if codeable_concept is not None:
                                output_string = serialize_json_object(codeable_concept)
                else:
                    codeable_concept = component.get("valueCodeableConcept")
                    if codeable_concept is not None:
                        output_string = serialize_json_object(codeable_concept)
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
    if is_json_format(input_string):
        try:
            # Handle cases based on the real data: a list of unsafe strength data from spark, a CodeableConcept, or text
            json_object = load_json_string(input_string)
            if isinstance(json_object, list):
                # Came from a spark list of unsafe Medication.ingredient.strength values with mixed orders of num/denom
                return None, input_string
            else:
                # A single CodeableConcept value as a serialized string, not a list
                return input_string, None
        except Exception as e:
            pass
    if is_spark_format:
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
        This function calls normalize_source_codeable_concept() and serialize_json_object() to return an RCDM model
        compliant sourceCodeableConcept serialized as a string with correctly ordered JSON keys and list members.
        This output is suitable as input to id_helper.py functions as a code_string to create a code_id or mapping_id.
    """
    code_string = convert_source_concept_spark_export_string_to_json_string_unordered(spark_export_string)
    code_object = load_json_string(code_string)
    rcdm_object = normalize_source_codeable_concept(code_object)
    rcdm_string = serialize_json_object(rcdm_object)
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
    return serialize_json_object(json_object)
