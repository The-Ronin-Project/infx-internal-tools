import datetime
import json
import re
from enum import Enum

from app.helpers.data_helper import is_uuid_format, is_spark_format, is_json_format, load_json_string, \
    normalize_source_ratio, serialize_json_object, escape_for_sql, normalize_source_codeable_concept, \
    cleanup_json_string
from app.helpers.message_helper import message_exception_classname


class DataExtensionUrl(Enum):
    """
    DataExtensionUrls for ConceptMaps from the RCDM specification
    """
    SOURCE_CODEABLE_CONCEPT = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceCodeableConcept"
    SOURCE_RATIO = "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMapSourceRatio"


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


def prepare_dynamic_value_for_sql(old_value: str, old_display: str = None) -> (str, str, str, str):
    """
    For inserting a code value into a table that has already been migrated to the ConceptMap v5 schema.

    Calls the helper function prepare_dynamic_value_for_sql_issue() to process the inputs.

    @param old_value - may be a string, or may be a FHIR code which as data is a string value
    @param old_display - IMPORTANT - caller must supply this value if the old_value is a FHIR code - used for some tests
    @raise ValueError if the old_value cannot be correctly stored, so the caller should not insert it into the database.
    """
    (value_schema, value_simple, value_jsonb, value_string) = prepare_dynamic_value_for_sql(old_value, old_display)
    if value_schema is None:
        raise ValueError("Value cannot be stored: format issues found")
    if IssuePrefix.COLUMN_VALUE_FORMAT.value in value_schema:
        raise ValueError(value_schema)
    return value_schema, value_simple, value_jsonb, value_string


def prepare_dynamic_value_for_sql_issue(old_value: str, old_display: str = None) -> (str, str, str, str):
    """
    Converts 1 input string value into 4 strings to use in forming correct syntax for a subsequent SQL INSERT query.
    This function is intended to support multiple data types for FHIR dynamic values: code, CodeableConcept, Ratio, etc.

    For CodeableConcept only, old_display (the associated display value) may be provided and an extra error check
    happens. For other FHIR dynamic value data types, the old_display is ignored.

    @param old_value - may be a string, or may be a FHIR code which as data is a string value
    @param old_display - IMPORTANT - caller must supply this value if the old_value is a FHIR code - used for some tests

    @return
        value_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        value_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        value_jsonb (string) - None if primitive or old_value has "format issue:" - else, object with that value_schema
            - note: because this function prepares these values for SQL INSERT query formation, this value is a string -
            all JSON uses double quotes, and if there is a single quote value in the JSON, it is '' for SQL escaping
        value_string (string) - either value_simple, or value_jsonb binary JSON value, correctly serialized to a string
            - this returned value_string value is ready for the  caller to input to hash_string to create the code_id
    """
    # init
    value_schema = None
    value_simple = None
    value_jsonb = None
    value_string = None

    # unwrap any wrappers in the JSON
    if old_value is not None and '"valueCodeableConcept"' in old_value:
        old_value = remove_deprecated_wrapper(old_value)

    # begin: detect code issue cases
    # handle None (bad)
    if old_value is None:
        (value_schema, value_simple, value_jsonb, value_string) = prepare_code_none_issue_for_sql(
            issue="value is None",
            id="value is null"
        )

    # handle '' (bad if this is a code; fine for string)
    elif old_display is not None and old_value == "":
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="value is ''",
            code_string=old_value
        )

    # handle uuid case (bad)
    elif is_uuid_format(old_value):
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="uuid",
            code_string=old_value
        )

    # handle '[]' (bad)
    elif old_value == "[]":
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="value is '[]'",
            code_string=old_value
        )

    # handle '{}' (bad)
    elif old_value == "{}":
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="value is " + "'{}'",
            code_string=old_value
        )

    # handle '[null]' (bad)
    elif old_value == "[null]":
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="value is '[null]'",
            code_string=old_value
        )

    # handle 'null' (bad)
    elif old_value == "null":
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue="value is 'null'",
            code_string=old_value
        )

    # handle {"text":"Line 1"} spark or JSON (bad)
    elif "{Line 1}" in old_value or ('"text"' in old_value and '"Line 1"' in old_value):
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
            issue='{"text":"Line 1"}',
            code_string=old_value
        )

    # handle coding not present in CodeableConcept (bad)
    elif '"text"' in old_value and '"coding"' not in old_value:
        (value_schema, value_simple, value_jsonb,value_string) = prepare_string_format_issue_for_sql(
            issue="CodeableConcept with only text",
            code_string=old_value
        )

    # handle spark case (bad)
    elif is_spark_format(old_value):
        # try to convert to json
        json_string = convert_source_concept_spark_export_string_to_json_string(old_value)
        # failed to convert to json: report format issue
        if json_string == old_value:
            (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
                issue="spark",
                code_string=old_value
            )
        # converted to json: handle the json
        else:
            (value_schema, value_simple, value_jsonb, value_string) = prepare_json_format_for_sql(json_string)

    # handle json case (good or bad)
    elif is_json_format(old_value):
        (value_schema, value_simple, value_jsonb, value_string) = prepare_json_format_for_sql(old_value)

    # see if code and display columns were reversed
    elif old_display is not None and is_code_display_reversed(old_value, old_display):
        if value_schema is not None and len(value_schema) > 0:
            value_schema += ", code and display might be reversed"
        else:
            (value_schema, value_simple, value_jsonb, value_string) = prepare_string_format_issue_for_sql(
                issue="code and display might be reversed",
                code_string=old_value
            )

    # string case (good) - if there is no old_display supplied by the caller, it is not a code: it is a depends_on
    elif old_display is None:
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_depends_on_for_sql(
            code_string=old_value
        )

    # handle code case (good)
    else:
        (value_schema, value_simple, value_jsonb, value_string) = prepare_string_source_code_for_sql(
            code_string=old_value
        )
    # end: detect code issue cases

    return value_schema, value_simple, value_jsonb, value_string


def prepare_json_format_for_sql(json_string: str) -> (str, str, str, str):
    """
    Normalizes the JSON string to an expected format with double quotes and no wasted space - calls helper functions
    in the correct order: load the object, normalize to RCDM (sorts lists), serialize (sorts keys, strips space)
    @return
        code_schema (string) - FHIR primitive name, DataExtensionUrls value, or message prefixed with "format issue:"
        code_simple (string) - old_code if primitive or old_code has "format issue: " - else None
        code_jsonb (string) - None if primitive or old_code has "format issue:" - else, the object with that code_schema
            - note: because this function prepares these values for SQL INSERT query formation, this value is a string -
            all JSON uses double quotes, and if there is a single quote value in the JSON, it is '' for SQL escaping
        code_string (string) - either code_simple, or the code_jsonb binary JSON value correctly serialized to a string
    """
    try:
        json_object = load_json_string(json_string)

        # Ratio
        if '"numerator"' in json_string or '"denominator"' in json_string:
            # todo: prepare_object_source_ratio_for_sql(ratio_object=json_object) - not yet, questions out to Content
            (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_sql(
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
                (code_schema, code_simple, code_jsonb, code_string) = prepare_object_source_code_for_sql(
                    code_object=json_object
                )
            except Exception as e:
                name = message_exception_classname(e)
                if name == "BadDataError":
                    name = "invalid JSON for CodeableConcept"
                (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_sql(
                    issue=name,
                    code_string=json_string
                )

        # unsupported FHIR resource type, or an unexpected data dictionary
        else:
            (code_schema, code_simple, code_jsonb, code_string) = prepare_object_format_issue_for_sql(
                issue="JSON is an unsupported FHIR resource type, or an unexpected data dictionary",
                code_string=json_string
            )

    except Exception as e:
        name = message_exception_classname(e)
        if name == "JSONDecodeError":
            name = "invalid JSON for json.loads()"
        (code_schema, code_simple, code_jsonb, code_string) = prepare_string_format_issue_for_sql(
            issue=name,
            code_string=json_string
        )

    return code_schema, code_simple, code_jsonb, code_string


def prepare_object_source_ratio_for_sql(code_object) -> (str, str, str, str):
    """
    @throws BadDataError if the value does not match the sourceRatio schema
    """
    rcdm_object = normalize_source_ratio(code_object)
    rcdm_string = serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_RATIO.value,
        None,
        escape_for_sql(rcdm_string),
        rcdm_string
    )


def prepare_object_source_code_for_sql(code_object) -> (str, str, str, str):
    """
    @throws BadDataError if the value does not match the sourceCodeableConcept schema
    """
    rcdm_object = normalize_source_codeable_concept(code_object)
    rcdm_string = serialize_json_object(rcdm_object)
    return (
        DataExtensionUrl.SOURCE_CODEABLE_CONCEPT.value,
        None,
        escape_for_sql(rcdm_string),
        rcdm_string
    )


def prepare_data_dictionary_for_sql(info_dict) -> str:
    """
    Serialize a python data dictionary for a string valued column - like additional_data for INFX internal use only
    """
    if info_dict is None:
        return None
    info_string = json.dumps(info_dict)
    clean_info = cleanup_json_string(info_string)
    sql_escaped = escape_for_sql(clean_info)
    return sql_escaped


def prepare_string_depends_on_for_sql(code_string: str) -> (str, str, str, str):
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


def prepare_string_source_code_for_sql(code_string: str) -> (str, str, str, str):
    return (
        "code",
        code_string,
        None,
        code_string
    )


def prepare_code_none_issue_for_sql(issue: str, id: str) -> (str, str, str, str):
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        None,
        None,
        id
    )


def prepare_string_format_issue_for_sql(issue: str, code_string: str) -> (str, str, str, str):
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        code_string,
        None,
        code_string
    )


def append_string_format_issue_for_sql(issue: str, schema: str) -> str:
    if schema is None or len(schema) == 0:
        return f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}"
    else:
        return schema + f", {issue}"


def prepare_object_format_issue_for_sql(issue: str, code_string: str) -> (str, str, str, str):
    code_clean = cleanup_json_string(code_string)
    return (
        f"{IssuePrefix.COLUMN_VALUE_FORMAT.value}{issue}",
        code_clean,
        None,
        code_clean
    )


def remove_deprecated_wrapper(input_string: str) -> str:
    """
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


def is_code_display_reversed(code_string: str, display_string: str):
    """
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


def convert_source_concept_spark_export_string_to_json_string(spark_export_string: str) -> str:
    """
    @raise exception if parsing hits any snags, which it should not
    """
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
