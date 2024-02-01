import hashlib
import json

import uuid
from collections import OrderedDict

from app.errors import BadDataError


def load_json_string(input_json_string: str):
    """
    The standard JSON deserialization function.

    json.loads() options:
    No options explicitly provided at present - uses defaults - this is a deliberate decision - defaults are documented.
    """
    return json.loads(input_json_string)


def serialize_json_object(json_object) -> str:
    """
    The standard JSON serialization function. See notes and cautions at cleanup_json_string and order_object_list.

    For now:
    If the json_object contains lists, and you want to force list members into a known, consistent order, do not start
    with this function; first call order_object_list() on any lists, then call serialize_json_object() on that result.

    Todo: someday, make this function recursively auto-detect any list members within the object, at any level, and for
    each list member, call order_object_list() on that list to order the members. This would be much better as a
    top-level function, but for now, the only list we are serializing are the "coding" in aFHIR CodeableConcept.

    json.dumps() options:
    sort_keys=True - sort the keys in the output string alphabetically; respects the current order of list elements
    ensure_ascii=False - ensures that double quotes are used to wrap strings in the output, needed for SQL query format
    separators=(",", ":") - omits space characters from JSON syntax, but retains space characters inside JSON content
    """
    return json.dumps(json_object, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def order_object_list(input_object_list) -> list:
    """
    The standard function to ensure a consistent order for FHIR array members regardless of order in source data.

    See notes and cautions at cleanup_json_string and serialize_json_object.

    Provides sa consistent, standard order for FHIR array members that are objects. Per the FHIR standard, arrays do not
    have any guaranteed order. Per the json library, arrays cannot be given a standard order by dumps() or loads().
    The technique we use is to do a md5 numeric hash of each list member  and sort list members by their hash values.

    The ONLY requirement is to guarantee we always output any identical list members appear in the SAME ORDER in our
    lists, even if they did not arrive in that same order in the data. There is no requirement to ALSO make this order
    VISIBLE or INTUITIVE to humans. We have no way to make the ordering criteria apparent to human viewers, but it works

    Function steps in detail:
    1. serialize and hash each member of the input_list using our own functions (which standardize key order in JSON)
    2. use the hash values as keys in a dictionary with the values being the corresponding input_list member
    3. create a new list from the dictionary values in hash value key order
    Note: for now, ignore the infinite theoretical potential for additional levels of lists within objects within lists:
    will handle when we encounter an RCDM defined model that permits this and is also in a jsonb column in the database.

    @param input_object_list is a list of objects, such as CodeableConcept.coding list
    @return a new list in an order that will be consistent any time the same set of list members are input in any order
    """
    object_list: dict = {}
    for obj in input_object_list:
        object_list.update({hash_jsonb(obj): obj})
    sorted_list = OrderedDict(sorted(object_list.items()))
    return list(sorted_list.values())


def cleanup_json_string(input_json_string: str) -> str:
    """
    A convenience function for steps in JSON string re-formatting. See notes for p.

    A function to put any JSON serialized string into a consistent format and consistent attribute order.
    This function does NOT address list member order which is a critical issue in INFX Systems processing.
    To ensure full consistency in JSON strings, do not use this function; instead use one of these sequences:

    1. Ordinary JSON:
    Call load_json_string(),
    call order_object_list() on any lists, then
    call serialize_json_object().

    2. RCDM profile JSON:
    Call load_json_string(),
    provide and call a normalization function like normalize_source_codeable_concept() to prune the object attributes,
    call order_object_list() on any lists, then
    call serialize_json_object().
    """
    json_object = load_json_string(input_json_string)
    return serialize_json_object(json_object)


def hash_string(input_string: str) -> str:
    """
    For all hashed id values - like mapping_id and code_id - caller is responsible for a correctly formed input_string
    """
    # create a new md5 hash object
    hash_object = hashlib.md5()
    # update the hash object with the bytes-like object
    hash_object.update(input_string.encode("utf-8"))
    # return the hexadecimal representation of the hash
    return hash_object.hexdigest()


def hash_jsonb(input_object) -> str:
    """
    serialize an object to a string and then hash the string
    """
    return hash_string(serialize_json_object(input_object))


def is_json_format(input_string: str) -> bool:
    """
    good-enough check for at least one key-value pair in the object - has worked for >1000000 random values
    """
    clean_string = "".join(input_string.split(" "))
    return (
        contains_brackets(clean_string) or contains_braces(clean_string)
    ) and (
        contains_colon(clean_string)
    ) and ((
        contains_double_quoted_strings(input_string) and '":"' in clean_string
        ) or (
        contains_single_quoted_strings(input_string) and "':'" in clean_string
    ))


def is_spark_format(input_string: str) -> bool:
    """
    good-enough check for spark format - has worked for >1000000 random values
    """
    return (
        contains_braces(input_string) or contains_brackets(input_string)
    ) and (
        contains_alphanumeric(input_string)
    ) and (
        not is_json_format(input_string)
    )


def is_uuid4_format(input_string: str) -> bool:
    """
    check for uuid4 format
    """
    try:
        return uuid.UUID(input_string).version == 4
    except ValueError:
        return False


def escape_sql_input_value(input_string: str) -> str:
    """
    SQL escape single quotes and colons in JSON string content values, so they can be input to INSERT - "Hodgkin's: yes"
    """
    # single quote
    escaped_single_quotes = input_string.replace("'", "''")

    # colon
    colon_content_list = escaped_single_quotes.split('":')
    escaped_colon_content_list = []
    for content in colon_content_list:
        escaped = content.replace(".:", ".\:")
        escaped_colon_content_list.append(escaped)
    escaped_string = '":'.join(escaped_colon_content_list)
    return escaped_string

    # todo:
    # So far, there have been issues handling true/false in the JSON string when we get to SQL. This was a formatting
    # issue for our SQL INSERT query text. Text values in JSON work fine with our query because we wrap them in
    # double quotes, but unwrapped values, e.g. boolean, had sqlalchemy formatting errors. For CodeableConcept this
    # was resolved by RCDM not using userSelected, a boolean. Adding Ratio etc, we should beware numeric and boolean.
    # The split('".') used to find colon characters in content could also be handy to find cases like {"mine":true}


def normalize_source_ratio(input_object):
    """
    shape the Ratio attributes as needed to conform to the RCDM profile - remove the unsupported attributes -
    require required attributes
    """
    # todo: implement
    return input_object


def normalize_source_codeable_concept(input_object):
    """
    shape the CodeableConcept attributes as needed to conform to the RCDM profile - remove the unsupported attributes -
    require required attributes - calls order_object_list() on "coding" list to ensure members are in consistent order
    - ≈
    """
    if input_object.get("id") is not None:
        del input_object["id"]
    coding_list = input_object.get("coding")
    text_value = input_object.get("text")
    if coding_list is None and text_value is None:
        raise BadDataError(
            code="CodeableConcept.schema",
            description="CodeableConcept was expected, but the object has no coding or text attribute at the top level",
            errors="Invalid CodeableConcept"
        )
    if coding_list is not None:
        for coding in coding_list:
            if coding.get("id") is not None:
                del coding["id"]
            if coding.get("userSelected") is not None:
                del coding["userSelected"]
        input_object["coding"] = order_object_list(coding_list)
    return input_object


def contains_double_quoted_strings(input_string: str) -> bool:
    """
    good-enough check for at least 1 pair of double quotes
    """
    return sum(1 for char in input_string if char == '"') >= 2


def contains_single_quoted_strings(input_string: str) -> bool:
    """
    good-enough check for at least 1 pair of single quotes
    """
    return sum(1 for char in input_string if char == "'") >= 2


def contains_colon(input_string: str) -> bool:
    """
    good-enough check for at least 1 colon
    """
    return ":" in input_string


def contains_braces(input_string: str) -> bool:
    """
    good-enough check for curly braces
    """
    return "{" in input_string and "}" in input_string


def contains_brackets(input_string: str) -> bool:
    """
    good-enough check for square brackets
    """
    return "[" in input_string and "]" in input_string


def contains_alphanumeric(input_string: str):
    return any(char.isalnum() for char in input_string)


def get_next_hex_char(hex_char):
    hex_int = int(hex_char, 16)
    hex_int = (hex_int + 1) % 16
    next_hex_char = hex(hex_int)[2:]
    return next_hex_char

