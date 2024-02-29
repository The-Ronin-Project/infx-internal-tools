import hashlib
import json

import uuid
from collections import OrderedDict

from app.errors import BadDataError

OID_URL_CONVERSIONS = {
    "urn:oid:2.16.840.1.113883.6.96": "http://snomed.info/sct",
    "urn:oid:2.16.840.1.113883.6.88": "http://www.nlm.nih.gov/research/umls/rxnorm",
    "urn:oid:2.16.840.1.113883.6.1": "http://loinc.org",
    "urn:oid:2.16.840.1.113883.6.8": "http://unitsofmeasure.org",
    "urn:oid:2.16.840.1.113883.3.26.1.2": "http://ncimeta.nci.nih.gov",
    "urn:oid:2.16.840.1.113883.6.12": "http://www.ama-assn.org/go/cpt",
    "urn:oid:2.16.840.1.113883.6.209": "http://hl7.org/fhir/ndfrt",
    "urn:oid:2.16.840.1.113883.4.9": "http://fdasis.nlm.nih.gov",
    "urn:oid:2.16.840.1.113883.6.69": "http://hl7.org/fhir/sid/ndc",
    "urn:oid:2.16.840.1.113883.12.292": "http://hl7.org/fhir/sid/cvx",
    "urn:oid:1.0.3166.1.2.2": "urn:iso:std:iso:3166",
    "urn:oid:2.16.840.1.113883.6.344": "http://hl7.org/fhir/sid/dsm5",
    "urn:oid:2.16.840.1.113883.6.301.5": "http://www.nubc.org/patient-discharge",
    "urn:oid:2.16.840.1.113883.6.256": "http://www.radlex.org",
    "urn:oid:2.16.840.1.113883.6.3": "http://hl7.org/fhir/sid/icd-10",
    "urn:oid:2.16.840.1.113883.6.42": "http://hl7.org/fhir/sid/icd-9-cm",
    "urn:oid:2.16.840.1.113883.6.90": "http://hl7.org/fhir/sid/icd-10-cm",
    "urn:oid:2.16.840.1.113883.2.4.4.31.1": "http://hl7.org/fhir/sid/icpc-1",
    "urn:oid:2.16.840.1.113883.6.139": "http://hl7.org/fhir/sid/icpc-2",
    "urn:oid:2.16.840.1.113883.6.254": "http://hl7.org/fhir/sid/icf-nl",
    "urn:oid:1.3.160": "https://www.gs1.org/gtin",
    "urn:oid:2.16.840.1.113883.6.73": "http://www.whocc.no/atc",
    "urn:oid:2.16.840.1.113883.6.24": "urn:iso:std:iso:11073:10101",
    "urn:oid:1.2.840.10008.2.16.4": "http://dicom.nema.org/resources/ontology/DCM",
    "urn:oid:2.16.840.1.113883.5.1105": "http://hl7.org/fhir/NamingSystem/ca-hc-din",
    "urn:oid:2.16.840.1.113883.6.101": "http://nucc.org/provider-taxonomy",
    "urn:oid:2.16.840.1.113883.6.14": "https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets",
    "urn:oid:2.16.840.1.113883.6.43.1": "http://terminology.hl7.org/CodeSystem/icd-o-3",
    "urn:oid:2.16.840.1.113883.4.1": "http://hl7.org/fhir/sid/us-ssn",
    "urn:oid:2.16.840.1.113883.4.6": "http://hl7.org/fhir/sid/us-npi",
    "urn:oid:2.16.840.1.113883.4.7": "http://hl7.org/fhir",
}


def load_json_string(input_json_string: str):
    """
    The standard JSON deserialization function.

    Do not use json.loads() by itself. This function accommodates edge cases in our data and then calls json.loads().
    """
    output_object = None
    try:
        # temporary fix to '' issue while outputting JSON samples for internal review
        if "''" in input_json_string:
            input_json_string = input_json_string.replace("''", "'")

        # normal case
        output_object = json.loads(input_json_string)
    except json.decoder.JSONDecodeError as e:
        if "Invalid \escape" in str(e):
            try:
                # the rare str formation "micr.:leukocytes present" can be mistaken for SQL binding syntax
                output_object = json.loads(unescape_sql_jsonb_value(input_json_string))
            except Exception as e:
                raise e
        elif "Expecting value" in str(e):
            raise ValueError("A code string value was input where a CodeableConcept object value was expected.")
    except Exception as e:
        raise e
    return output_object


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

    @return (str) - a serialized JSON string, or "" (an empty string) if the input json_object was None
    """
    from app.models.codes import FHIRCodeableConcept
    # Avoid returning the str value "null" which is what json.dumps() returns for a None json_object
    if json_object is None:
        return ""
    if type(json_object) is FHIRCodeableConcept:
        input = json_object.serialize()
    else:
        input = json_object
    return json.dumps(input, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


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
    A convenience function for steps in JSON string re-formatting.

    A function to put any JSON serialized string into a consistent format and consistent attribute order.
    This function does NOT address list member order which is a critical issue in INFX Systems processing.
    To ensure full consistency in JSON strings, do not use this function; instead use one of these sequences:

    1. Ordinary JSON:
    Call load_json_string(),
    call order_object_list() on any lists, then
    call serialize_json_object().

    2. RCDM profile JSON:
    Call load_json_string(),
    provide and call a normalization function like normalized_source_codeable_concept() to prune the object attributes,
    call order_object_list() on any lists, then
    call serialize_json_object().

    @return a serialized JSON string, or None if the input string was None.
    """
    json_object = load_json_string(input_json_string)
    return serialize_json_object(json_object)


def hash_string(input_string: str) -> str:
    """
    For all hashed id values - like mapping_id and code_id - caller is responsible for a correctly formed input_string.
    @return a serialized JSON string, or "" (an empty string) if the input string was None.
    """
    if input_string is None:
        return ""
    # create a new md5 hash object
    hash_object = hashlib.md5()
    # update the hash object with the bytes-like object
    hash_object.update(input_string.encode("utf-8"))
    # return the hexadecimal representation of the hash
    return hash_object.hexdigest()


def hash_jsonb(input_object) -> str:
    """
    Serialize an object to a string and then hash the string
    @return a serialized JSON string, or "" (an empty string) if the input object was None.
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


def unescape_sql_jsonb_value(input_string: str) -> str:
    """
    Handle an edge case around SQL binding, example is the JSON Binary value {"text":"Urine micr.:leukocytes present"}
    is being read from SELECT as a str {"text":"Urine micr.\:leukocytes present"} which breaks JSON Decode of the str.
    """
    return input_string.replace(".\\:", ".:")


def normalized_source_ratio(input_object):
    """
    shape the Ratio attributes as needed to conform to the RCDM profile - remove the unsupported attributes -
    require required attributes
    @return the normalized JSON object, or None if no valid attributes were present in the input_object
    """
    if input_object is None:
        return None
    numerator = input_object.get("numerator")
    denominator = input_object.get("denominator")
    if numerator is None or denominator is None:
        raise BadDataError(
            code="Ratio.schema",
            description="Ratio was expected, but one or more attributes is missing",
            errors="Invalid Ratio"
        )
    normalized_numerator = normalized_quantity(numerator)
    normalized_denominator = normalized_quantity(denominator)
    if normalized_numerator is None or normalized_denominator is None:
        return None
    output_object = {
        "numerator": normalized_numerator,
        "denominator": normalized_denominator
    }
    return output_object


def normalized_quantity(input_object):
    """
    shape Quantity attributes as needed to conform to the RCDM profile - remove the unsupported attributes -
    require required attributes
    @return the normalized JSON object, or None if no valid attributes were present in the input_object
    """
    if input_object is None:
        return None
    output_object = {}
    code = input_object.get("code")
    if code is not None:
        output_object["code"] = code
    system = input_object.get("system")
    if system is not None:
        output_object["system"] = system
    unit = input_object.get("unit")
    if unit is not None:
        output_object["unit"] = unit
    value = input_object.get("value")
    if value is not None:
        output_object["value"] = value
    return output_object if len(output_object) > 0 else None


def normalized_source_codeable_concept(input_object):
    """
    shape the CodeableConcept attributes as needed to conform to the RCDM profile - remove the unsupported attributes -
    require required attributes - calls order_object_list() on "coding" list to ensure members are in consistent order
    """
    from app.models.codes import FHIRCodeableConcept
    if input_object is None:
        return None
    if type(input_object) is FHIRCodeableConcept:
        input = input_object.serialize()
    else:
        input = input_object

    if "coding" not in input and "text" not in input:
        raise BadDataError(
            code="CodeableConcept.schema",
            description="CodeableConcept was expected, but the object has no coding or text attribute",
            errors="Invalid CodeableConcept"
        )
    coding_list = input.get("coding")
    text_value = input.get("text")
    if coding_list is not None:
        for coding in coding_list:
            if coding.get("id") is not None:
                del coding["id"]
            if coding.get("userSelected") is not None:
                del coding["userSelected"]
            system = coding.get("system")
            if system is not None:
                if system in OID_URL_CONVERSIONS:
                    coding["system"] = OID_URL_CONVERSIONS[system]
    output_object = {}
    if coding_list is not None:
        output_object["coding"] = order_object_list(coding_list)
    if text_value is not None:
        output_object["text"] = text_value
    return output_object
    # todo: revise to cover the unlikely edge case that non-standard attributes are in tenant data - see ratio function


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
