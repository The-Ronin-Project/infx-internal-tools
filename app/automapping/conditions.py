import app.models.codes
import app.automapping.models
import requests
from typing import Optional
import json
from sqlalchemy import create_engine, text

API_BASE_URL = "https://snowstorm.prod.projectronin.io"
# API_ACCESS_KEY = "your_api_access_key"

SYNONYMS = {
    "tumor": "neoplasm",
    "secondary": "metastatic",
}

CONDITIONS_SYSTEM = "http://snomed.info/sct"
CONDITIONS_SYSTEM_VERSION = "2023-03-01"


def normalize_synonyms(text):
    """
    Replaces the words in the input text with their synonyms if they exist in the SYNONYMS dictionary.

    Args:
        text (str): The input text containing words to be replaced with synonyms.

    Returns:
        str: The text with synonyms replaced if they exist in the SYNONYMS dictionary.
    """
    words = text.split()
    normalized_words = [SYNONYMS.get(word.lower(), word) for word in words]
    return " ".join(normalized_words)


def get_concept_data(concept_id, branch="MAIN/2023-03-01"):
    """
    Fetches the preferred term, fully specified name, and status for the given concept ID
    using the SNOMED API.

    Args:
        concept_id (str): The SNOMED concept ID to fetch data for.
        branch (str): The branch of the concepts repository (default: "MAIN").

    Returns:
        tuple: A tuple containing the preferred term, fully specified name, and status of the concept.
    """
    url = f"{API_BASE_URL}/{branch}/concepts/{concept_id}"
    response = requests.get(url)

    if response.status_code == 200:
        concept_data = response.json()
        preferred_term = concept_data["pt"]["term"]
        fully_specified_name = concept_data["fsn"]["term"]
        is_active = concept_data["active"]
        return preferred_term, fully_specified_name, is_active
    else:
        print(
            f"Error fetching data for concept ID {concept_id}: {response.status_code}"
        )
        return None, None, None


def get_concept_descriptions(concept_id, branch="MAIN/2023-03-01"):
    """
    Fetches the descriptions for the given concept ID using the SNOMED API.

    Args:
        concept_id (str): The SNOMED concept ID to fetch descriptions for.
        branch (str): The branch of the concepts repository (default: "MAIN").

    Returns:
        list: A list of description terms for the given concept ID.
    """
    url = f"{API_BASE_URL}/{branch}/concepts/{concept_id}/descriptions"
    headers = {"Accept-Language": "en-X-900000000000509007"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        descriptions_data = response.json()
        descriptions = descriptions_data["conceptDescriptions"]
        filtered_terms = [
            desc["term"]
            for desc in descriptions
            if any(
                value in {"ACCEPTABLE", "PREFERRED"}
                for value in desc["acceptabilityMap"].values()
            )
        ]
        return filtered_terms
    else:
        print(
            f"Error fetching descriptions for concept ID {concept_id}: {response.status_code}"
        )
        return None


# def do_mapping(source_concept_uuid, code, display, matched_reason):
#     """
#     Maps the input SNOMED code and display to its corresponding concept data, including the preferred term,
#     fully specified name, and status. If there's a match, the function returns the concept data, otherwise,
#     it returns an error message.
#
#     Args:
#         code (str): The SNOMED code to be mapped.
#         display (str): The display text associated with the SNOMED code.
#
#     Returns:
#         tuple: A tuple containing the SNOMED code, display text, preferred term, fully specified name,
#                status, and a flag indicating if the code was successfully matched.
#     """
#     # Assign mapping row to Automapping user
#     # todo: implement this
#     # Submit mapping
#
#     result = requests.post(
#         "http://infx-internal-ds.prod.projectronin.io/mappings/",
#         json={
#             "source_concept_uuid": str(source_concept_uuid),
#             "relationship_code_uuid": "f2a20235-bd9d-4f6a-8e78-b3f41f97d07f",
#             "target_concept_code": code,
#             "target_concept_display": display,
#             "target_concept_terminology_version_uuid": "306ae926-50aa-41d1-8ec8-1df123b0cd77",
#             "mapping_comments": f"{matched_reason} match",
#             "author": "Automapping",
#         },
#     )
#     if result.status_code == 200:
#         print(
#             "Mapped:",
#             code,
#             "|",
#             display,
#             matched_reason,
#             "to source concept:",
#             source_concept_uuid,
#         )
#         assignment_update = requests.patch(
#             f"http://infx-internal-ds.prod.projectronin.io/SourceConcepts/{source_concept_uuid}",
#             json={
#                 "assigned_mapper": "8990714d-8eeb-4acf-a5b7-abf92007a53a"  # Automap user
#             },
#         )
#         if assignment_update.status_code == 200:
#             print("Updated assignment to automap user")
#         else:
#             print(
#                 "Failed to update assignment for source concept with uuid",
#                 source_concept_uuid,
#             )
#     else:
#         print(result)
#
#     # TODO


def resolve_inactive_concept_automapping(
    inactive_matched_code, branch="MAIN/2023-03-01"
):
    # Here's a sample code to use: 255102004
    # inactive_matched_code is referencedComponentId

    # We only want to use the following ref sets
    # 900000000000526001 | REPLACED BY association reference set| (this one will need additional check: we only want it if its replaced by one thing)
    # 900000000000527005 | SAME AS association reference set|
    reference_set_replaced_by = "900000000000526001"
    reference_set_same_as = "900000000000527005"

    target_component_id = None
    fsn_for_matched_code = None
    matched_reason = None

    # Look and see if there is a a 'REPLACED BY' for this code
    url_replaced_by = f"{API_BASE_URL}/{branch}/members?referencedComponentId={inactive_matched_code}&referenceSet={reference_set_replaced_by}&active=true"
    response_replaced_by = requests.get(url_replaced_by)
    if response_replaced_by.status_code == 200:
        data = response_replaced_by.json()
        # target_component_id is the new id to do the mapping with
        if len(data["items"]) == 1:
            target_component_id = data["items"][0]["additionalFields"][
                "targetComponentId"
            ]
            # fsn_for_matched_code = data['items'][0]['referencedComponent']['fsn']['term']
            _, fsn_for_matched_code, _ = get_concept_data(target_component_id)
            matched_reason = "REPLACED BY"

        elif len(["items"]) > 1:
            pass
            # do manual mapping
            # im thinking for now we could just call this from within the big funciton unless we wanna change it
        else:
            # Look and see if there is a a 'SAME AS' for this code
            url_same_as = f"{API_BASE_URL}/{branch}/members?referencedComponentId={inactive_matched_code}&referenceSet={reference_set_same_as}&active=true"
            response_same_as = requests.get(url_same_as)
            # same as above to check and then do the mapping
            if response_same_as.status_code == 200:
                data = response_same_as.json()
                if len(data["items"]) == 1:
                    target_component_id = data["items"][0]["additionalFields"][
                        "targetComponentId"
                    ]
                    # fsn_for_matched_code = data['items'][0]['referencedComponent']['fsn']['term']
                    _, fsn_for_matched_code, _ = get_concept_data(target_component_id)
                    matched_reason = "SAME AS"
        # This returns what we input into the do_mapping function
        return target_component_id, fsn_for_matched_code, matched_reason

        # If we can't resolve, update the comments to indicate this
        # response = requests.patch(
        #     f'http://infx-internal-ds.prod.projectronin.io/SourceConcepts/{item.get("source_concept_uuid")}',
        #     json={
        #         'comments': f'Would auto-map to inactive concept {matched_code}'
        #     }
        # )
        # # print("NO MATCH", input_display)
        # if response.status_code == 200:
        #     messages.append(f"Inactive Concept {input_display}")
        # else:
        #     print(response)


def automap_condition(
    input_codeable_concept: app.models.codes.CodeableConcept,
) -> Optional[app.automapping.models.AutomapDecision]:
    # todo: this is where the main logic goes
    # See if there is a SNOMED code in the CodeableConcept.coding, if there is none, return None.

    ignorable_strings = [
        ", NOS",
        ", not otherwise specified",
        "O/E - ",
        "O/E ",
        "O/C - ",
        "On examination - ",
        ", unspecified site",
        "<Unspecified site>",
        ", site unspecified",
        ", unspecified",
        ", initial",
        ", subsequent",
        " <initial>",
        " <subsequent>",
        " <Initial>",
        " <Subsequent>",
        " <Unspecified side>",
        "<Unspecified side>",
        ", initial encounter",
        ", subsequent encounter",
        " <initial encounter>",
        " <subsequent encounter>",
    ]
    messages = []

    coding = input_codeable_concept.coding
    input_display = input_codeable_concept.text

    # Isolate the SNOMED code (if present) from the coding array
    snomed_codes = [code for code in coding if code.system == CONDITIONS_SYSTEM]
    if len(snomed_codes) > 1:
        return None  # We won't handle multiple SNOMED codes
    if len(snomed_codes) == 0:
        return None  # We can't handle no SNOMED codes
    code = snomed_codes[0].code

    # Handle the data where the SNOMED code is literally 0
    if code == "0":
        return None

    preferred_term, fully_specified_name, is_active = get_concept_data(code)
    if (preferred_term, fully_specified_name, is_active) == (None, None, None):
        return None  # todo: figure why this is here
    concept_descriptions = get_concept_descriptions(code)
    if not concept_descriptions:
        return None  # todo: figure why this is here

    matched_code = None
    # fsn_for_matched_code = None
    # matched_reason = None
    automapping_decision = None

    if input_display == preferred_term or input_display == fully_specified_name:
        automapping_decision = app.automapping.models.AutomapDecision(
            target_code=app.models.codes.Code(
                code=code,
                display=fully_specified_name,
                system=CONDITIONS_SYSTEM,
                version=CONDITIONS_SYSTEM_VERSION,  # todo: will need to dynamically use the latest version
            ),
            reason="EXACT",
        )
    else:
        normalized_input_display = normalize_synonyms(input_display)
        descriptions_to_check = [
            preferred_term,
            fully_specified_name,
        ] + concept_descriptions
        if any(
            normalized_input_display == description
            for description in descriptions_to_check
        ):
            automapping_decision = app.automapping.models.AutomapDecision(
                target_code=app.models.codes.Code(
                    code=code,
                    display=get_concept_data(code)[1],
                    system=CONDITIONS_SYSTEM,
                    version=CONDITIONS_SYSTEM_VERSION,
                ),
                reason="SYNONYM",
            )
        else:
            modified_display = normalized_input_display
            for string in ignorable_strings:
                modified_display = modified_display.replace(string, "")

            if any(
                modified_display == description for description in descriptions_to_check
            ):
                automapping_decision = app.automapping.models.AutomapDecision(
                    target_code=app.models.codes.Code(
                        code=code,
                        display=get_concept_data(code)[1],
                        system=CONDITIONS_SYSTEM,
                        version=CONDITIONS_SYSTEM_VERSION,
                    ),
                    reason="NORMALIZED DESCRIPTION",
                )

    # # else: # This is for if the concept is an inactive SNOMED concept
    if not is_active:  # JUST FOR INACTIVE
        if code:
            code, display, matched_reason = resolve_inactive_concept_automapping(code)
            if code is not None:
                automapping_decision = app.automapping.models.AutomapDecision(
                    target_code=app.models.codes.Code(
                        code=code,
                        display=get_concept_data(code)[1],
                        system=CONDITIONS_SYSTEM,
                        version=CONDITIONS_SYSTEM_VERSION,
                    ),
                    reason="REPLACED_INACTIVE",
                )

    return automapping_decision
