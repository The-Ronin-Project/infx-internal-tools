import json

import requests

import app.helpers.data_helper
import app.helpers.format_helper


def parse_rxnorm_codes_from_source_code(source_code):
    """This step will parse the source code and return an array of RxNorm codes"""
    is_spark_format = app.helpers.data_helper.is_spark_format(source_code)

    if not is_spark_format:
        raise NotImplementedError(
            "Only spark formatted inputs are supported for parsing out RxNorm codes"
        )

    if is_spark_format:
        converted_input = app.helpers.format_helper.convert_source_concept_spark_export_string_to_json_string_unordered(
            source_code
        )
        json_codeable_concept = json.loads(converted_input)

        rxnorm_codes = []
        for coding in json_codeable_concept.get("coding"):
            system = coding.get("system")
            code = coding.get("code")
            if (
                system == "http://www.nlm.nih.gov/research/umls/rxnorm"
                or system == "urn:oid:2.16.840.1.113883.6.88"
            ):
                rxnorm_codes.append(code)

        return rxnorm_codes


def get_rxnorm_data_for_source_code(source_code):
    """
    When we get Medication resources from clients, the codings within the CodeableConcept
    often contain RxNorm codes with only codes and no displays or additional information.

    The content team has historically looked up these displays, as well as reference data
    like term type, manually.

    This endpoint will automate the lookup of that reference data, returning a data structure
    which powers a module in Retool to provide the reference data to the content team and
    speed up mapping.

    Input:
        source_code: string, the exact source code (usually a stringified CodeableConcept) the mapper needs to map

    Returns:
        A list of dictionaries, where each dictionary contains the RxNorm code details ('status', 'rxcui', 'name', 'tty').
    """
    # First, we need to parse the RxNorm codes out of the codeable concept
    rxnorm_codes = parse_rxnorm_codes_from_source_code(source_code)
    # Look up and get details for each RxCUI
    rxnorm_data = []
    obsolete_count = 0

    for code in rxnorm_codes:
        try:
            response = requests.get(
                f"https://rxnav.prod.projectronin.io/REST/rxcui/{code}/historystatus.json"
            )
            if response.status_code == 200:
                data = response.json()
                # Extract necessary information from response
                status = data.get("rxcuiStatusHistory").get("metaData").get("status")
                rxcui = data.get("rxcuiStatusHistory").get("attributes").get("rxcui")
                name = data.get("rxcuiStatusHistory").get("attributes").get("name")
                tty = data.get("rxcuiStatusHistory").get("attributes").get("tty")
                if status == "Obsolete":
                    obsolete_count += 1

                else:
                    rxnorm_data.append(
                        {
                            "status": status,
                            "rxcui": rxcui,
                            "name": name,
                            "tty": tty,
                        }
                    )
            else:
                rxnorm_data.append({"rxnorm_code": code, "error": "API request failed"})
        except Exception as e:
            rxnorm_data.append({"rxnorm_code": code, "error": str(e)})

    return {"rxnorm_info": rxnorm_data, "obsolete_count": obsolete_count}
