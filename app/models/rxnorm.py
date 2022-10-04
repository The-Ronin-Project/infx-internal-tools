import requests

RX_NAV_BASE_URL = "https://rxnav.prod.projectronin.io/REST"


def exact_search(query_string):
    # Check if input is an RxCUI (numeric code) already
    if query_string.isdigit():
        rxcui = query_string
    else:
        rxcui = None

    # If not, use getRxCUIbyString endpoint to find an exact match
    if rxcui is None:
        rxcui_request = requests.get(
            f"{RX_NAV_BASE_URL}/rxcui.json",
            params={"name": query_string, "allsrc": "0", "search": "2"},
        )
        if rxcui_request.json():
            if rxcui_request.json().get("idGroup"):
                id_group = rxcui_request.json().get("idGroup")
                if "rxnormId" in id_group:
                    rxcui = id_group.get("rxnormId")[0]

    if rxcui is None:
        return None, None

    # get all related info and return results
    related_info = requests.get(
        f"{RX_NAV_BASE_URL}/rxcui/{rxcui}/allrelated.json"
    ).json()
    concept_group = related_info.get("allRelatedGroup").get("conceptGroup")
    concept_group_by_tty = {
        x.get("tty"): x.get("conceptProperties") for x in concept_group
    }
    return concept_group_by_tty, rxcui


def approx_search(query_string):
    approx_search_json = requests.get(
        f"{RX_NAV_BASE_URL}/approximateTerm.json",
        params={"term": query_string, "maxEntries": 20},
    ).json()

    rxcuis = [
        x.get("rxcui")
        for x in approx_search_json.get("approximateGroup").get("candidate")
    ]
    top_rxcui = rxcuis[0]
    rxcuis = list(set(rxcuis))

    final_results = {
        "SCD": [],  # Make sure the term types we care about always return
        "SBD": [],
        "GPCK": [],
        "BPCK": [],
    }
    for rxcui in rxcuis:
        info_request = requests.get(
            f"{RX_NAV_BASE_URL}/RxTerms/rxcui/{rxcui}/allinfo.json"
        ).json()
        if info_request:
            info_json = info_request.get("rxtermsProperties")
            info_json["name"] = info_json.get("fullName")
            term_type = info_json.get("termType")

            if term_type in final_results:
                final_results[term_type].append(info_json)
            else:
                final_results[term_type] = [info_json]

    return final_results, top_rxcui


def exact_with_approx_fallback_search(query_string):
    exact_results, top_rxcui = exact_search(query_string)

    if exact_results is not None:
        results = exact_results
        search_type = "EXACT"
    else:
        results, top_rxcui = approx_search(query_string)
        search_type = "APPROX"

    return {"search_type": search_type, "top_rxcui": top_rxcui, "results": results}


if __name__ == "__main__":
    print(exact_with_approx_fallback_search("ibuprofen 100 mg/5 ml oral suspension"))
    print(
        exact_with_approx_fallback_search(
            "ibuprofen 100 mg/5 ml oral suspension 600 mg"
        )
    )
