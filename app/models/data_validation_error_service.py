import requests
from decouple import config
from requests.exceptions import HTTPError
import requests


def get_access_token():
    base_url = config("auth0_url")
    payload = {
        "grant_type": "client_credentials",
        "client_id": config("auth0_client_id"),
        "client_secret": config("auth0_client_secret"),
        "audience": config("auth0_audience"),
    }
    response = requests.post(base_url, data=payload)
    return response.json()["access_token"]


def get_resource_from_service():
    """
    This will be the first part of the infx error ingestion. Getting the errors.
    @return:
    """
    # get token
    access_token = get_access_token()
    # Add the token to the Authorization header of the request
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    # Get resources using the token
    resources = requests.get(
        "https://interop-validation.prod.projectronin.io/resources",
        # this end point has parameters to filter by status, organization_id and/or resource_type
        headers=headers,
    )

    return resources.json()


def filter_ids_resource_severity_failed():
    """
    This function filters the errors down to severity = failed
    @return:
    """
    resources = get_resource_from_service()
    severity_list = []
    for item in resources:
        if item["severity"] == "failed":
            severity_list.append(item["id"])
    return severity_list


def get_code_issues():
    """
    This function filters the severity = failed list down to the error type RONIN_NOV_CODING_001
    @return:
    """
    severity_list = filter_ids_resource_severity_failed()
    issues_with_code_errors = []
    for resource_id in severity_list:
        # get token
        access_token = get_access_token()
        # Add the token to the Authorization header of the request
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(
            f"https://interop-validation.prod.projectronin.io/resources/{resource_id}/issues",
            headers=headers,
        )
        issues = response.json()
        for issue in issues:
            if issue["type"] == "RONIN_NOV_CODING_001":
                issues_with_code_errors.append(issue)
    return issues_with_code_errors


if __name__ == "__main__":
    get_resource_from_service()
