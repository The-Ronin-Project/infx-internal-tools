from decouple import config
from requests import Response
from werkzeug.exceptions import BadRequest
import datetime
import json
from flask import g, has_request_context
from werkzeug.exceptions import NotFound
import requests


# Use config() to read values from the .env file
# BASE_URL = config("SIMPLIFIER_BASE_URL")
BASE_URL = config("SIMPLIFIER_BASE_URL", default="https://fhir.simplifier.net")
TOKEN_BASE_URL = config("SIMPLIFIER_TOKEN", default="https://api.simplifier.net/token")


def get_access_token():
    """
    Retrieves an access token for the Simplifier API.

    If called within a Flask request context, the function checks if the access token has already been
    retrieved and stored in the 'g' object. If it has, the function returns the stored access token.
    Otherwise, the function calls the 'authenticate_simplifier()' function to retrieve a new access token
    and stores it in the 'g' object before returning it.

    If called outside of a Flask request context, the function calls the 'authenticate_simplifier()'
    function to retrieve a new access token and returns it.

    :return: An access token for the Simplifier API.
    """

    if has_request_context():
        if "simplifer_access_token" not in g:
            access_token = authenticate_simplifier()
            g.simplifier_access_token = access_token
        return g.simplifier_access_token
    return authenticate_simplifier()


def authenticate_simplifier():
    """
    This function authenticates the user with the Simplifier API by sending a POST request with the user's email and password. The access token is returned if the authentication is successful; otherwise, an error message is printed, and None is returned.

    Returns:
    str: The access token required for making authorized API requests, or None if the authentication fails.
    """
    # Prepare the payload for the authentication request
    payload = {
        "email": config("SIMPLIFIER_USER"),
        "password": config("SIMPLIFIER_PASSWORD"),
    }

    # Send the authentication request
    response = requests.post(
        f"{TOKEN_BASE_URL}",
        json=payload,
    )

    # Check the response status code
    if response.status_code == 200:
        # Get the access token from the response
        access_token = response.json().get("token")
        return access_token
    else:
        print(f"Authentication failed with status code {response.status_code}")
        return None


def get_from_simplifier(resource_type, resource_id):
    """
    This function retrieves a specified FHIR resource from the Simplifier API by sending a GET request with the resource type and resource ID. The function raises an exception if the resource is not found or if there's an error in the request.

    Args:
    resource_type (str): The FHIR resource type to be retrieved (e.g., Patient, Observation).
    resource_id (str): The unique identifier of the FHIR resource to be retrieved.

    Returns:
    dict: The JSON representation of the requested FHIR resource.

    Raises:
    NotFound: If the specified resource is not found in the Simplifier API.
    Exception: If an error occurs while attempting to fetch the resource.
    """
    # Prepare the headers for the authentication request
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
    }

    response = requests.get(
        f"https://fhir.simplifier.net/Ronin-Common-FHIR-Model/{resource_type}/{resource_id}",
        headers=headers,
    )

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        raise NotFound(
            f"Not found in Simplifier: https://fhir.simplifier.net/Ronin-Common-FHIR-Model/{resource_type}/{resource_id}"
        )
    else:
        raise Exception(
            f"Status code {response.status_code} while attempting to fetch https://fhir.simplifier.net/Ronin-Common-FHIR-Model/{resource_type}/{resource_id}"
        )


def remove_file(resource_type, resource_id):
    """
    This function removes a specified FHIR resource from the Simplifier API by sending a DELETE request with the resource type and resource ID. The function assumes the necessary access token has already been obtained and stored.

    Args:
    resource_type (str): The FHIR resource type to be deleted (e.g., Patient, Observation).
    resource_id (str): The unique identifier of the FHIR resource to be deleted.

    Returns:
    str: A message indicating that the file has been removed.
    """
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
    }
    remove = requests.delete(
        f"https://fhir.simplifier.net/Ronin-Common-FHIR-Model/{resource_type}/{resource_id}/",
        headers=headers,
    )
    if remove.status_code == 200 or remove.status_code == 201:
        return "File removed"
    else:
        return f"Error: {remove.status_code}, {remove.text}"


def add_file(resource_type, resource_id, resource_body):
    """
    Add or update a FHIR resource on Simplifier.net.

    This function sends a PUT request to Simplifier.net to create or update
    a FHIR resource with the given resource_type and resource_id.

    Args:
        resource_type (str): The FHIR resource type (e.g., "Patient", "Observation").
        resource_id (str): The unique identifier for the FHIR resource.
        resource_body (dict): A Python dictionary representing the FHIR resource body.

    Returns:
        str: A message indicating the success or failure of the operation.
    """

    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }
    add = requests.put(
        f"https://fhir.simplifier.net/Ronin-Common-FHIR-Model/{resource_type}/{resource_id}",
        headers=headers,
        json=resource_body,
    )
    if add.status_code == 200 or add.status_code == 201:
        return "File added"
    else:
        return f"Error: {add.status_code}, {add.text}"

    return "File added"


def publish_to_simplifier(resource_type, resource_id, resource_body):
    """
    Publishes a file to the Simplifier API.

    Attempts to retrieve a file from the Simplifier API with the given 'resource_type' and 'resource_id'.
    If the file exists, it is deleted and replaced with a new file. If the file does not exist, a new file is created.

    :param resource_type: A string representing the resource type of the file to be published.
    :param resource_id: A string representing the resource ID of the file to be published.
    :return: None
    """

    try:
        get_from_simplifier(resource_type, resource_id)
        file_in_simplifier = True
    except NotFound:
        file_in_simplifier = False

    if file_in_simplifier:
        remove_file(resource_type, resource_id)
        add_file(resource_type, resource_id, resource_body)
    else:
        add_file(resource_type, resource_id, resource_body)


# if __name__ == "__main__":
# access_token = authenticate_simplifier()
# resource = get_from_simplifier("ValueSet", "2040eb90-6d8b-11ec-bcc4-f7e61651b088")
# print(resource)
# resource_type = "ValueSet"
# resource_id = "a3735146-9329-422d-bdf3-cac25e48d011"
# resource_body = {
#     "date": "2023-01-06T10:51:25.000000+00:00",
#     "description": "Contains SNOMED CT and ICD-10-CM codes that indicate RMSF Initial version",
#     "expansion": {
#         "contains": [
#             {
#                 "code": "A77.0",
#                 "display": "Spotted fever due to Rickettsia rickettsii",
#                 "system": "http://hl7.org/fhir/sid/icd-10-cm",
#                 "version": "2022",
#             },
#             {
#                 "code": "240615004",
#                 "display": "Western Rocky Mountain spotted fever (disorder)",
#                 "system": "http://snomed.info/sct",
#                 "version": "2022-09-01",
#             },
#             {
#                 "code": "186772009",
#                 "display": "Rocky Mountain spotted fever (disorder)",
#                 "system": "http://snomed.info/sct",
#                 "version": "2022-09-01",
#             },
#             {
#                 "code": "240616003",
#                 "display": "Eastern Rocky Mountain spotted fever (disorder)",
#                 "system": "http://snomed.info/sct",
#                 "version": "2022-09-01",
#             },
#         ],
#         "identifier": "urn:uuid:3d4d9738-8de2-11ed-8239-4ed2cdfbd88f",
#         "timestamp": "2023-01-06",
#         "total": 4,
#     },
#     "experimental": True,
#     "extension": [
#         {
#             "url": "http://projectronin.io/fhir/StructureDefinition/Extension/ronin-valueSetSchema",
#             "valueString": "2",
#         }
#     ],
#     "id": "a3735146-9329-422d-bdf3-cac25e48d011",
#     "meta": {
#         "profile": [
#             "http://projectronin.io/fhir/StructureDefinition/ronin-valueSet"
#         ]
#     },
#     "name": "Testrmsf",
#     "purpose": "Practice VS creation",
#     "resourceType": "ValueSet",
#     "url": "http://projectronin.io/fhir/ValueSet/a3735146-9329-422d-bdf3-cac25e48d011",
#     "version": "1",
# }
#
# result = add_file(resource_type, resource_id, resource_body)
# print(result)
