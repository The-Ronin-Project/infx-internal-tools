from decouple import config
from requests import Response
from werkzeug.exceptions import BadRequest
import datetime
import json
from flask import g, has_request_context
from werkzeug.exceptions import NotFound
import requests
import logging

LOGGER = logging.getLogger()


# Use config() to read values from the .env file
BASE_URL = config("SIMPLIFIER_BASE_URL", default="https://fhir.simplifier.net")
TOKEN_BASE_URL = config(
    "SIMPLIFIER_TOKEN_URL", default="https://api.simplifier.net/token"
)


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
        resource_id (str): The id of the value set, uuid for Ronin and name for HL7.
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


def publish_to_simplifier(resource_type, resource_id, resource_body):
    """
    Publishes a file to the Simplifier API.

    Attempts to retrieve a file from the Simplifier API with the given 'resource_type' and 'resource_id'.
    If the file exists, it is deleted and replaced with a new file. If the file does not exist, a new file is created.

    :param resource_type: A string representing the resource type of the file to be published.
    :param resource_id: A string representing the resource ID of the file to be published.
    :@param resource_body: a JSON expansion of the version in RCDM format
    :return: None
    """

    if is_simplifier_write_disabled():
        LOGGER.info("Simplifier write operations are disabled.")
        return {"message": "No attempt to publish to simplifier, write operations are disabled"}

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


def is_simplifier_write_disabled():
    """
    For use while testing to disable writing to simplifier
    """
    return config("DISABLE_SIMPLIFIER_WRITE", default="False", cast=bool)


# TODO What is this commented out stuff?
# if __name__ == "__main__":
#     access_token = authenticate_simplifier()
# #     resource = get_from_simplifier("ValueSet", "administrative-gender")
# #     print(resource)
# resource_type = "ValueSet"
# resource_id = "AdministrativeGender"
# resource_body = {
#     "date": "2022-09-09T13:58:04.000000+00:00",
#     "description": "{{pagelink:Ronin-Implementation-Guide-Home/List/Valuesets/AdministrativeGender.page.md}} Initial version",
#     "expansion": {
#         "contains": [
#             {
#                 "code": "female",
#                 "display": "Female",
#                 "system": "http://hl7.org/fhir/administrative-gender",
#                 "version": "4.0.1",
#             },
#             {
#                 "code": "unknown",
#                 "display": "Unknown",
#                 "system": "http://hl7.org/fhir/administrative-gender",
#                 "version": "4.0.1",
#             },
#             {
#                 "code": "male",
#                 "display": "Male",
#                 "system": "http://hl7.org/fhir/administrative-gender",
#                 "version": "4.0.1",
#             },
#             {
#                 "code": "other",
#                 "display": "Other",
#                 "system": "http://hl7.org/fhir/administrative-gender",
#                 "version": "4.0.1",
#             },
#         ],
#         "identifier": "urn:uuid:b9bee2a2-306f-11ed-b1fb-9a3d1799f71d",
#         "timestamp": "2022-09-09",
#         "total": 4,
#     },
#     "experimental": False,
#     "extension": [
#         {
#             "url": "http://projectronin.io/fhir/StructureDefinition/Extension/ronin-valueSetSchema",
#             "valueString": "2",
#         }
#     ],
#     "id": "AdministrativeGender",
#     "meta": {
#         "profile": ["http://projectronin.io/fhir/StructureDefinition/ronin-valueSet"]
#     },
#     "name": "AdministrativeGender",
#     "purpose": "FHIR Interops",
#     "resourceType": "ValueSet",
#     "status": "active",
#     "title": "AdministrativeGender",
#     "url": "http://hl7.org/fhir/ValueSet/AdministrativeGender",
#     "version": "1",
# }
#
# result = add_file(resource_type, resource_id, resource_body)
# print(result)
