import os.path
from typing import Optional
import oci
from decouple import config
from oci.object_storage import ObjectStorageClient
from werkzeug.exceptions import BadRequest, Conflict
import datetime
import json
import logging

from app.errors import NotFoundException, OCIException

LOGGER = logging.getLogger()


def oci_authentication():
    """
    config and connection for oci storage. get env variables and create oci storage client to be used for accessing
    the infx shared bucket in oci
    @return: oci storage client - ObjectStorageClient (oci)
    """
    oci_config = {
        "user": config("OCI_CLI_USER"),
        "fingerprint": config("OCI_CLI_FINGERPRINT"),
        "key_file": config("OCI_CLI_KEY_FILE"),
        "tenancy": config("OCI_CLI_TENANCY"),
        "region": config("OCI_CLI_REGION"),
    }
    oci.config.validate_config(oci_config)
    object_storage_client = ObjectStorageClient(oci_config)
    return object_storage_client


def folder_path_for_oci(content: dict, path: str, content_type: str):
    """
    This function creates the oci folder and file path based on path and content_type.
    @param content: content to publish, with metadata to support publication
    @param path: full folder path in OCI storage
    @param content_type: "csv" or "json" - filename extension
    @return: string path - full folder and file path in OCI
    """
    path = path + f"/{content['version']}.{content_type}"
    return path


def set_up_and_save_to_object_store(content: dict, oci_file_path: str, overwrite_allowed: bool = False) -> dict:
    """
    This function is the conditional matrix for saving a resource to oci.  The function LOOKS
    to see if the resource already exists and LOOKS to see where it should be saved.
    @param content: content to publish, with metadata to support publication
    @param oci_file_path: path location for this resource - includes the filename and extension. folder_path_for_oci()
        can be used to help format this correctly.
    @param overwrite_allowed: if true, write to oci even if the object exists. Default is False
    @return: content, if saved to oci. Otherwise, raises an exception
    """
    object_storage_client = oci_authentication()

    is_value_set: bool = (content.get("resourceType") == "ValueSet")
    if is_value_set and content["status"] not in (
        "active",
        "in progress",
        "pending",
        "draft",
    ):
        raise ValueError(
            "This object cannot be saved in object store, status must be either active or in progress."
        )

    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data

    status = content.get("status")
    if status is not None:
        del content["status"]

    file_exists = file_in_bucket(
        oci_file_path, object_storage_client, bucket_name, namespace
    )

    if not file_exists or overwrite_allowed:
        save_to_object_store(
            oci_file_path, object_storage_client, bucket_name, namespace, content
        )
    else:
        raise ValueError(
            "This object already exists in the object store and cannot be overwritten without an override."
        )

    return content


def file_in_bucket(
        file_path: str,
        object_storage_client: ObjectStorageClient,
        bucket_name: str,
        namespace: str,
        start_with: Optional[str] = None
):
    """
    This function will check if a specified file already exists in oci by recursively searching through the
    paginated results that the OCI API returns.
    @param file_path: string path of file (e.g. ConceptMaps/v4/published/<some hash>/1.json)
    @param object_storage_client: oci client
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param start_with: Value that holds the element to start the next paginated search with. External callers
        initiating a search should just pass None. It's used as part of the recursive search to iterate through
        paginated results coming from the OCI API. See ObjectStorageClient.list_objects for more information.
    @return: True or False depending on if the file exists
    """
    it_exists = False
    dir_path = os.path.dirname(file_path)

    # Initial run would start the search at the beginning.
    if not start_with:
        result_object = object_storage_client.list_objects(namespace, bucket_name, prefix=dir_path)
    else:
        result_object = object_storage_client.list_objects(namespace, bucket_name, prefix=dir_path, start=start_with)

    # OCI API populates this value if there are more paginated results to search
    next_starts_with = result_object.data.next_start_with

    # Search for search_key in the returned page of objects
    for data_object in result_object.data.objects:
        if file_path == data_object.name:
            it_exists = True
            break

    if not it_exists and next_starts_with:
        # If we haven't found a match yet and there are more pages, recursively continue the search.
        return file_in_bucket(file_path, object_storage_client, bucket_name, namespace, next_starts_with)
    else:
        return it_exists


def save_to_object_store(
    object_path: str,
    object_storage_client: ObjectStorageClient,
    bucket_name: str,
    namespace: str,
    content: dict,
) -> dict:
    """
    This function saves the given item to the oci infx-shared bucket based on the folder path given
    @param object_path: absolute path for oci object (file)
    @param object_storage_client: oci client
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param content: content to publish, with metadata to support publication
    @return: completion message and content
    """
    LOGGER.info(f"Attempting to save {object_path} to OCI bucket")
    put_object_response = object_storage_client.put_object(
        namespace,
        bucket_name,
        object_path,
        json.dumps(content, indent=2).encode("utf-8"),
    )

    LOGGER.info(f"OCI response status code {put_object_response.status}")
    if put_object_response.status != 200:
        raise OCIException(f"Failed to publish {object_path}. Response status: {put_object_response.status}")
    return {"message": "object pushed to bucket", "object": content}


def get_data_from_oci(
    oci_root,
    resource_schema_version: str,
    release_status,
    resource_id,
    resource_version,
    content_type,
    return_content=True,
):
    """
    Return data of the specified content_type from the OCI infx-shared bucket.
    The path to the data in infx-shared is given by the following inputs in order:
    @param oci_root: top level folder name for this resource_type: "ConceptMap", "ValueSet" etc.
    @param resource_schema_version: "1", "2" etc. - caller may also append a sub-folder path using "/" i.e. "5/vitals"
    @param release_status: status value i.e. "published" - or "dev" vs. "stage" vs. "prod"
    @param resource_id: uuid in the database for the resource_type
    @param resource_version: integer - this is the file name
    @param content_type: for example "json" or "csv" - the content type and file name extension
    @param return_content: True or False
    @return:
    """
    # Reset to default if not explicitly passed in as False
    if return_content is None:
        return_content = True

    object_storage_client = oci_authentication()
    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data
    path = f"{oci_root}/v{resource_schema_version}/{release_status}/{resource_id}/{resource_version}.{content_type}"
    try:
        resource = object_storage_client.get_object(namespace, bucket_name, path)
        if return_content and content_type == "json":
            return resource.data.json()
        elif return_content and content_type == "csv":
            return resource.data.content.decode("utf-8")
        else:
            return {
                "message": f"Found {path} in OCI, but {content_type} is not an expected content-type for this response"
            }
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            raise NotFoundException(
                f"Did not find the expected file /{path} in OCI"
            )
        else:
            raise e
