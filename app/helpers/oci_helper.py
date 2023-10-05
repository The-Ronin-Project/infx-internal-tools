import oci
from decouple import config
from oci.object_storage import ObjectStorageClient
from werkzeug.exceptions import BadRequest
import datetime
import json

from app.errors import NotFoundException


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


def folder_path_for_oci(content, path, content_type):
    """
    This function creates the oci folder and file path based on path (contains "prerelease" or not) and content_type.
    prerelease includes a utc timestamp appended at the end of the filename, as prerelease can contain multiple versions
    @param content: content to publish, with metadata to support publication
    @param path: full folder path in OCI storage
    @param content_type: "csv" or "json" - filename extension
    @return: string path - full folder and file path in OCI
    """
    if "/prerelease" in path:
        path = path + f"/{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.{content_type}"
        return path
    else:
        path = path + f"/{content['version']}.{content_type}"
        return path


def check_for_prerelease_in_published(
    path, object_storage_client, bucket_name, namespace, content
):
    """
    This function changes the folder path to reflect published if the folder passed was prerelease.  We do this
    specifically to check if a PRERELEASE concept map is already in the PUBLISHED FOLDER
    @param path: complete string folder path for the content
    @param object_storage_client: oci client to check for file existence
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param content: content to publish, with metadata to support publication
    @return: True or False depending on if the file exists in the published folder
    """
    published_path = path.replace("prerelease", "published")
    path_to_check = published_path + f"/{content['version']}.json"
    exists_in_published = folder_in_bucket(
        path_to_check, object_storage_client, bucket_name, namespace
    )
    return exists_in_published


def set_up_object_store(content, initial_path, folder, content_type):
    """
    This function is the conditional matrix for saving a resource to oci.  The function LOOKS
    to see if the resource already exists and LOOKS to see where it should be saved.
    @param content: content to publish, with metadata to support publication
    @param initial_path: path location for this type of resource - includes the destination sub-folder
    @param folder: destination sub-folder: "prerelease" "published" "labs/dev" "vitals/stage" "documents/prod" etc.
    @param content_type: "csv" or "json" - filename extension
    @return: content if saved to oci, otherwise messages returned based on findings
    """
    object_storage_client = oci_authentication()

    isValueSet: bool = (content.get("resourceType") == "ValueSet")
    if isValueSet and content["status"] not in (
        "active",
        "in progress",
        "pending",
        "draft",
    ):
        raise BadRequest(
            "This object cannot be saved in object store, status must be either active or in progress."
        )

    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data
    path = folder_path_for_oci(content, initial_path, content_type=content_type)

    if folder == "prerelease":  # flow for prerelease after status check
        pre_in_pub = check_for_prerelease_in_published(  # check if file exists in PUBLISHED folder
            path, object_storage_client, bucket_name, namespace, content
        )
        if pre_in_pub:
            return {"message": "This object is already in the published bucket"}
    folder_exists = folder_in_bucket(
        path, object_storage_client, bucket_name, namespace
    )
    if not folder_exists:
        status = content.get("status")
        if status is not None:
            del content["status"]
        save_to_object_store(  # another function in this file
            path, object_storage_client, bucket_name, namespace, content
        )
        return content
    elif folder_exists:
        status = content.get("status")
        if status is not None:
            del content["status"]
        if folder == "prerelease":
            save_to_object_store(
                path, object_storage_client, bucket_name, namespace, content
            )
            return content
        if folder == "published":  # flow for published
            version_exist = folder_in_bucket(
                path, object_storage_client, bucket_name, namespace
            )
            if version_exist:
                return {"message": "This object is already in the published bucket"}
            else:
                save_to_object_store(
                    path, object_storage_client, bucket_name, namespace, content
                )
            return content


def folder_in_bucket(path, object_storage_client, bucket_name, namespace):
    """
    This function will check if a specified folder/file already exists in oci
    @param path: string path of folder
    @param object_storage_client: oci client
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @return: True or False depending on if the file exists in the published folder
    """
    object_list = object_storage_client.list_objects(namespace, bucket_name)
    exists = [x for x in object_list.data.objects if path in x.name]
    return True if exists else False


def save_to_object_store(
    path, object_storage_client, bucket_name, namespace, content
):
    """
    This function saves the given item to the oci infx-shared bucket based on the folder path given
    @param path: string path for folder
    @param object_storage_client: oci client
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param content: content to publish, with metadata to support publication
    @return: completion message and concept map
    """
    object_storage_client.put_object(
        namespace,
        bucket_name,
        path,
        json.dumps(content, indent=2).encode("utf-8"),
    )
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
    @param release_status: status value i.e. "published" vs. "prerelease" - or "dev" vs. "stage" vs. "prod"
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


def put_data_to_oci(
    content,
    oci_root,
    resource_schema_version: str,
    release_status,
    resource_id,
    resource_version,
    content_type
):
    """
    Store data of the specified content_type in the OCI infx-shared bucket.
    The path to the data in infx-shared is given by the following inputs in order:
    @param content: the data
    @param oci_root: top level folder name for this resource_type: "ConceptMap", "ValueSet" etc.
    @param resource_schema_version: "1", "2" etc. - caller may also append a sub-folder path using "/" i.e. "5/vitals"
    @param release_status: status value i.e. "published" vs. "prerelease" - or "dev" vs. "stage" vs. "prod"
    @param resource_id: uuid in the database for the resource_type
    @param resource_version: integer - this is the file name
    @param content_type: for example "json" or "csv" - the content type and file name extension
    @return:
    """
    object_storage_client = oci_authentication()
    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data
    path = f"{oci_root}/v{resource_schema_version}/{release_status}/{resource_id}/{resource_version}.{content_type}"
    object_storage_client.put_object(
        namespace,
        bucket_name,
        path,
        content
    )
