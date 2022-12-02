import oci

from decouple import config
from oci.object_storage import ObjectStorageClient
from werkzeug.exceptions import BadRequest
import datetime
import json
from sqlalchemy import text

from app.database import get_db
from app.helpers.db_helper import db_cursor


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


def pre_export_validate(object_type_version):
    if object_type_version.pre_export_validate is False:
        raise BadRequest("This object cannot be published because it failed validation")


def folder_path_for_oci(folder, object_type, path):
    """
    This function creates the oci folder path based on folder given (prerelease or published) - prerelease includes
    an utc timestamp appended at the end as there can be multiple versions in prerelease
    @param folder: destination folder (prerelease or published)
    @param object_type: either concept map or value set object
    @param path: string path - complete folder path location
    @return: string of folder path
    """
    if folder == "prerelease":
        path = (
            path + f"/{datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}.json"
        )
        return path
    if folder == "published":
        path = path + f"/{object_type['version']}.json"
        return path


def check_for_prerelease_in_published(
    path, object_storage_client, bucket_name, namespace, object_type
):
    """
    This function changes the folder path to reflect published if the folder passed was prerelease.  We do this
    specifically to check if a PRERELEASE concept map is already in the PUBLISHED FOLDER
    @param path: complete string folder path for the object_type
    @param object_storage_client: oci client to check for file existence
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param object_type: either concept map or value set object - used here to get the version appended to the folder path
    @return: True or False depending on if the file exists in the published folder
    """
    published_path = path.replace("prerelease", "published")
    path_to_check = published_path + f"/{object_type['version']}.json"
    exists_in_published = folder_in_bucket(
        path_to_check, object_storage_client, bucket_name, namespace
    )
    return exists_in_published


def set_up_object_store(object_type, folder):
    """
    This function is the conditional matrix for saving a concept map to oci.  The function LOOKS
    to see if the concept map already exists and LOOKS to see where it should be saved.
    @param object_type:  either concept map or value set object - dictionary of respective metadata
    @param folder: string folder destination (prerelease or published)
    @return: object_type if saved to oci, otherwise messages returned based on findings
    """
    object_storage_client = oci_authentication()
    if (
        object_type["resourceType"] == "ConceptMap"
    ):  # using resourceType to set correct initial folder in path and pull overall uuid
        object_type_uuid = object_type["url"].rsplit("/", 1)[1]
        top_folder_name = "ConceptMaps"
    else:
        object_type_uuid = str(object_type["id"])
        top_folder_name = "ValueSets"
    if (
        object_type["status"] == "active"
        or object_type["status"] == "in progress"
        or object_type["status"] == "pending"
    ):
        path = f"{top_folder_name}/v1/{folder}/{object_type_uuid}"
    else:
        raise BadRequest(
            "This object cannot be saved in object store, status must be either active or in progress."
        )
    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data

    if folder == "prerelease":  # flow for prerelease after status check
        pre_in_pub = check_for_prerelease_in_published(  # check if file exists in PUBLISHED folder
            path, object_storage_client, bucket_name, namespace, object_type
        )
        if pre_in_pub:
            return {"message": "concept map is already in the published bucket"}
    folder_exists = folder_in_bucket(
        path, object_storage_client, bucket_name, namespace
    )
    if not folder_exists:
        del object_type["status"]
        path = folder_path_for_oci(folder, object_type, path)
        save_to_object_store(  # another function in this file
            path, object_storage_client, bucket_name, namespace, object_type
        )
        return object_type
    elif folder_exists:
        del object_type["status"]
        path = folder_path_for_oci(folder, object_type, path)
        if folder == "prerelease":
            save_to_object_store(
                path, object_storage_client, bucket_name, namespace, object_type
            )
            return object_type
        if folder == "published":  # flow for published
            version_exist = folder_in_bucket(
                path, object_storage_client, bucket_name, namespace
            )
            if version_exist:
                return {"message": "concept map already in bucket"}
            else:
                save_to_object_store(
                    path, object_storage_client, bucket_name, namespace, object_type
                )
            return object_type


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
    path, object_storage_client, bucket_name, namespace, object_type
):
    """
    This function saves the given concept map to the oci infx-shared bucket based on the folder path given
    @param path: string path for folder
    @param object_storage_client: oci client
    @param bucket_name: bucket for oci - most cases 'infx-shared'
    @param namespace: oci namespaced for infx bucket
    @param object_type: either concept map or value set object - used here to get the version appended to the folder path
    @return: completion message and concept map
    """
    if object_type["resourceType"] == "ValueSet":
        object_type["additionalData"]["value_set_uuid"] = str(
            object_type["additionalData"]["value_set_uuid"]
        )
        object_type["additionalData"]["version_uuid"] = str(
            object_type["additionalData"]["version_uuid"]
        )
    object_storage_client.put_object(
        namespace,
        bucket_name,
        path,
        json.dumps(object_type, indent=2).encode("utf-8"),
    )
    return {"message": "object pushed to bucket", "object": object_type}


@db_cursor
def version_set_status_active(conn, version_uuid, object_type):
    """
    This function updates the status of the object_type version to "active" and inserts the publication date as now
    @param conn: db_cursor wrapper function to create connection to sql db
    @param version_uuid: UUID; object_type version used to set status in pgAdmin
    @param object_type: either concept map or value set object, used to determine the correct schema and table
    @return: result from query
    """
    if object_type == "concept_map":
        schema_and_table = "concept_maps.concept_map_version"
        data = conn.execute(
            text(
                f"""
                UPDATE {schema_and_table}
                SET status=:status, published_date=:published_date
                WHERE uuid=:version_uuid
                """
            ),
            {
                "status": "active",
                "published_date": datetime.datetime.now(),
                "version_uuid": version_uuid,
            },
        )
    else:
        schema_and_table = "value_sets.value_set_version"
        data = conn.execute(
            text(
                f"""
                UPDATE {schema_and_table}
                SET status=:status
                WHERE uuid=:version_uuid
                """
            ),
            {
                "status": "active",
                "version_uuid": version_uuid,
            },
        )
    return data


@db_cursor
def get_object_type_from_db(conn, version_uuid, object_type):
    """
    This function runs the below sql query to get the overall object_type uuid and version for use in searching
    oci storage, sql query returns most recent version
    @param conn: db_cursor wrapper function to create connection to sql db
    @param version_uuid: UUID; concept map version used to retrieve overall object_type uuid
    @param object_type: either concept map or value set object, used to determine the correct schema and table
    @return: dictionary containing overall object_type uuid and version
    """
    if object_type == "concept_map":
        schema_and_table = "concept_maps.concept_map_version"
    else:
        schema_and_table = "value_sets.value_set_version"
    data = conn.execute(
        text(
            f"""
            select * from {schema_and_table}
            where uuid=:uuid order by version desc 
            """
        ),
        {"uuid": version_uuid},
    ).first()
    if data is None:
        return False

    result = dict(data)
    return {"folder_name": result["concept_map_uuid"], "version": result["version"]}


def get_object_type_from_object_store(object_type, location_info, folder):
    """
    This function gets the requested object_type from oci storage
    @param object_type: either concept map or value set object used to get the most recent version
    @param location_info: information pertaining to object_type (note: variable in @app is either concept_map or value_se but returns a dictionary of overall uuid and version integer)
    @param folder: string path to look for in oci
    @return: json of object_type found in oci storage
    """
    if object_type == "concept_map":
        top_folder_name = "ConceptMaps"
    else:
        top_folder_name = "ValueSets"
    object_storage_client = oci_authentication()
    bucket_name = config("OCI_CLI_BUCKET")
    namespace = object_storage_client.get_namespace().data
    path = f"{top_folder_name}/v1/{folder}/{str(location_info['folder_name'])}/{location_info['version']}.json"
    try:
        object_type_found = object_storage_client.get_object(
            namespace, bucket_name, path
        )
    except:
        return {"message": f"{path} not found."}
    return object_type_found.data.json()
