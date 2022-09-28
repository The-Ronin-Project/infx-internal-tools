import oci
from oci.object_storage import ObjectStorageClient


def oci_authentication():
    config = oci.config.from_file(
        file_location="./configs/oci_config", profile_name="DEFAULT"
    )
    oci.config.validate_config(config)
    object_storage_client = ObjectStorageClient(config)
    return object_storage_client
