import oci

from decouple import config
from oci.object_storage import ObjectStorageClient


def oci_authentication():
    oci_config = {
        "user": config("OCI_USER"),
        "fingerprint": config("OCI_FINGERPRINT"),
        "key_file": config("OCI_KEY_FILE"),
        "tenancy": config("OCI_TENANCY"),
        "region": config("OCI_REGION"),
    }
    oci.config.validate_config(oci_config)
    object_storage_client = ObjectStorageClient(oci_config)
    return object_storage_client
