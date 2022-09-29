import oci

from decouple import config
from oci.object_storage import ObjectStorageClient


def is_oci():
    oci_config = {
        "USER": config("OCI_USER"),
        "FINGERPRINT": config("OCI_FINGERPRINT"),
        "KEY_FILE": config("OCI_KEY_FILE"),
        "TENANCY": config("OCI_TENANCY"),
        "REGION": config("OCI_REGION"),
    }
    return oci_config


def oci_authentication():
    oci_vars = is_oci()
    oci_config = {
        "user": oci_vars["USER"],
        "fingerprint": oci_vars["FINGERPRINT"],
        "key_file": oci_vars["KEY_FILE"],
        "tenancy": oci_vars["TENANCY"],
        "region": oci_vars["REGION"],
    }
    oci.config.validate_config(oci_config)
    object_storage_client = ObjectStorageClient(oci_config)
    return object_storage_client
