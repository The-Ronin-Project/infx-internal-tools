import oci
from oci.object_storage import ObjectStorageClient


def oci_authentication():
    config = {
        "user": "ocid1.user.oc1..aaaaaaaamtdwu3jkstjno46jq6qw5uascqkhfley27mwzjuzkirukigv633a",
        "fingerprint": "97:b4:87:a8:bd:81:69:db:d2:29:a2:93:32:93:4e:6b",
        "tenancy": "ocid1.tenancy.oc1..aaaaaaaapjtgtxtifoh4yi5wq3o5vkafdnd5nplvew4phqtrntp74pehz4yq",
        "region": "us-phoenix-1",
        "key_file": "./app/helpers/oci_key.pem",
    }
    oci.config.validate_config(config)
    object_storage_client = ObjectStorageClient(config)
    return object_storage_client
