import os.path
from typing import Optional
import oci
from decouple import config
from oci.object_storage import ObjectStorageClient
from app.helpers.oci_helper import is_oci_write_disabled


def test_is_oci_write_disabled():
    """
    Test to verify if the OCI writing capability is disabled via an environment variable.

    This function sets the 'DISABLE_OCI_WRITE' environment variable to 'True', simulating the scenario where writing
    to OCI is explicitly disabled in the environment.

    Test Environment:
    - The 'DISABLE_OCI_WRITE' environment variable controls the ability to write to OCI.
    - A value of 'True' for 'DISABLE_OCI_WRITE' indicates that OCI writing is disabled.
    - For example: DISABLE_OCI_WRITE=True
    """
    os.environ["DISABLE_OCI_WRITE"] = "True"
    assert is_oci_write_disabled()


def test_not_is_oci_write_disabled():
    """
    Test to verify that OCI writing capability is enabled when the corresponding environment variable is set to 'False'.

    This function sets the 'DISABLE_OCI_WRITE' environment variable to 'False', simulating the default environment.

    Test Environment:
    - The 'DISABLE_OCI_WRITE' environment variable controls the ability to write to OCI.
    - A value of 'False' for 'DISABLE_OCI_WRITE' indicates that OCI writing is enabled.
    - 'False' is the default state.
    """
    os.environ["DISABLE_OCI_WRITE"] = "False"
    assert not is_oci_write_disabled()


