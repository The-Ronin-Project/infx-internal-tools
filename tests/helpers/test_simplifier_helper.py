import unittest
import os.path
import pytest
from decouple import config
from app.helpers.simplifier_helper import is_simplifier_write_disabled


def test_is_simplifier_write_disabled():
    """
    Test to verify if the Simplifier writing capability is disabled via an environment variable.
    This function sets the 'DISABLE_SIMPLIFIER_WRITE' environment variable to 'True', simulating the scenario where writing
    to Simplifier is explicitly disabled in the environment.
    Test Environment:
    - The 'DISABLE_SIMPLIFIER_WRITE' environment variable controls the ability to write to Simplifier.
    - A value of 'True' for 'DISABLE_SIMPLIFIER_WRITE' indicates that Simplifier writing is disabled.
    - For example: DISABLE_SIMPLIFIER_WRITE=True
    """
    os.environ["DISABLE_SIMPLIFIER_WRITE"] = "True"
    assert is_simplifier_write_disabled()


def test_not_is_simplifier_write_disabled():
    """
    Test to verify that Simplifier writing capability is enabled when the corresponding environment variable is set to 'False'.
    This function sets the 'DISABLE_SIMPLIFIER_WRITE' environment variable to 'False', simulating the default environment.
    Test Environment:
    - The 'DISABLE_SIMPLIFIER_WRITE' environment variable controls the ability to write to Simplifier.
    - A value of 'False' for 'DISABLE_SIMPLIFIER_WRITE' indicates that Simplifier writing is enabled.
    - 'False' is the default state.
    """
    os.environ["DISABLE_SIMPLIFIER_WRITE"] = "False"
    assert not is_simplifier_write_disabled()
