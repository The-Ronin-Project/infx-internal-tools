import asyncio
import datetime
import json
import logging
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from json import JSONDecodeError
from typing import Dict, List, Optional
import traceback
import sys
from unittest import skip

import httpx
from cachetools.func import ttl_cache
from decouple import config
from deprecated.classic import deprecated
from httpx import ReadTimeout
from httpcore import PoolTimeout as HttpcorePoolTimeout
from httpx import PoolTimeout as HttpxPoolTimeout
from sqlalchemy import text, Table, Column, MetaData, Text, bindparam
from sqlalchemy.dialects.postgresql import UUID as UUID_column_type
from werkzeug.exceptions import BadRequest

from app.models.normalization_error_service import get_environment_from_service_url, \
    DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL, get_token, AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE, \
    make_get_request
from app.terminologies.models import Terminology
from app.database import get_db


@skip("This is a utility, not a test. This one-time repair function has been used already. Retaining as an example.")
def test_norm_registry_null_environments():
    """
    Not a test. Really a tool for developers to perform a one-time correction in the error_service_issue table.
    Repeat test_norm_registry_null_environments() in each environment until there are no results to the first query.
    This function offers a useful pattern for using GET /resources/:resource_uuid to see whether a particular resource
    still exists on the Data Ingestion Error Validation Service. The pattern is that if a JSONDecodeError is raised
    by this GET it is because it returned an empty string. This particular GET does that when the resource is not found.
    """
    from app.database import get_db

    conn = get_db()
    query = """
        select distinct resource_uuid from 
        custom_terminologies.error_service_issue
        where environment is null
    """
    results = conn.execute(text(query))
    environment = get_environment_from_service_url(
        DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL
    )
    environment_uuid_list = []
    for row in results:
        resource_uuid = row.resource_uuid
        try:
            token = get_token(AUTH_URL, CLIENT_ID, CLIENT_SECRET, AUTH_AUDIENCE)
            url = f"/resources/{resource_uuid}"
            timeout_config = httpx.Timeout(timeout=600.0, pool=600.0, read=600.0, connect=600.0)
            with httpx.Client(timeout=timeout_config) as client:
                # try to GET this resource_uuid from the environment
                response = make_get_request(
                    token=token,
                    client=client,
                    base_url=DATA_NORMALIZATION_ERROR_SERVICE_BASE_URL,
                    api_url=url,
                )
                if response is not None:
                    # the GET response shows this resource_uuid was found in this environment
                    environment_uuid_list.append(str(resource_uuid))
        except JSONDecodeError:
            # the GET response was an empty string that JSON could not decode because the resource_uuid was not found
            continue
        except Exception as e:
            # the GET request experienced an uncaught exception that we need to study and/or prevent
            raise e
    # insert the environment name into the environment column for each row whose resource_uuid is in the uuid_list
    query = f"""
        update custom_terminologies.error_service_issue set environment = '{environment}'
        where resource_uuid in ('{"','".join(environment_uuid_list)}')
    """
    try:
        conn.execute(text(query))
        conn.commit()
        conn.close()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise e
