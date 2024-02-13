import csv
import datetime
import json
from dataclasses import dataclass, field
from cachetools.func import ttl_cache
from typing import List, Dict, Tuple, Optional, Any
import re
import requests
import concurrent.futures
import logging

from psycopg2 import DatabaseError
from sqlalchemy import text, MetaData, Table, Column, String, Row
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime
from dateutil import parser
from collections import defaultdict
from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy.sql.expression import bindparam


from app.errors import (
    NotFoundException,
    BadDataError,
    DataIntegrityError,
    BadRequestWithCode,
)
from app.helpers.message_helper import message_exception_classname

from app.helpers.oci_helper import set_up_object_store

from app.models.codes import Code

import app.concept_maps.models
import app.models.data_ingestion_registry
import app.models.codes

from app.terminologies.models import Terminology

from app.database import get_db  # , get_elasticsearch
from flask import current_app
from app.helpers.simplifier_helper import publish_to_simplifier

from app.models.use_case import load_use_case_by_value_set_uuid, UseCase

ECL_SERVER_PATH = "https://snowstorm.prod.projectronin.io"
SNOSTORM_LIMIT = 1000

# RXNORM_BASE_URL = "https://rxnav.nlm.nih.gov/REST/"
RXNORM_BASE_URL = "https://rxnav.prod.projectronin.io/REST/"

MAX_ES_SIZE = 1000

metadata = MetaData()
expansion_member = Table(
    "expansion_member",
    metadata,
    Column("expansion_uuid", UUID, nullable=False),
    Column("code", String, nullable=False),
    Column("display", String, nullable=False),
    Column("system", String, nullable=False),
    Column("version", String, nullable=False),
    Column("custom_terminology_uuid", UUID, nullable=True),
    schema="value_sets",
)


#
# Value Set Rules
#


class VSRule:
    """
    This is a base class for creating value set rules.
    """

    def __init__(
        self,
        uuid,
        position,
        description,
        prop,
        operator,
        value,
        include,
        value_set_version,
        fhir_system,
        terminology_version,
    ):
        """
        This method initializes the VSRule object with the provided parameters.
        The property, operator, and value are the core pieces.

        :param prop: A string this can be a code, display, specimen or other fundamental property of the code system
        :param operator: Defines the relationship between the property and the value.
        :param value: A string representing the user supplied value for the property (this could be code value, etc.)
        :param uuid: A string representing the UUID of the rule.
        :param position: An integer that represents position when we are displaying the rule in the UI to provide structure
        :param description: A string representing the description of the rule.
        :param include: An integer indicating whether to include the results in the value set.
        :param value_set_version: A string representing the value set version of the rule.
        :param fhir_system: A string representing the FHIR system of the rule.
        :param terminology_version: A TerminologyVersion object representing the terminology version of the rule.
        """
        self.uuid = uuid
        self.position = position
        self.description = description
        self.property = prop
        self.operator = operator
        self.value = value
        self.include = include
        if self.include == 1:
            self.include = True
        if self.include == 0:
            self.include = False
        self.value_set_version = value_set_version
        self.terminology_version = terminology_version
        self.fhir_system = fhir_system

        self.results = set()

    @classmethod
    def load(cls, rule_uuid):
        if rule_uuid is None:
            raise BadRequestWithCode(
                "ValueSetRule.load.empty",
                "Cannot update Value Set Rule: empty Terminology Version ID",
            )
        conn = get_db()
        result = conn.execute(
            text(
                """
              select * from value_sets.value_set_rule
              where uuid =:uuid
              """
            ),
            {"uuid": rule_uuid},
        ).first()

        if result is None:
            raise NotFoundException(f"No Value Set Rule found with UUID: {rule_uuid}")

        return cls(
            uuid=result.uuid,
            position=result.position,
            description=result.description,
            prop=result.property,
            operator=result.operator,
            value=result.value,
            include=result.include,
            value_set_version=result.value_set_version,
            terminology_version=result.terminology_version,
            fhir_system=None,
        )

    def update(self, new_terminology_version_uuid):
        if new_terminology_version_uuid is None:
            raise BadRequestWithCode(
                "ValueSetRule.update.empty",
                "Cannot update Value Set Rule: empty Terminology Version ID",
            )
        conn = get_db()
        conn.execute(
            text(
                """
              update value_sets.value_set_rule
              set terminology_version = :new_terminology_version_uuid
              where uuid = :rule_uuid
              """
            ),
            {
                "rule_uuid": self.uuid,
                "new_terminology_version_uuid": new_terminology_version_uuid,
            },
        )

    def execute(self):
        """
        Executes the rule by calling the corresponding method based on the operator and property.
        """
        if self.operator == "descendent-of":
            self.descendent_of()
        elif self.operator == "self-and-descendents":
            self.self_and_descendents()
        elif self.operator == "direct-child":
            self.direct_child()
        elif self.operator == "is-a":
            self.direct_child()
        elif self.operator == "in" and self.property == "concept":
            self.concept_in()
        elif self.operator == "in-section":
            self.in_section()
        elif self.operator == "in-chapter":
            self.in_chapter()
        elif self.operator == "has-body-system":
            self.has_body_system()
        elif self.operator == "has-root-operation":
            self.has_root_operation()
        elif self.operator == "has-body-part":
            self.has_body_part()
        elif self.operator == "has-qualifier":
            self.has_qualifier()
        elif self.operator == "has-approach":
            self.has_approach()
        elif self.operator == "has-device":
            self.has_device()

        if self.property == "code" and self.operator == "in":
            self.code_rule()
        if self.property == "display" and self.operator == "regex":
            self.display_regex()

        # RxNorm Specific
        if self.property == "term_type_within_class":
            self.term_type_within_class()
        if self.property == "term_type":
            self.rxnorm_term_type()
        if self.property == "all_active_rxnorm":
            self.all_active_rxnorm()

        # SNOMED
        if self.property == "ecl":
            self.ecl_query()

        # LOINC
        if self.property == "property":
            self.property_rule()
        elif self.property == "timing":
            self.timing_rule()
        elif self.property == "system":
            self.system_rule()
        elif self.property == "component":
            self.component_rule()
        elif self.property == "scale":
            self.scale_rule()
        elif self.property == "method":
            self.method_rule()
        elif self.property == "class_type":
            self.class_type_rule()
        elif self.property == "order_or_observation":
            self.order_observation_rule()

        # FHIR
        if self.property == "has_fhir_terminology":
            self.has_fhir_terminology_rule()
        # Include entire code system rules
        if self.property == "include_entire_code_system":
            self.include_entire_code_system()

    def serialize(self):
        """
        Prepares a JSON representation to return to the API and returns the property, operator, and value of the rule

        :return: A dictionary containing the property, operator, and value of the rule.
        """
        return {"property": self.property, "op": self.operator, "value": self.value}


class UcumRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for UCUM specific value set rules.
    """

    def code_rule(self):
        """
        This method executes the code rule by querying the database for the codes provided in the rule's value.
        """
        conn = get_db()
        codes = self.value.replace(" ", "").split(",")

        # Get all descendants of the provided codes through a recursive query
        query = """
          select code from ucum.common_units
          where code in :codes 
          order by code
          """

        converted_query = text(query).bindparams(bindparam("codes", expanding=True))

        results_data = conn.execute(converted_query, {"codes": codes})
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.code)
            for x in results_data
        ]
        self.results = set(results)

    def include_entire_code_system(self):
        """
        This function gathers all UCUM codes into a value set
        """
        conn = get_db()
        query = """
        select code from ucum.common_units
        """
        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.code)
            for x in results_data
        ]
        self.results = set(results)


class ICD10CMRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for ICD-10-CM specific value set rules.
    """

    def direct_child(self):
        """
        This method executes the direct child rule by querying the database for the direct children of the provided code.
        """
        pass

    def code_rule(self):
        """
        This method executes the code rule by querying the database for the codes provided in the rule's value.
        """
        conn = get_db()
        query = ""

        if self.property == "code":
            # Lookup UUIDs for provided codes
            codes = self.value.replace(" ", "").split(",")

            # Get all descendants of the provided codes through a recursive query
            query = """
      select code, display from icd_10_cm.code 
      where code in :codes 
      and version_uuid=:version_uuid
      order by code
      """
            # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/

        converted_query = text(query).bindparams(bindparam("codes", expanding=True))

        results_data = conn.execute(
            converted_query,
            {"codes": codes, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def self_and_descendents(self):
        """
        Looks up the UUIDs for all codes and gets all descendants of the provided codes through a recursive query
        :return: Returns a set with the fhir_system, terminology version, code, and display.
        """
        conn = get_db()
        query = ""

        if self.property == "code":
            # Lookup UUIDs for provided codes
            codes = self.value.replace(" ", "").split(",")

            # Get all descendants of the provided codes through a recursive query
            query = """
      with recursive icd_hierarchy as (
        select parent_code_uuid parent_uuid, uuid child_uuid
        from icd_10_cm.code
        where parent_code_uuid in
        (select uuid
        from icd_10_cm.code
        where code in :codes
        and version_uuid=:version_uuid)
        union all
        select code.parent_code_uuid, code.uuid
        from icd_10_cm.code
        join icd_hierarchy on code.parent_code_uuid=icd_hierarchy.child_uuid
      )
      select code, display from icd_hierarchy
      join icd_10_cm.code
      on code.uuid=child_uuid
      union all
      (select code, display from icd_10_cm.code 
      where code in :codes 
      and version_uuid=:version_uuid)
      order by code
      """
        # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/

        converted_query = text(query).bindparams(bindparam("codes", expanding=True))

        results_data = conn.execute(
            converted_query,
            {"codes": codes, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def descendent_of(self):
        """
        Lookup UUIDs and gets all descendants of the provided codes through a recursive query
        :return: Returns a set with the fhir_system, terminology version, code, and display for the descendents.
        """
        conn = get_db()
        query = ""

        if self.property == "code":
            # Lookup UUIDs for provided codes
            codes = self.value.split(",")

            # Get all descendants of the provided codes through a recursive query
            query = """
      with recursive icd_hierarchy as (
        select parent_code_uuid parent_uuid, uuid child_uuid
        from icd_10_cm.code
        where parent_code_uuid in
        (select uuid
        from icd_10_cm.code
        where code in :codes
        and version_uuid=:version_uuid)
        union all
        select code.parent_code_uuid, code.uuid
        from icd_10_cm.code
        join icd_hierarchy on code.parent_code_uuid=icd_hierarchy.child_uuid
      )
      select code, display from icd_hierarchy
      join icd_10_cm.code
      on code.uuid=child_uuid
      """
            # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/

        converted_query = text(query).bindparams(bindparam("codes", expanding=True))

        results_data = conn.execute(
            converted_query,
            {"codes": codes, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def in_section(self):
        conn = get_db()

        query = """
      select * from icd_10_cm.code
      where section_uuid=:section_uuid
      and version_uuid = :version_uuid
    """

        results_data = conn.execute(
            text(query),
            {"section_uuid": self.value, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def in_chapter(self):
        conn = get_db()

        query = """
    select * from icd_10_cm.code
    where section_uuid in 
    (select uuid from icd_10_cm.section 
    where chapter = :chapter_uuid
    and version_uuid = :version_uuid)
    """

        results_data = conn.execute(
            text(query),
            {"chapter_uuid": self.value, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def include_entire_code_system(self):
        """
        This function gathers all ICD-10-CM codes.
        @return: A set of all ICD-10-CM codes by code and display.
        """
        conn = get_db()
        query = """
        select * from icd_10_cm.code 
        where version_uuid=:terminology_version_uuid
        """
        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)


class SNOMEDRule(VSRule):
    def concept_in(self):
        conn = get_db()
        query = """
    select * from snomedct.simplerefset_f
    join snomedct.concept_f
    on snomedct.concept_f.id=simplerefset_f.referencedcomponentid
    where refsetid=:value
    """

        results_data = conn.execute(text(query), {"value": self.value})
        results = [
            Code(
                self.fhir_system, self.terminology_version.version, x.conceptid, x.term
            )
            for x in results_data
        ]
        self.results = set(results)

    def ecl_query(self):
        """
        Executes an ECL query against our internal Snowstorm instance.
        Uses pagination to ensure all results are captured.
        Puts final results into self.results, per value set specs
        """
        self.results = set()
        results_complete = False
        search_after_token = None

        while results_complete is False:
            branch = "MAIN"

            params = {"ecl": self.value, "limit": SNOSTORM_LIMIT}
            if search_after_token is not None:
                params["searchAfter"] = search_after_token

            r = requests.get(
                f"{ECL_SERVER_PATH}/{branch}/{self.terminology_version.version}/concepts",
                params=params,
            )

            if "error" in r.json():
                raise BadRequest(r.json().get("message"))

            # Handle pagination
            if len(r.json().get("items")) == 0:
                results_complete = True
            search_after_token = r.json().get("searchAfter")

            # Add data to results
            data = r.json().get("items")
            results = [
                Code(
                    self.fhir_system,
                    self.terminology_version.version,
                    x.get("conceptId"),
                    x.get("fsn").get("term"),
                )
                for x in data
            ]
            self.results.update(set(results))


class RxNormRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for RxNorm specific value set rules.
    """

    def json_extract(self, obj, key):
        """Recursively fetch values from nested JSON."""

        def extract(obj, arr, key):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, arr, key)
                    elif k == key:
                        arr.append(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr, key)
            return arr

        arr = []
        values = extract(obj, arr, key)
        return values

    def load_rxnorm_properties(self, rxcui):
        return requests.get(f"{RXNORM_BASE_URL}rxcui/{rxcui}/properties.json?").json()

    def load_additional_members_of_class(self, rxcui):
        data = requests.get(f"{RXNORM_BASE_URL}rxcui/{rxcui}/allrelated.json?").json()
        return self.json_extract(data, "rxcui")

    def term_type_within_class(self):
        json_value = json.loads(self.value)
        rela_source = json_value.get("rela_source")
        class_id = json_value.get("class_id")
        term_type = json_value.get("term_type")

        # This calls the RxClass API to get its members
        payload = {"classId": class_id, "relaSource": rela_source}
        class_request = requests.get(
            f"{RXNORM_BASE_URL}rxclass/classMembers.json?", params=payload
        )

        # Extracts a list of RxCUIs from the JSON response
        rxcuis = self.json_extract(class_request.json(), "rxcui")

        # Calls the related info RxNorm API to get additional members of the drug class
        related_rxcuis = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            results = pool.map(self.load_additional_members_of_class, rxcuis)
            for result in results:
                related_rxcuis.append(result)

        # Appending RxCUIs to the first list of RxCUIs and removing empty RxCUIs
        flat_list = [item for sublist in related_rxcuis for item in sublist]
        de_duped_list = list(set(flat_list))
        if "" in de_duped_list:
            de_duped_list.remove("")
        rxcuis.extend(de_duped_list)

        # Calls the concept property RxNorm API
        concept_properties = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
            results = pool.map(self.load_rxnorm_properties, rxcuis)
            for result in results:
                concept_properties.append(result)

        # Making a final list of RxNorm codes
        final_rxnorm_codes = []
        for item in concept_properties:
            properties = item.get("properties")
            result_term_type = properties.get("tty")
            display = properties.get("name")
            code = properties.get("rxcui")
            if result_term_type in term_type:
                final_rxnorm_codes.append(
                    Code(
                        self.fhir_system,
                        self.terminology_version.version,
                        code,
                        display,
                    )
                )

        self.results = set(final_rxnorm_codes)

    def rxnorm_term_type(self):
        term_type = self.value.replace(",", " ")

        # Calls the getAllConceptsByTTY API
        payload = {"tty": term_type}
        tty_member_request = requests.get(
            f"{RXNORM_BASE_URL}allconcepts.json", params=payload
        )

        # New API call for RxNorm codes with a "quantified" status
        quantified_rxnorm_codes = requests.get(
            f"{RXNORM_BASE_URL}allstatus.json?status=quantified"
        )

        # Combine the two API responses into one set
        if tty_member_request.ok and quantified_rxnorm_codes.ok:
            concepts_data = (
                tty_member_request.json().get("minConceptGroup").get("minConcept")
            )
            status_data = (
                quantified_rxnorm_codes.json().get("minConceptGroup").get("minConcept")
            )

            # Combine the data from both responses
            combined_data = concepts_data + status_data
            results = [
                Code(
                    self.fhir_system,
                    self.terminology_version.version,
                    x.get("rxcui"),
                    x.get("name"),
                )
                for x in combined_data
            ]
            self.results = set(results)

    def all_active_rxnorm(self):
        """
        This function gathers all active RxNorm concepts.
        @return: A json of all active RxNorm concepts by rxcui and display.
        """
        self.results = set()

        r = requests.get(
            f"{RXNORM_BASE_URL}allstatus.json?status=Active",
        )
        # Add data to results
        data = r.json().get("minConceptGroup").get("minConcept")
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.get("rxcui"),
                x.get("name"),
            )
            for x in data
        ]
        self.results.update(set(results))


class LOINCRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for LOINC specific value set rules.
    """

    def codes_from_results(self, db_result):
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.loinc_num,
                x.long_common_name,
            )
            for x in db_result
        ]

        return set(results)

    def loinc_rule(self, query):
        conn = get_db()

        converted_query = text(query).bindparams(bindparam("value", expanding=True))
        results_data = conn.execute(
            converted_query,
            {
                "value": self.split_value,
                "terminology_version_uuid": self.terminology_version.uuid,
            },
        )
        self.results = self.codes_from_results(results_data)

    @property
    def split_value(self):
        """
        ReTool saves arrays like this: {"Alpha-1-Fetoprotein","Alpha-1-Fetoprotein Ab","Alpha-1-Fetoprotein.tumor marker"}
        Sometimes, we also save arrays like this: Alpha-1-Fetoprotein,Alpha-1-Fetoprotein Ab,Alpha-1-Fetoprotein.tumor marker

        This function will handle both formats also, newline character sequences  (LF, CR, and CRLF) by replacing them with a space, and output a python list of strings
        """
        new_value = self.value
        if new_value[:1] == "{" and new_value[-1:] == "}":
            new_value = new_value[1:-1]

        # Replace newline characters with a space
        new_value = new_value.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")

        # Using csv.reader to handle commas inside quotes
        reader = csv.reader([new_value])
        for row in reader:
            return row

    def code_rule(self):
        query = """
    select * from loinc.code
    where loinc_num in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def method_rule(self):
        query = """
    select * from loinc.code
    where method_typ in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def timing_rule(self):
        query = """
    select * from loinc.code
    where time_aspct in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def system_rule(self):
        query = """
    select * from loinc.code
    where system in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def component_rule(self):
        query = """
    select * from loinc.code
    where component in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def scale_rule(self):
        query = """
    select * from loinc.code
    where scale_typ in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def property_rule(self):
        query = """
    select * from loinc.code
    where property in :value
    and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def class_type_rule(self):
        query = """
            select * from loinc.code
            where classtype in :value
            and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
            and terminology_version_uuid=:terminology_version_uuid
            order by long_common_name
            """
        self.loinc_rule(query)

    def order_observation_rule(self):
        query = """
            select * from loinc.code
            where order_obs in :value
            and status in ('ACTIVE', 'DISCOURAGED', 'TRIAL')
            and terminology_version_uuid=:terminology_version_uuid
            order by long_common_name
            """
        self.loinc_rule(query)

    def include_entire_code_system(self):
        """
        This function returns all LOINC codes.
        @return: A set of all LOINC codes by number and long common name.
        """
        conn = get_db()
        query = """
        select * from loinc.code 
        where terminology_version_uuid=:terminology_version_uuid
        and status != 'DEPRECATED'
        """
        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        self.results = self.codes_from_results(results_data)


class ICD10PCSRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for ICD-10-PCS specific value set rules.
    """

    def icd_10_pcs_rule(self, query):
        conn = get_db()

        converted_query = text(query).bindparams(bindparam("value", expanding=True))

        value_param = self.value
        if type(self.value) != list:
            value_param = json.loads(value_param)

        results_data = conn.execute(
            converted_query,
            {"value": value_param, "version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def code_rule(self):
        query = """
    select * from icd_10_pcs.code
    where code in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def in_section(self):
        query = """
    select * from icd_10_pcs.code
    where section in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_body_system(self):
        query = """
    select * from icd_10_pcs.code
    where body_system in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_root_operation(self):
        query = """
    select * from icd_10_pcs.code
    where root_operation in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_body_part(self):
        query = """
    select * from icd_10_pcs.code
    where body_part in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_approach(self):
        query = """
    select * from icd_10_pcs.code
    where approach in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_device(self):
        query = """
    select * from icd_10_pcs.code
    where device in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)

    def has_qualifier(self):
        query = """
    select * from icd_10_pcs.code
    where qualifier in :value
    and version_uuid = :version_uuid
    """
        self.icd_10_pcs_rule(query)


class CPTRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for CPT specific value set rules.
    """

    @staticmethod
    def parse_cpt_retool_array(retool_array):
        array_string_copy = retool_array
        array_string_copy = array_string_copy[1:]
        array_string_copy = array_string_copy[:-1]
        array_string_copy = "[" + array_string_copy + "]"
        python_array = json.loads(array_string_copy)
        return [json.loads(x) for x in python_array]

    def parse_input_array(self, input_array):
        try:
            if type(input_array) == list:
                return input_array
            elif type(input_array) == str:
                return json.loads(input_array)
        except:  # uncaught exceptions can be so costly here, that a 'bare except' is acceptable, despite PEP 8: E722
            return self.parse_cpt_retool_array(input_array)

    def parse_code_number_and_letter(self, code):
        if code.isnumeric():
            code_number = code
            code_letter = None
        else:
            code_number = code[:-1]
            code_letter = code[-1]
        return code_number, code_letter

    def code_rule(self):
        """Process CPT rules where property=code and operator=in, where we are selecting codes from a range"""
        parsed_value = self.parse_input_array(self.value)

        # Input may be list of dicts with a 'range' key, or may be list of ranges directly
        if type(parsed_value[0]) == dict:
            ranges = [x.get("range") for x in parsed_value]
        else:
            ranges = [x for x in parsed_value]

        # Since each range in the above array may include multiple ranges, we need to re-join them and then split them apart
        ranges = ",".join(ranges)
        ranges = ranges.replace(" ", "")
        ranges = ranges.split(",")

        where_clauses = []

        for x in ranges:
            if "-" in x:
                start, end = x.split("-")
                start_number, start_letter = self.parse_code_number_and_letter(start)
                end_number, end_letter = self.parse_code_number_and_letter(end)

                if start_letter != end_letter:
                    raise Exception(
                        f"Letters in CPT code range do not match: {start_letter} and {end_letter}"
                    )

                where_clauses.append(
                    f"(code_number between {start_number} and {end_number} and code_letter {'=' if start_letter is not None else 'is'} {start_letter if start_letter is not None else 'null'})"
                )
            else:
                code_number, code_letter = self.parse_code_number_and_letter(x)
                where_clauses.append(
                    f"(code_number={code_number} and code_letter {'=' if code_letter is not None else 'is'} {code_letter if code_letter is not None else 'null'})"
                )

        query = "select * from cpt.code where " + " or ".join(where_clauses)

        conn = get_db()
        results_data = conn.execute(text(query))
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.code,
                x.long_description,
            )
            for x in results_data
        ]
        self.results = set(results)

    # def display_regex(self):
    #     """Process CPT rules where property=display and operator=regex, where we are string matching to displays"""
    #     es = get_elasticsearch()
    #
    #     results = es.search(
    #         query={
    #             "simple_query_string": {
    #                 "fields": ["display"],
    #                 "query": self.value,
    #             }
    #         },
    #         index="cpt_codes",
    #         size=MAX_ES_SIZE,
    #     )
    #
    #     search_results = [x.get("_source") for x in results.get("hits").get("hits")]
    #     final_results = [
    #         Code(
    #             self.fhir_system,
    #             self.terminology_version.version,
    #             x.get("code"),
    #             x.get("display"),
    #         )
    #         for x in search_results
    #     ]
    #     self.results = set(final_results)

    def include_entire_code_system(self):
        """
        This function gathers all CPT codes.
        @return: A set of all CPT codes by code and long description.
        """
        conn = get_db()
        query = """
        select * from cpt.code 
        where version_uuid=:terminology_version_uuid
        """
        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.code,
                x.long_description,
            )
            for x in results_data
        ]
        self.results = set(results)


class FHIRRule(VSRule):
    """
    This class inherits from the VSRule class and provides implementation for FHIR specific value set rules.
    """

    def has_fhir_terminology_rule(self):
        conn = get_db()
        query = """
    select * from fhir_defined_terminologies.code_systems_new
    where terminology_version_uuid=:terminology_version_uuid
    """
        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def code_rule(self):
        conn = get_db()
        query = ""

        if self.property == "code":
            # Lookup UUIDs for provided codes
            codes = self.value.replace(" ", "").split(",")

            # Get provided codes through a recursive query
            query = """
          select code, display from fhir_defined_terminologies.code_systems_new 
          where code in :codes 
          and terminology_version_uuid=:terminology_version_uuid
          order by code
          """
            # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/

        converted_query = text(query).bindparams(
            bindparam("codes", expanding=True), bindparam("terminology_version_uuid")
        )

        results_data = conn.execute(
            converted_query,
            {"codes": codes, "terminology_version_uuid": self.terminology_version.uuid},
        )
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)


class CustomTerminologyRule(VSRule):
    def codes_from_results(self, db_result):
        results = set()
        for x in db_result:
            # Prepare the data for each code in db_result
            code_data = {
                "system": self.fhir_system,
                "version": self.terminology_version.version,
                "code_schema": x.code_schema,
                "code_simple": x.code_simple,
                "code_jsonb": x.jsonb,
                "display": x.display,
                "depends_on_property": x.depends_on_property,
                "depends_on_system": x.depends_on_system,
                "depends_on_value": x.depends_on_value,
                "depends_on_display": x.depends_on_display,
                "custom_terminology_code_uuid": x.uuid,
            }

            # Use the create_code_object method to instantiate the correct object
            code_object = Code.create_code_or_codeable_concept(code_data)

            # Add the instantiated object to the results set
            results.add(code_object)

        return results

    def include_entire_code_system(self):
        self.terminology_version.load_content()
        codes = self.terminology_version.codes
        return set(codes)

    def display_regex(self):
        conn = get_db()
        query = """
        select * from custom_terminologies.code_poc 
        where terminology_version_uuid=:terminology_version_uuid
        and display like :value
        """
        results_data = conn.execute(
            text(query),
            {
                "terminology_version_uuid": self.terminology_version.uuid,
                "value": self.value,
            },
        )

        self.results = self.codes_from_results(results_data)

    def code_rule(self):
        conn = get_db()
        query = """
        select * from custom_terminologies.code_poc
        where code in :value
        and terminology_version_uuid=:terminology_version_uuid
        """
        converted_query = text(query).bindparams(bindparam("value", expanding=True))
        results_data = conn.execute(
            converted_query,
            {
                "terminology_version_uuid": self.terminology_version.uuid,
                "value": [x.strip() for x in self.value.split(",")],
            },
        )

        self.results = self.codes_from_results(results_data)


#
# End of Value Set Rules
#

# class GroupingValueSetRule(VSRule):
#   def most_recent_active_version(self):
#     version = ValueSet.load_most_recent_active_version(name)
#     version.expand()
#     results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
#     self.results = set(results)

#   def specific_version(self):
#     pass


class ValueSet:
    """A class that represents a value set and provides methods to create, load and manipulate value sets.

    Attributes:
        uuid: str, the unique identifier for the value set.
        name: str, the name of the value set. (machine-readable name)
        title: str, the title of the value set. (FHIR's place to put human-readable)
        description: str, a human-readable description of the value set to be displayed in the UI
        immutable: bool, specifies if the value set is immutable.
        experimental: bool, specifies if the value set is experimental.
        type: str, the type of the value set.
        synonyms: dict, the synonyms of the value set.
        database_schema_version (int): The current output schema version for ValueSet JSON files in OCI.
        next_schema_version (int): The pending next output schema version for ValueSet JSON files in OCI.
        object_storage_folder_name (str): "ValueSets" folder name for OCI storage, for easy retrieval by utilities.

    Methods:
        __init__(self, uuid, name, title, publisher, contact, description, immutable, experimental, purpose, vs_type, synonyms={}):
            Initializes the value set with the given attributes.
        create(cls, name, title, publisher, contact, value_set_description, immutable, experimental, purpose, vs_type, effective_start, effective_end, version_description, use_case_uuid=None):
            Creates a new value set with the given attributes and adds it to the database.
        load(cls, vs_uuid):
            Loads a value set with the given uuid.
        serialize(self):
            Returns the serialized version of the value set.
        delete(self):
            Deletes the value set from the database.
        load_all_value_set_metadata(cls, active_only=True):
            Loads all value sets metadata from the database.

    """

    database_schema_version = 2
    next_schema_version = 5
    object_storage_folder_name = "ValueSets"

    def __init__(
        self,
        uuid,
        name,
        title,
        publisher,
        contact,
        description,
        immutable,
        experimental,
        purpose,
        vs_type,
        synonyms={},
        primary_use_case=None,
        secondary_use_cases=[],
    ):
        """Initializes the value set with the given attributes.

        Args:
            uuid (str): The unique identifier for the value set.
            name (str): The name of the value set.
            title (str): The title of the value set.
            publisher (str): The publisher of the value set.
            contact (str): The contact information for the value set.
            description (str): The description of the value set.
            immutable (bool): Specifies if the value set is immutable.
            experimental (bool): Specifies if the value set is experimental.
            purpose (str): The purpose of the value set.
            vs_type (str): The type of the value set.
            synonyms (dict, optional): The synonyms of the value set. Defaults to {}.
        """
        self.uuid = uuid
        self.name = name
        self.title = title
        self.publisher = publisher
        self.contact = contact
        self.description = description
        self.immutable = immutable
        self.experimental = experimental
        if self.experimental == 1:
            self.experimental = True
        if self.experimental == 0:
            self.experimental = False
        self.purpose = purpose
        self.type = vs_type
        self.synonyms = synonyms
        self.primary_use_case = primary_use_case
        self.secondary_use_cases = secondary_use_cases

    @classmethod
    def create(
        cls,
        name,
        title,
        publisher,
        contact,
        value_set_description,
        immutable,
        experimental,
        purpose,
        vs_type,
        effective_start,
        effective_end,
        version_description,
        primary_use_case=None,
        secondary_use_case=[],
    ):
        """
        Class method to create a new ValueSet entry in the database.

        This method generates a new UUID, creates an entry in the `value_sets.value_set` table with the provided attributes,
        sets up the link between the value set and its use cases, creates a new version of the
        value set in the `value_sets.value_set_version` table, and returns the loaded value set.

        Raises any database exceptions to the caller

        Parameters
        ----------
        name : str
            The name of the ValueSet.
        title : str
            The title of the ValueSet.
        publisher : str
            The publisher of the ValueSet.
        contact : str
            The contact information for the ValueSet.
        value_set_description : str
            The description of the ValueSet.
        immutable : bool
            Specifies whether the ValueSet is immutable.
        experimental : bool
            Specifies whether the ValueSet is experimental.
        purpose : str
            The purpose of the ValueSet.
        vs_type : str
            The type of the ValueSet.
        effective_start : datetime
            The effective start date of the ValueSet version.
        effective_end : datetime
            The effective end date of the ValueSet version.
        version_description : str
            The description of the ValueSet version.
        primary_use_case : str, optional
            The primary use case of the ValueSet.
        secondary_use_case : list, optional
            The secondary use cases of the ValueSet.

        Returns
        -------
        ValueSet
            The newly created ValueSet instance loaded from the database.

        """
        conn = get_db()
        vs_uuid = uuid.uuid4()

        # Insert the value_set into the value_sets.value_set table
        try:
            conn.execute(
                text(
                    """  
                    insert into value_sets.value_set  
                    (uuid, name, title, publisher, contact, description, immutable, experimental, purpose, type)  
                    values  
                    (:uuid, :name, :title, :publisher, :contact, :value_set_description, :immutable, :experimental, :purpose, :vs_type)  
                    """
                ),
                {
                    "uuid": vs_uuid,
                    "name": name,
                    "title": title,
                    "publisher": publisher,
                    "contact": contact,
                    "value_set_description": value_set_description,
                    "immutable": immutable,
                    "experimental": experimental,
                    "purpose": purpose,
                    "vs_type": vs_type,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Call to insert the value_set and use_case associations into the value_sets.value_set_use_case_link table
        cls.value_set_use_case_link_set_up(
            primary_use_case, secondary_use_case, vs_uuid
        )

        # Insert the value_set_version into the value_sets.value_set_version table
        new_version_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """  
                    insert into value_sets.value_set_version  
                    (uuid, effective_start, effective_end, value_set_uuid, status, description, created_date, version)  
                    values  
                    (:new_version_uuid, :effective_start, :effective_end, :value_set_uuid, :status, :version_description, :created_date, :version)  
                    """
                ),
                {
                    "new_version_uuid": new_version_uuid,
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                    "value_set_uuid": vs_uuid,
                    "status": "pending",
                    "version_description": version_description,
                    "created_date": datetime.now(),
                    "version": 1,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        return cls.load(vs_uuid)

    @classmethod
    def load(cls, vs_uuid):
        if vs_uuid is None:
            raise Exception("Cannot load a Value Set with None as uuid")

        conn = get_db()
        vs_data = conn.execute(
            text(
                """  
            select * from value_sets.value_set where uuid=:uuid  
            """
            ),
            {"uuid": vs_uuid},
        ).first()

        if vs_data is None:
            raise NotFoundException(f"No Value Set found with UUID: {vs_uuid}")

        synonym_data = conn.execute(
            text(
                """  
            select context, synonym  
            from resource_synonyms  
            where resource_uuid=:uuid  
            """
            ),
            {"uuid": vs_uuid},
        )
        synonyms = {x.context: x.synonym for x in synonym_data}

        primary_use_case, secondary_use_cases = load_use_case_by_value_set_uuid(vs_uuid)

        value_set = cls(
            vs_data.uuid,
            vs_data.name,
            vs_data.title,
            vs_data.publisher,
            vs_data.contact,
            vs_data.description,
            vs_data.immutable,
            vs_data.experimental,
            vs_data.purpose,
            vs_data.type,
            synonyms,
            primary_use_case,
            secondary_use_cases,
        )
        return value_set

    def serialize(self):

        use_case_info = load_use_case_by_value_set_uuid(self.uuid)
        # Extract the names from use_case_info
        use_case_names = []
        # Check if primary_use_case exists and if so, add its name to use_case_names
        primary_use_case = use_case_info.get("primary_use_case")
        if primary_use_case is not None:
            use_case_names.append(primary_use_case.name)

        # Check if secondary_use_cases exists and if so, iterate through and add their names to use_case_names
        secondary_use_cases = use_case_info.get("secondary_use_cases")
        if secondary_use_cases:
            for use_case in secondary_use_cases:
                use_case_names.append(use_case.name)
        return {
            "uuid": self.uuid,
            "name": self.name,
            "title": self.title,
            "publisher": self.publisher,
            "contact": self.contact,
            "description": self.description,
            "immutable": self.immutable,
            "experimental": self.experimental,
            "purpose": self.purpose,
            "type": self.type,
            "use case names": use_case_names,
        }

    def delete(self):
        conn = get_db()
        # check for a version
        vs_version_data = conn.execute(
            text(
                """
            select * from value_sets.value_set_version where value_set_uuid=:uuid
            """
            ),
            {"uuid": str(self.uuid)},
        ).first()

        use_case_info = load_use_case_by_value_set_uuid(self.uuid)
        # Extract the names from use_case_info
        use_case_names = [uc.name for uc in use_case_info] if use_case_info else []

        # reject request if has version
        if vs_version_data is not None:
            raise BadRequest(
                "ValueSet is not eligible for deletion because there is an associated version"
            )
        # reject request if has use case
        elif len(use_case_names) > 0:
            raise BadRequest(
                "ValueSet is not eligible for deletion because there are associated uses cases"
            )
        else:
            conn.execute(
                text(
                    """
                    delete from value_sets.value_set
                    where uuid=:uuid
                    """
                ),
                {"uuid": self.uuid},
            )

    @classmethod
    def load_all_value_set_metadata(cls, active_only=True):
        conn = get_db()

        if active_only is True:
            results = conn.execute(
                text(
                    """
                select * from value_sets.value_set
                where uuid in 
                (select value_set_uuid from value_sets.value_set_version
                where status='active')
                """
                )
            )
        else:
            results = conn.execute(
                text(
                    """
                select * from value_sets.value_set
                where uuid in 
                (select value_set_uuid from value_sets.value_set_version)
                """
                )
            )
        value_sets_metadata = []
        for x in results:
            use_case_info = load_use_case_by_value_set_uuid(x.uuid)
            # Extract the names from use_case_info
            use_case_names = (
                (
                    (
                        [use_case_info["primary_use_case"].name]
                        if use_case_info["primary_use_case"]
                        else []
                    )
                    + [uc.name for uc in use_case_info["secondary_use_cases"]]
                )
                if use_case_info
                else []
            )

            value_sets_metadata.append(
                {
                    "uuid": x.uuid,
                    "name": x.name,
                    "title": x.title,
                    "publisher": x.publisher,
                    "contact": x.contact,
                    "description": x.description,
                    "immutable": x.immutable,
                    "experimental": x.experimental,
                    "purpose": x.purpose,
                    "type": x.type,
                    "use_case_names": use_case_names,  # Add use_case_names to the dictionary
                }
            )

        return value_sets_metadata

    @classmethod
    def load_all_value_sets_by_status(cls, status):
        conn = get_db()
        query = text(
            """
            select uuid from value_sets.value_set_version
            where status in :status
            """
        ).bindparams(bindparam("status", expanding=True))
        results = conn.execute(query, {"status": status})

        return [ValueSetVersion.load(x.uuid) for x in results]

    @classmethod
    def name_to_uuid(cls, identifier):
        """Returns the UUID for a ValueSet, given either a name or UUID"""
        try:
            return uuid.UUID(identifier)
        except ValueError:
            conn = get_db()
            result = conn.execute(
                text(
                    """
                    select uuid, name from value_sets.value_set
                    where name=:name
                    """
                ),
                {"name": identifier},
            ).first()
            return result.uuid

    @classmethod
    def load_version_metadata(cls, uuid):
        """
        Result may be an empty list
        """
        conn = get_db()
        results = conn.execute(
            text(
                """
            select * from value_sets.value_set_version
            where value_set_uuid = :uuid
            order by version desc
            """
            ),
            {"uuid": str(uuid)},
        )
        return [
            {
                "uuid": x.uuid,
                "effective_start": x.effective_start,
                "effective_end": x.effective_end,
                "status": x.status,
                "description": x.description,
                "created_date": x.created_date,
                "version": x.version,
            }
            for x in results
        ]

    @classmethod
    @ttl_cache()
    def load_most_recent_active_version_with_cache(cls, uuid):
        return cls.load_most_recent_active_version(uuid)

    @classmethod
    def load_most_recent_active_version(cls, uuid):
        return cls.load_most_recent_version(uuid, active_only=True)

    @classmethod
    def load_most_recent_version(cls, uuid, active_only=False):
        conn = get_db()
        if active_only:
            query = text(
                """
                select * from value_sets.value_set_version
                where value_set_uuid = :uuid
                and status='active'
                order by version desc
                limit 1
                """
            )
        else:
            query = text(
                """
                select * from value_sets.value_set_version
                where value_set_uuid = :uuid
                order by version desc
                limit 1
                """
            )
        try:
            results = conn.execute(query, {"uuid": uuid})
        except DatabaseError:
            raise NotFoundException(
                f"Database unavailable while seeking ValueSet with UUID: {uuid}"
            )
        recent_version = results.first()
        if recent_version is None:
            message = f"No published version of ValueSet with UUID: {uuid}"
            if active_only:
                message = f"No active published version of ValueSet with UUID: {uuid}"
            raise BadRequest(message)
        return ValueSetVersion.load(recent_version.uuid)

    def duplicate_vs(
        self,
        name,
        title,
        contact,
        value_set_description,
        purpose,
        effective_start,
        effective_end,
        version_description,
        use_case_uuid=None,
    ):
        """
        Raises NotFoundException if the ValueSet to be duplicated, cannot be found.
        Raises any database exceptions to the caller
        """
        conn = get_db()
        # create new value set uuid
        new_vs_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.value_set
                    (uuid, name, title, publisher, contact, description, immutable, experimental, purpose, type, use_case_uuid)
                    select :new_vs_uuid, :name, :title, publisher, :contact, :value_set_description, immutable, experimental, :purpose, type, :use_case_uuid
                    from value_sets.value_set
                    where uuid = :old_uuid
                    """
                ),
                {
                    "new_vs_uuid": str(new_vs_uuid),
                    "name": name,
                    "title": title,
                    "contact": contact,
                    "value_set_description": value_set_description,
                    "purpose": purpose,
                    "use_case_uuid": use_case_uuid,
                    "old_uuid": self.uuid,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # get the most recent active version of the value set being duplicated
        most_recent_vs_version = conn.execute(
            text(
                """
                select * from value_sets.value_set_version
                where value_set_uuid=:value_set_uuid
                order by version desc
                """
            ),
            {"value_set_uuid": self.uuid},
        ).first()
        if most_recent_vs_version is None:
            raise NotFoundException(f"No Value Set found with UUID: {self.uuid}")

        # create version in the newly duplicated value set
        new_version_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.value_set_version
                    (uuid, effective_start, effective_end, value_set_uuid, status, description, created_date, version)
                    values
                    (:new_version_uuid, :effective_start, :effective_end, :value_set_uuid, :status, :description, :created_date, :version)
                    """
                ),
                {
                    "new_version_uuid": str(new_version_uuid),
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                    "value_set_uuid": new_vs_uuid,
                    "status": "pending",
                    "description": version_description,
                    "created_date": datetime.now(),
                    "version": 1,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Copy rules from original value set most recent active version into new duplicate version
        if current_app.config["MOCK_DB"] is False:
            try:
                conn.execute(
                    text(
                        """
                        insert into value_sets.value_set_rule
                        (position, description, property, operator, value, include, terminology_version, value_set_version)
                        select position, description, property, operator, value, include, terminology_version, :new_version_uuid
                        from value_sets.value_set_rule
                        where value_set_version = :previous_version_uuid
                        """
                    ),
                    {
                        "previous_version_uuid": str(most_recent_vs_version.uuid),
                        "new_version_uuid": str(new_version_uuid),
                    },
                )
            except Exception as e:
                conn.rollback()
                raise e

        return new_vs_uuid

    def create_new_version_from_previous(
        self, effective_start, effective_end, description
    ):
        """
        This will identify the most recent version of the value set and clone it, incrementing the version by 1,
        to create a new version

        Raises NotFoundException if the ValueSet data is not found.
        Raises any database exceptions to the caller
        """
        conn = get_db()
        most_recent_vs_version = conn.execute(
            text(
                """
                select * from value_sets.value_set_version
                where value_set_uuid=:value_set_uuid
                order by version desc
                """
            ),
            {"value_set_uuid": self.uuid},
        ).first()
        if most_recent_vs_version is None:
            raise NotFoundException(f"No Value Set found with UUID: {self.uuid}")

        # Create new version
        new_version_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.value_set_version
                    (uuid, effective_start, effective_end, value_set_uuid, status, description, created_date, version)
                    values
                    (:new_version_uuid, :effective_start, :effective_end, :value_set_uuid, :status, :description, :created_date, :version)
                    """
                ),
                {
                    "new_version_uuid": str(new_version_uuid),
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                    "value_set_uuid": self.uuid,
                    "status": "pending",
                    "description": description,
                    "created_date": datetime.now(),
                    "version": most_recent_vs_version.version + 1,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Copy rules from previous version to new version
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.value_set_rule
                    (position, description, property, operator, value, include, terminology_version, value_set_version, rule_group)
                    select position, description, property, operator, value, include, terminology_version, :new_version_uuid, rule_group
                    from value_sets.value_set_rule
                    where value_set_version = :previous_version_uuid
                    """
                ),
                {
                    "previous_version_uuid": str(most_recent_vs_version.uuid),
                    "new_version_uuid": str(new_version_uuid),
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Copy over mapping inclusions
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.mapping_inclusion
                    (concept_map_uuid, relationship_types, match_source_or_target, concept_map_name, vs_version_uuid)
                    select concept_map_uuid, relationship_types, match_source_or_target, concept_map_name, :new_value_set_version_uuid from value_sets.mapping_inclusion
                    where vs_version_uuid=:previous_version_uuid
                    """
                ),
                {
                    "new_value_set_version_uuid": str(new_version_uuid),
                    "previous_version_uuid": str(most_recent_vs_version.uuid),
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Copy over explicitly included codes
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.explicitly_included_code
                    (vs_version_uuid, code_uuid, review_status)
                    select :new_value_set_version_uuid, code_uuid, review_status from value_sets.explicitly_included_code
                    where vs_version_uuid = :previous_version_uuid
                    """
                ),
                {
                    "new_value_set_version_uuid": str(new_version_uuid),
                    "previous_version_uuid": str(most_recent_vs_version.uuid),
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        return new_version_uuid

    def perform_terminology_update(
        self,
        old_terminology_version_uuid,
        new_terminology_version_uuid,
        effective_start,
        effective_end,
        description,
    ):
        """
        This function performs a terminology update on a value set by creating a new version of the value set with
        updated terminology rules. It first checks if the highest version of the value set is active and has not been
        updated already. If the conditions are met, it creates a new version of the value set and updates the rules
        to target the new terminology version. The function then expands the previous and new value set versions and
        calculates the diff between them. Finally, it updates the status of the new value set version based on the
        differences in codes.

        If exceptions occur while attempting to update the database, these are raised to the caller.
        If exceptions occur while gathering informational data, they are noted by name in the report, without details.

        Args:
        old_terminology_version_uuid (str): The UUID of the old terminology version to be replaced.
        new_terminology_version_uuid (str): The UUID of the new terminology version to replace the old one.
        effective_start (str): The effective start date for the new value set version.
        effective_end (str): The effective end date for the new value set version.
        description (str): A description of the new value set version.

        Returns:
        str: A string representing the status of the new value set version, which can be one of the following:
        - "already_updated": The value set has already been updated with the new terminology version.
        - "latest_version_not_active": The latest version of the value set is not active.
        - "failed_to_create_new": An exception occurred while creating a new value set version from previous.
        - "failed_to_update_rules": An exception occurred while updating rules for terminologies.
        - "failed_to_expand": An exception occurred while expanding the new value set version.
        - "failed_to_diff_versions": An exception occurred while comparing old and new value set versions.
        - "reviewed": The new value set version has no differences in codes and is marked as reviewed.
        - "pending": The new value set version has differences in codes and is marked as pending.
        """
        if old_terminology_version_uuid is None or new_terminology_version_uuid is None:
            raise NotFoundException(
                f"Unable to compare Terminology with UUID: {old_terminology_version_uuid} "
                + f"to Terminology with UUID: {new_terminology_version_uuid}"
            )

        value_set_metadata = ValueSet.load_version_metadata(self.uuid)
        if len(value_set_metadata) == 0:
            raise NotFoundException(
                f"No versions found for Value Set with UUID: {self.uuid}"
            )
        sorted_versions = sorted(
            value_set_metadata, key=lambda x: x["version"], reverse=True
        )
        highest_version = sorted_versions[0]

        # Check to see if it's already been updated
        most_recent_version = ValueSetVersion.load(highest_version.get("uuid"))
        if most_recent_version.contains_content_from_terminology(
            new_terminology_version_uuid
        ):  # safely returns True or False
            return "already_updated"

        # Safety check: Raise an exception if the highest version does not have a status of 'active'
        # (we will not auto-update pending value sets still being worked on)
        if most_recent_version.status != "active":
            return "latest_version_not_active"

        # Create a new version of the value set
        try:
            new_value_set_version_uuid = self.create_new_version_from_previous(
                effective_start=effective_start,
                effective_end=effective_end,
                description=description,
            )
        except Exception as e:
            return f"failed_to_create_new: {message_exception_classname(e)}"
        new_value_set_version = ValueSetVersion.load(new_value_set_version_uuid)

        # Update the rules targeting the previous version to target the new version
        try:
            new_value_set_version.update_rules_for_terminology(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
            )
        except Exception as e:
            return f"failed_to_update_rules: {message_exception_classname(e)}"

        # Expand the previous and new value set versions
        try:
            most_recent_version.expand()
            new_value_set_version.expand(force_new=True)
        except Exception as e:
            return f"failed_to_expand: {message_exception_classname(e)}"

        # Get the diff between the two value set versions
        try:
            diff = ValueSetVersion.diff_for_removed_and_added_codes(
                most_recent_version.uuid,
                new_value_set_version_uuid,
            )
        except Exception as e:
            return f"failed_to_diff_versions: {message_exception_classname(e)}"

        # Update the status of the new value set version
        if not diff["removed_codes"] and not diff["added_codes"]:
            new_value_set_version.update(status="reviewed")
            return "reviewed"
        else:
            new_value_set_version.update(status="pending")
            return "pending"

    @staticmethod
    def value_set_use_case_link_set_up(
        primary_use_case, secondary_use_cases, value_set_uuid
    ):
        """
        Raises any database exceptions to the caller
        """
        # Insert the value_set and use_case associations into the value_sets.value_set_use_case_link table
        if primary_use_case is not None:
            UseCase.save_value_set_link(
                primary_use_case, value_set_uuid, is_primary=True
            )

        for secondary_use_case in secondary_use_cases:
            UseCase.save_value_set_link(
                secondary_use_case, value_set_uuid, is_primary=False
            )

    @staticmethod
    def get_value_sets_from_use_case(use_case_uuid):
        """
        Retrieves all value sets associated with a specific use case.

        This static method retrieves all value set rows from the 'value_sets.value_set' table
        that are linked to the specified use case via the 'value_sets.value_set_use_case_link' table.

        Parameters
        ----------
        use_case_uuid : uuid.UUID
            The unique identifier of the use case to retrieve associated value sets for.

        Returns
        -------
        list of ValueSet
            A list of ValueSet instances, each representing a value set associated with the specified use case.
            If the use case is not linked to any value sets, the returned list is empty.

        Raises
        ------
        Any exceptions raised by the database operation will propagate up to the caller.

        Notes
        -----
        The returned value sets are created as instances of the ValueSet class, with the attributes
        of each instance populated from the corresponding row in the 'value_sets.value_set' table.
        The database connection is obtained from the `get_db` function.
        """
        conn = get_db()
        query = conn.execute(
            text(
                """
                    SELECT vs.*
                    FROM value_sets.value_set AS vs
                    INNER JOIN value_sets.value_set_use_case_link AS link
                    ON vs.uuid = link.value_set_uuid
                    WHERE link.use_case_uuid =:use_case_uuid
                """
            ),
            {"use_case_uuid": use_case_uuid},
        )
        value_sets_associated = query.fetchall()
        value_sets = [
            ValueSet(
                uuid=value_set_item.uuid,
                name=value_set_item.name,
                title=value_set_item.title,
                publisher=value_set_item.publisher,
                contact=value_set_item.contact,
                description=value_set_item.description,
                immutable=value_set_item.immutable,
                experimental=value_set_item.experimental,
                purpose=value_set_item.purpose,
                vs_type=value_set_item.type,
            )
            for value_set_item in value_sets_associated
        ]
        return value_sets


class RuleGroup:
    def __init__(self, vs_version_uuid, rule_group_id):
        self.vs_version_uuid = vs_version_uuid
        self.rule_group_id = rule_group_id
        self.expansion = set()
        self.rules = {}
        self.load_rules()

    # Move load rules to here, at version level, just load distinct rule groups and instantiate this class
    def load_rules(self):
        """
        Rules will be structured as a dictionary where each key is a terminology
        and the value is a list of rules for that terminology within this value set version.
        """
        conn = get_db()

        terminologies = Terminology.load_terminologies_for_value_set_version(
            self.vs_version_uuid
        )

        rules_data = conn.execute(
            text(
                """
            select value_set_rule.uuid as rule_uuid, * 
            from value_sets.value_set_rule 
            join terminology_versions
            on terminology_version=terminology_versions.uuid
            where value_set_version=:vs_version
            and rule_group=:rule_group
            """
            ),
            {"vs_version": self.vs_version_uuid, "rule_group": self.rule_group_id},
        )

        for x in rules_data:
            terminology = terminologies.get(x.terminology_version)
            rule = None

            if terminology.name == "ICD-10 CM":
                rule = ICD10CMRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "SNOMED CT":
                rule = SNOMEDRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "RxNorm":
                rule = RxNormRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "LOINC":
                rule = LOINCRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "CPT":
                rule = CPTRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "ICD-10 PCS":
                rule = ICD10PCSRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.name == "UCUM":
                rule = UcumRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.fhir_terminology == True:
                rule = FHIRRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            elif terminology.fhir_terminology == False:
                rule = CustomTerminologyRule(
                    x.rule_uuid,
                    x.position,
                    x.description,
                    x.property,
                    x.operator,
                    x.value,
                    x.include,
                    self,
                    x.fhir_uri,
                    terminologies.get(x.terminology_version),
                )
            if terminology in self.rules:
                self.rules[terminology].append(rule)
            else:
                self.rules[terminology] = [rule]

    # Move execute, so that the logic previously kept at a version level is now at a rule group level
    def generate_expansion(self):
        """
        Calculates and returns the set of codes that belong to a particular group of rules.

        The method first initializes an empty set called expansion to store the codes that belong to the group.
        It then extracts a list of terminologies (objects representing different medical terminologies)
        from a dictionary called self.rules, where the keys of the dictionary are the terminologies and
        the values are lists of rules associated with those terminologies.

        Next, the method loops through each terminology and its associated rules, executing each rule
        and dividing them into two lists: one for rules that are marked as "inclusion" rules
        and another for rules that are marked as "exclusion" rules.
        For each list, the method prints a description of the rules it contains.

        The method then performs a series of set operations on the codes that are identified
        by the inclusion and exclusion rules. It starts by intersecting the results
        of the first inclusion rule with the results of each subsequent inclusion rule,
        effectively creating a single set of codes that satisfy all of the inclusion rules.

        It then subtracts the codes identified by the exclusion rules from this set, one rule at a time,
        to produce the final set of codes for the terminology.

        The method then updates the expansion set with the codes for the current terminology and
        repeats the process for each additional terminology. When all terminologies have been processed,
        the method returns the expansion set and a report of the expansion process.
        """
        self.expansion = set()
        terminologies = self.rules.keys()
        expansion_report = f"EXPANDING RULE GROUP {self.rule_group_id}\n"

        for terminology in terminologies:
            expansion_report += f"\nProcessing rules for terminology {terminology.name} version {terminology.version}\n"

            rules = self.rules.get(terminology)
            errors = []  # list to hold error messages

            for rule in rules:
                try:
                    rule.execute()
                except BadRequest as e:
                    if e.code == 400:
                        errors.append(f"{e.description}")
                    else:
                        raise e

            include_rules = [x for x in rules if x.include is True]
            exclude_rules = [x for x in rules if x.include is False]

            expansion_report += "\nInclusion Rules\n"
            for x in include_rules:
                expansion_report += f"{x.description}, {x.property}, {x.operator}, {x.value}, {len(x.results)} codes included\n"
            expansion_report += "\nExclusion Rules\n"
            for x in exclude_rules:
                expansion_report += f"{x.description}, {x.property}, {x.operator}, {x.value}, {len(x.results)} codes excluded\n"

            terminology_set = include_rules.pop(0).results
            # todo: if it's a grouping value set, we should use union instead of intersection
            for x in include_rules:
                terminology_set = terminology_set.intersection(x.results)

            expansion_report += "\nIntersection of Inclusion Rules\n"

            # .join w/ a list comprehension used for performance reasons
            expansion_report += "".join(
                [
                    f"{x.code}, {x.display}, {x.system}, {x.version}\n"
                    for x in terminology_set
                ]
            )

            for x in exclude_rules:
                remove_set = terminology_set.intersection(x.results)
                terminology_set = terminology_set - remove_set

                expansion_report += f"\nProcessing Exclusion Rule: {x.description}, {x.property}, {x.operator}, {x.value}\n"
                expansion_report += "The following codes were removed from the set:\n"
                # for removed in remove_set:
                #   expansion_report += f"{removed.code}, {removed.display}, {removed.system}, {removed.version}\n"
                expansion_report += "".join(
                    [
                        f"{removed.code}, {removed.display}, {removed.system}, {removed.version}\n"
                        for removed in remove_set
                    ]
                )

            self.expansion = self.expansion.union(terminology_set)

            expansion_report += f"\nThe expansion will contain the following codes for the terminology {terminology.name}:\n"

            # .join w/ a list comprehension used for performance reasons
            expansion_report += "".join(
                [
                    f"{x.code}, {x.display}, {x.system}, {x.version}\n"
                    for x in terminology_set
                ]
            )
            expansion_report += "\nErrors\n\n"
            if len(errors) == 0:
                expansion_report += "(None)\n"
            else:
                expansion_report += "\n".join(errors) + "\n"
            expansion_report += "\n"

        return self.expansion, expansion_report

    # Move serialization logic for rule groups here
    def serialize_include(self):
        include_rules = self.include_rules
        terminology_keys = include_rules.keys()
        serialized = []

        for key in terminology_keys:
            rules = include_rules.get(key)
            serialized_rules = [x.serialize() for x in rules]
            serialized.append(
                {
                    "system": key.fhir_uri,
                    "version": key.version,
                    "filter": serialized_rules,
                }
            )
        return serialized

    def serialize_exclude(self):
        exclude_rules = self.exclude_rules
        terminology_keys = exclude_rules.keys()
        serialized = []

        for key in terminology_keys:
            rules = exclude_rules.get(key)
            serialized_rules = [x.serialize() for x in rules]
            serialized.append(
                {
                    "system": key.fhir_uri,
                    "version": key.version,
                    "filter": serialized_rules,
                }
            )

        return serialized

    def update_terminology_version_in_rules(
        self, old_terminology_version_uuid, new_terminology_version_uuid
    ):
        """
        Raises NotFoundException if Terminology.load fails on either version
        """
        old_terminology_version = Terminology.load(old_terminology_version_uuid)
        rules_with_old_terminology = self.rules.get(old_terminology_version)
        if rules_with_old_terminology is None:
            return
        for rule in rules_with_old_terminology:
            rule.update(new_terminology_version_uuid=new_terminology_version_uuid)

    # Move include and exclude rule properties to here
    @property
    def include_rules(self):
        keys = self.rules.keys()
        include_rules = {}

        for key in keys:
            rules_for_terminology = self.rules.get(key)
            include_rules_for_terminology = [
                x for x in rules_for_terminology if x.include is True
            ]
            if include_rules_for_terminology:
                include_rules[key] = include_rules_for_terminology

        return include_rules

    @property
    def exclude_rules(self):
        keys = self.rules.keys()
        exclude_rules = {}

        for key in keys:
            rules_for_terminology = self.rules.get(key)
            exclude_rules_for_terminology = [
                x for x in rules_for_terminology if x.include is False
            ]
            if exclude_rules_for_terminology:
                exclude_rules[key] = exclude_rules_for_terminology

        return exclude_rules


class ValueSetVersion:
    def __init__(
        self,
        uuid,
        effective_start,
        effective_end,
        version,
        value_set,
        status,
        description,
        comments,
    ):
        self.uuid = uuid
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.version = version
        self.value_set = value_set
        self.status = status
        self.description = description
        self.comments = comments
        self.version = version
        self.expansion_uuid = None

        # self.rules = {}
        self.rule_groups = []
        self.expansion = set()
        self.expansion_timestamp = None
        self.extensional_codes = {}
        self.explicitly_included_codes = []

        self._expanded = False

    def __repr__(self):
        return f"<ValueSetVersion uuid={self.uuid}, title={self.value_set.title}, version={self.version}>"

    @classmethod
    def create(
        cls,
        effective_start,
        effective_end,
        value_set_uuid,
        status,
        description,
        created_date,
        version,
        comments,
    ):
        """
        Raises any database exceptions to the caller
        """
        conn = get_db()
        vsv_uuid = uuid.uuid4()
        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.value_set_version
                    (uuid, effective_start, effective_end, value_set_uuid, status, description, created_date, version, comments)
                    values
                    (:uuid, :effective_start, :effective_end, :value_set_uuid, :status, :description, :created_date, :version, :comments)
                    """
                ),
                {
                    "uuid": vsv_uuid,
                    "effective_start": effective_start,
                    "effective_end": effective_end,
                    "value_set_uuid": value_set_uuid,
                    "status": status,
                    "description": description,
                    "created_date": created_date,
                    "version": version,
                    "comments": comments,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e
        return cls.load(vsv_uuid)

    @classmethod
    def load(cls, uuid):
        """
        Raises NotFoundException if the uuid does not find a ValueSetVersion
        """
        conn = get_db()
        vs_version_data = conn.execute(
            text(
                """
            select * from value_sets.value_set_version where uuid=:uuid
            """
            ),
            {"uuid": str(uuid)},
        ).first()

        if vs_version_data is None:
            raise NotFoundException(f"No Value Set Version found with UUID: {uuid}")

        value_set = ValueSet.load(vs_version_data.value_set_uuid)

        value_set_version = cls(
            vs_version_data.uuid,
            vs_version_data.effective_start,
            vs_version_data.effective_end,
            vs_version_data.version,
            value_set,
            vs_version_data.status,
            vs_version_data.description,
            vs_version_data.comments,
        )
        value_set_version.load_rules()

        value_set_version.explicitly_included_codes = (
            ExplicitlyIncludedCode.load_all_for_vs_version(value_set_version)
        )

        if value_set.type == "extensional":
            extensional_members_data = conn.execute(
                text(
                    """
                select * from value_sets.extensional_member
                join terminology_versions tv
                on terminology_version_uuid=tv.uuid
                where vs_version_uuid=:uuid
                """
                ),
                {"uuid": uuid},
            )
            extensional_data = [x for x in extensional_members_data]

            for item in extensional_data:
                code = Code(item.fhir_uri, item.version, item.code, item.display)
                if (
                    item.fhir_uri,
                    item.version,
                ) not in value_set_version.extensional_codes:
                    value_set_version.extensional_codes[
                        (item.fhir_uri, item.version)
                    ] = [code]
                else:
                    value_set_version.extensional_codes[
                        (item.fhir_uri, item.version)
                    ].append(code)

        return value_set_version

    def load_rules(self):
        conn = get_db()
        rule_groups_query = conn.execute(
            text(
                """
                select distinct rule_group
                from value_sets.value_set_rule
                where value_set_version=:vs_version_uuid
                """
            ),
            {"vs_version_uuid": self.uuid},
        )
        rule_group_ids = [x.rule_group for x in rule_groups_query]
        self.rule_groups = [RuleGroup(self.uuid, x) for x in rule_group_ids]

    def expand(self, force_new=False, no_repeat=False):
        if no_repeat is True:
            if self._expanded is True:
                return

        if force_new is True:
            self.create_expansion()
            self._expanded = True
            return

        if self.expansion_already_exists():
            self.load_current_expansion()
            self._expanded = True
            return
        else:
            self.create_expansion()
            self._expanded = True
            return

    def expansion_already_exists(self):
        conn = get_db()
        query = conn.execute(
            text(
                """
            select * from value_sets.expansion
            where vs_version_uuid=:version_uuid
            order by timestamp desc
            """
            ),
            {"version_uuid": self.uuid},
        )
        if query.first() is not None:
            return True
        return False

    def load_current_expansion(self):
        """
        Raises NotFoundException if the ValueSetVersion is not found
        """
        conn = get_db()

        expansion_metadata = conn.execute(
            text(
                """
            select uuid, timestamp from value_sets.expansion
            where vs_version_uuid=:version_uuid
            order by timestamp desc
            limit 1
            """
            ),
            {"version_uuid": self.uuid},
        ).first()
        if expansion_metadata is None:
            raise NotFoundException(
                f"No Value Set Version found with UUID: {self.uuid}"
            )

        self.expansion_uuid = expansion_metadata.uuid

        # print(expansion_metadata.timestamp, type(expansion_metadata.timestamp))
        self.expansion_timestamp = expansion_metadata.timestamp
        if isinstance(self.expansion_timestamp, str):
            self.expansion_timestamp = parser.parse(self.expansion_timestamp)

        query_result = conn.execute(
            text(
                """
            select * from value_sets.expansion_member_data
            where expansion_uuid = :expansion_uuid
            """
            ),
            {"expansion_uuid": self.expansion_uuid},
        )

        for row in query_result:
            # TODO: come back and add depends on support
            code_schema = app.models.codes.RoninCodeSchemas(row.code_schema)
            from_custom_terminology = True if row.custom_terminology_uuid is not None else False
            from_fhir_terminology = True if row.fhir_terminology_uuid is not None else False

            if code_schema == app.models.codes.RoninCodeSchemas.code:
                new_code = app.models.codes.Code(
                    code_schema=code_schema,
                    system=row.system,
                    version=row.version,
                    code=row.code_simple,
                    display=row.display,
                    from_custom_terminology=from_custom_terminology,
                    custom_terminology_code_uuid=row.custom_terminology_uuid,
                    from_fhir_terminology=from_fhir_terminology,
                    fhir_terminology_code_uuid=row.fhir_terminology_uuid,
                    saved_to_db=True
                )
            elif code_schema == app.models.codes.RoninCodeSchemas.codeable_concept:
                code_object = app.models.codes.FHIRCodeableConcept.deserialize(row.code_jsonb)
                new_code = app.models.codes.Code(
                    code_schema=code_schema,
                    system=row.system,
                    version=row.version,
                    code=None,
                    display=None,
                    code_object=code_object,
                    from_custom_terminology=from_custom_terminology,
                    custom_terminology_code_uuid=row.custom_terminology_uuid,
                    from_fhir_terminology=from_fhir_terminology,
                    fhir_terminology_code_uuid=row.fhir_terminology_uuid,
                    saved_to_db=True
                )
            else:
                raise NotImplementedError(f"ValueSetVersion.load_current_expansion cannot load code with schema {code_schema}")

            self.expansion.add(new_code)

    def save_expansion(self, report=None):
        """
        Raises any database exceptions to the caller
        """
        conn = get_db()
        self.expansion_uuid = uuid.uuid1()

        # Create a new expansion entry in the value_sets.expansion table
        current_time_string = (
            datetime.now()
        )  # + timedelta(days=1) # Must explicitly create this, since SQLite can't use now()
        self.expansion_timestamp = current_time_string
        try:
            conn.execute(
                text(
                    """
                insert into value_sets.expansion
                (uuid, vs_version_uuid, timestamp, report)
                values
                (:expansion_uuid, :version_uuid, :curr_time, :report)
                """
                ),
                {
                    "expansion_uuid": str(self.expansion_uuid),
                    "version_uuid": str(self.uuid),
                    "report": report,
                    "curr_time": current_time_string,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        if self.expansion:
            try:
                conn.execute(
                    expansion_member_data.insert(),
                    [
                        {
                            "expansion_uuid": str(self.expansion_uuid),
                            "code_schema": code.code_schema,
                            "code_simple": code.code_simple,
                            "code_jsonb": code.code_jsonb,
                            "display": code.display,
                            "system": code.system,
                            "version": code.version,
                            "custom_terminology_uuid": str(
                                code.custom_terminology_code_uuid
                            )
                            if code.custom_terminology_code_uuid
                            else None,
                        }
                        for code in self.expansion
                    ],
                )
            except Exception as e:
                conn.rollback()
                raise e

    def create_expansion(self):
        """
        1. Rules are processed
        2. Mapping inclusions are processed
        3. Explicitly included codes are added directly to the final expansion
        """
        if self.value_set.type == "extensional":
            return None

        self.expansion = set()
        expansion_report_combined = ""

        for rule_group in self.rule_groups:
            expansion, expansion_report = rule_group.generate_expansion()
            self.expansion = self.expansion.union(expansion)
            expansion_report_combined += expansion_report

        self.process_mapping_inclusions()

        codes_for_explicit_inclusion = [x.code for x in self.explicitly_included_codes]
        self.expansion = self.expansion.union(set(codes_for_explicit_inclusion))

        self.save_expansion(report=expansion_report_combined)

    def parse_mapping_inclusion_retool_array(self, retool_array):
        array_string_copy = retool_array
        array_string_copy = array_string_copy[1:]
        array_string_copy = array_string_copy[:-1]
        array_string_copy = "[" + array_string_copy + "]"
        python_array = json.loads(array_string_copy)
        return python_array

    def process_mapping_inclusions(self):
        # Load mapping inclusion rules for version
        conn = get_db()
        mapping_inclusions_query = conn.execute(
            text(
                """
                select * from value_sets.mapping_inclusion
                where vs_version_uuid=:version_uuid
                """
            ),
            {"version_uuid": self.uuid},
        )
        mapping_inclusions = [x for x in mapping_inclusions_query]

        for inclusion in mapping_inclusions:
            # Load appropriate concept maps
            allowed_relationship_types = self.parse_mapping_inclusion_retool_array(
                inclusion.relationship_types
            )
            concept_map = app.concept_maps.models.DeprecatedConceptMap(
                None, allowed_relationship_types, inclusion.concept_map_name
            )

            if inclusion.match_source_or_target == "source":
                mappings = concept_map.source_code_to_target_map
            elif inclusion.match_source_or_target == "target":
                mappings = concept_map.target_code_to_source_map

            # Identify mapped codes and insert into expansion
            codes_to_add_to_expansion = []
            for item in self.expansion:
                if item.code in mappings:
                    codes_to_add_to_expansion.extend(mappings[item.code])

            set_to_add_to_expansion = set(codes_to_add_to_expansion)
            self.expansion = self.expansion.union(set_to_add_to_expansion)

    def extensional_vs_time_last_modified(self):
        conn = get_db()
        last_modified_query = conn.execute(
            text(
                """
              select * from value_sets.history
              where table_name='extensional_member'
              and (new_val->>'vs_version_uuid' = :vs_version_uuid or old_val->>'vs_version_uuid' = :vs_version_uuid)
              order by timestamp desc
              limit 1
              """
            ),
            {"vs_version_uuid": str(self.uuid)},
        )
        result = last_modified_query.first()
        return result.timestamp

    def serialize_include(self):
        if self.value_set.type == "extensional":
            keys = self.extensional_codes.keys()
            serialized = []

            for key in keys:
                terminology = key[0]
                version = key[1]
                serialized_codes = [
                    x.serialize(with_system_and_version=False)
                    for x in self.extensional_codes.get(key)
                ]

                serialized.append(
                    {
                        "system": terminology,
                        "version": version,
                        "concept": serialized_codes,
                    }
                )

            return serialized

        elif self.value_set.type == "intensional":
            serialized = []
            for group in self.rule_groups:
                serialized_rules = group.serialize_include()
                for rule in serialized_rules:
                    serialized.append(rule)
            return serialized

    def serialize_exclude(self):
        if self.value_set.type == "intensional":
            serialized = []
            for item in [x.serialize_exclude() for x in self.rule_groups]:
                if item != []:
                    for rule in item:
                        serialized.append(rule)
            return serialized

        else:  # No exclude for extensional
            return []

    def serialize(
        self,
        schema_version: int = ValueSet.next_schema_version,
    ):
        """
        Transform the ValueSet instance into a dictionary in a format suitable for serialization.

        This method is primarily used to convert the instance into a format that can be easily serialized into JSON.
        This includes converting complex data types into simple data types that can be serialized.

        It also applies specific transformations to the data to ensure it meets the RCDM-compliant format,
        including generating a compliant name for the ValueSet, and transforming use case names into the required format.

        Parameters
        ----------
        schema_version : int, optional
        The schema version to use when serializing the ValueSet instance. Default is ValueSet.next_schema_version.

        Returns
        -------
        dict
            The dictionary representing the serialized state of the ValueSet instance.

        """
        # Prepare according to the version
        if schema_version not in [
            ValueSet.database_schema_version,
            ValueSet.next_schema_version,
        ]:
            raise BadRequestWithCode(
                "ValueSetVersion.serialize",
                f"Value Set schema version {schema_version} is not supported.",
            )
        is_schema_version_5_or_later = schema_version >= 5

        pattern = r"[A-Z]([A-Za-z0-9_]){0,254}"  # name transformer
        if re.match(pattern, self.value_set.name):  # name follows pattern use name
            rcdm_name = self.value_set.name
        else:
            index = re.search(
                r"[a-zA-Z]", self.value_set.name
            ).start()  # name does not follow pattern, uppercase 1st letter
            rcdm_name = (
                self.value_set.name[:index]
                + self.value_set.name[index].upper()
                + self.value_set.name[index + 1 :]
            )

        # Load the primary and secondary use cases associated with the value set
        use_cases = load_use_case_by_value_set_uuid(self.value_set.uuid)

        primary_use_case_name = (
            [use_cases["primary_use_case"].name]
            if use_cases["primary_use_case"]
            else []
        )
        secondary_use_case_names = [uc.name for uc in use_cases["secondary_use_cases"]]
        all_use_case_names = primary_use_case_name + secondary_use_case_names

        # Transform the use case names into the RCDM-compliant format
        use_case_coding = [
            {
                "code": use_case_name,  # the value will be the use case name
            }
            for use_case_name in all_use_case_names
        ]
        if not use_case_coding:
            use_case_coding = [{"code": "unknown"}]

        extension_value_string = "5" if is_schema_version_5_or_later else "2"

        serialized = {
            "resourceType": "ValueSet",
            "id": str(self.value_set.uuid),
            # for FHIR value sets, id will be a name (e.g. publication-status).  For Ronin value sets, id will be the value set uuid.
            "meta": {
                "profile": [
                    "http://projectronin.io/fhir/StructureDefinition/ronin-valueSet"
                ]
            },
            "extension": [
                {
                    "url": "http://projectronin.io/fhir/StructureDefinition/Extension/ronin-valueSetSchema",
                    "valueString": extension_value_string,
                }
            ],
            "url": f"http://projectronin.io/fhir/ValueSet/{self.value_set.uuid}",
            "name": rcdm_name,
            "title": self.value_set.title,
            "publisher": self.value_set.publisher,
            "contact": [{"name": self.value_set.contact}],
        }

        # if else for descriptions, depending on the schema version
        if is_schema_version_5_or_later:
            serialized["description"] = self.value_set.description or ""
            serialized["versionDescription"] = self.description or ""
            serialized["useContext"] = [
                [
                    {
                        "code": {
                            "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",  # static value
                            "code": "workflow",  # static value
                            "display": "Workflow Setting",
                        },
                        "valueCodeableConcept": {
                            "coding": use_case_coding,
                        },
                    }
                ]
            ]

        else:
            serialized["description"] = (
                (self.value_set.description or "") + " " + (self.description or "")
            )
            serialized["useContext"] = [
                {
                    "code": {
                        "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
                        "code": "workflow",
                        "display": "Workflow Setting",
                    },
                    "valueCodeableConcept": {
                        "coding": use_case_coding,
                    },
                }
            ]

        # continue with the rest of the dictionary
        serialized["immutable"] = self.value_set.immutable
        serialized["experimental"] = self.value_set.experimental
        serialized["purpose"] = self.value_set.purpose
        serialized["version"] = str(self.version)  # Version must be a string
        serialized["status"] = self.status
        serialized["expansion"] = {
            "contains": [x.serialize() for x in self.expansion],
            "timestamp": self.expansion_timestamp.strftime("%Y-%m-%d")
            if self.expansion_timestamp is not None
            else None,
        }
        # serialized["compose"] = {"include": self.serialize_include()},
        serialized[
            "additionalData"
        ] = {  # Place to put custom values that aren't part of the FHIR spec
            "effective_start": self.effective_start,
            "effective_end": self.effective_end,
            "version_uuid": self.uuid,
            "value_set_uuid": self.value_set.uuid,
            "expansion_uuid": self.expansion_uuid,
            "synonyms": self.value_set.synonyms,
        }

        # serialized_exclude = self.serialize_exclude()
        # if serialized_exclude:
        #     serialized["compose"]["exclude"] = serialized_exclude
        #
        # if self.value_set.type == "extensional":
        #     serialized.pop("expansion")

        return serialized

    def prepare_for_oci(self, schema_version: int = ValueSet.next_schema_version):
        """
        This method prepares the serialized representation of a value set for OCI publishing.

        It takes into account the value set's publisher, status, and other attributes to create an RCDM-compliant
        dictionary representation, including relevant fields such as ID, URL, status, date, and expansion.
        The method also determines the initial path for storage based on the value set's UUID.

        Parameters
        ----------
        schema_version : int, optional
        The schema version to use when preparing the ValueSet instance for OCI publishing. Default is ValueSet.next_schema_version.

        Returns:
        tuple: A tuple containing two elements:
        1. dict: The RCDM-compliant serialized representation of the value set.
        2. str: The initial storage path for the value set, based on its UUID.
        """
        serialized = self.serialize(schema_version=schema_version)
        rcdm_id = serialized.get("id")
        rcdm_url = "http://projectronin.io/ValueSet/"
        # id will depend on publisher
        if self.value_set.publisher == "Project Ronin":
            rcdm_id = serialized.get("id")
            rcdm_url = "http://projectronin.io/fhir/ValueSet/"
        elif self.value_set.publisher == "FHIR":
            rcdm_id = serialized.get("name")
            # transform rcdm_id in place
            rcdm_id = re.sub("([a-z])([A-Z])", r"\1-\2", rcdm_id).lower()
            rcdm_url = "http://hl7.org/fhir/ValueSet/"

        if self.status in {
            "pending",
            "reviewed",
        }:  # has a required binding (translate pending, reviewed to draft)
            rcdm_status = "draft"
        else:
            rcdm_status = self.status

        if self.status == "active":  # date value set version was made active
            comment_string = str(self.comments)
            pull_date = re.search(
                r"\b\w+\s\d+(?:st|nd|rd|th)? \d{4}, \d{1,2}:\d{2}:\d{2} [ap]m\b",
                comment_string,
            )
            if pull_date is not None:
                date_string = pull_date.group(0)
                dt = None
                try:
                    dt = datetime.strptime(date_string, "%B %drd %Y, %I:%M:%S %p")
                except ValueError:
                    pass

                if dt is None:
                    try:
                        dt = datetime.strptime(date_string, "%B %dst %Y, %I:%M:%S %p")
                    except ValueError:
                        pass

                if dt is None:
                    try:
                        dt = datetime.strptime(date_string, "%B %dth %Y, %I:%M:%S %p")
                    except ValueError:
                        pass

                if dt is None:
                    try:
                        dt = datetime.strptime(date_string, "%B %dnd %Y, %I:%M:%S %p")
                    except ValueError:
                        pass

                rcdm_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
            else:
                rcdm_date_now = datetime.now()
                rcdm_date = rcdm_date_now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            rcdm_date_now = datetime.now()
            rcdm_date = rcdm_date_now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        oci_serialized = {
            "id": rcdm_id,
            "url": f"{rcdm_url}{rcdm_id}",
            # specific to the overall value set; suffix matching the id field exactly
            "status": rcdm_status,
            # has a required binding (translate pending to draft)  (draft, active, retired, unknown)
            "date": rcdm_date,  # the date the status was set to active
            "expansion": {
                "identifier": f"urn:uuid:{self.expansion_uuid}",  # rcdm format specific
                "total": len(self.expansion),  # total number of codes in the expansion
                "contains": [x.serialize() for x in self.expansion],
                "timestamp": self.expansion_timestamp.strftime("%Y-%m-%d")
                if self.expansion_timestamp is not None
                else None,
            },
        }

        serialized.update(oci_serialized)  # Merge oci_serialized into serialized
        serialized.pop("additionalData")
        serialized.pop("immutable")
        serialized.pop("contact")
        serialized.pop("publisher")
        initial_path = f"{ValueSet.object_storage_folder_name}/v{schema_version}"

        return serialized, initial_path

    def publish(self, force_new):
        """
        Publish the ValueSet instance to OCI storage and Simplifier.

        This method first expands the ValueSet instance and then prepares it for OCI publishing using the `prepare_for_oci` method.
        It sends the serialized value set to OCI storage using the `set_up_object_store` method for both the database_schema_version
        and next_schema_version, if they are different.

        The method then creates a copy of the serialized value set for Simplifier and sets the status to active.

        """

        self.expand(force_new=force_new)

        # OCI: output as ValueSet.database_schema_version, which may be the same as ValueSet.next_schema_version
        value_set_to_json = self.send_to_oci(ValueSet.database_schema_version)

        # OCI: also output as ValueSet.next_schema_version, if different from ValueSet.database_schema_version
        if ValueSet.database_schema_version != ValueSet.next_schema_version:
            value_set_to_json = self.send_to_oci(ValueSet.next_schema_version)

        # Additional publishing activities
        self.version_set_status_active()
        self.retire_and_obsolete_previous_version()
        self.to_simplifier(value_set_to_json)

        # Publish new version of data normalization registry
        app.models.data_ingestion_registry.DataNormalizationRegistry.publish_data_normalization_registry()

    def send_to_oci(self, schema_version):
        value_set_to_json, initial_path = self.prepare_for_oci(schema_version)
        set_up_object_store(
            value_set_to_json,
            initial_path + f"/published/{self.value_set.uuid}",
            folder="published",
            content_type="json",
        )
        return value_set_to_json

    def to_simplifier(self, value_set_to_json):
        value_set_uuid = self.value_set.uuid
        resource_type = "ValueSet"  # param for Simplifier
        value_set_to_json["status"] = "active"  # Simplifier requires a status

        # Check if the 'expansion' and 'contains' keys are present
        if (
            "expansion" in value_set_to_json
            and "contains" in value_set_to_json["expansion"]
        ):
            # Store the original total value
            original_total = value_set_to_json["expansion"]["total"]

            # Limit the contains list to the top 50 entries
            value_set_to_json["expansion"]["contains"] = value_set_to_json["expansion"][
                "contains"
            ][:50]

            # Set the 'total' field to the original total
            value_set_to_json["expansion"]["total"] = original_total
        try:
            publish_to_simplifier(resource_type, value_set_uuid, value_set_to_json)
        except Exception as e:  # Publishing to Simplifier will be treated as optional, not required
            logging.warning(f"Unable to publish Value Set Version {self.uuid}, {self.value_set.title} version {self.version} to Simplifier")
            pass

    @classmethod
    def load_expansion_report(cls, expansion_uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                select * from value_sets.expansion
                where uuid=:expansion_uuid
                """
            ),
            {"expansion_uuid": expansion_uuid},
        ).first()
        return result.report

    def update_rules_for_terminology(
        self, old_terminology_version_uuid, new_terminology_version_uuid
    ):
        for rule_group in self.rule_groups:
            rule_group.update_terminology_version_in_rules(
                old_terminology_version_uuid=old_terminology_version_uuid,
                new_terminology_version_uuid=new_terminology_version_uuid,
            )
        self.load_rules()

    @classmethod
    def diff_for_removed_and_added_codes(
        cls,
        previous_version_uuid,
        new_version_uuid,
    ):
        """
        Raises NotFoundException if ValueSetVersion.load fails on either version
        """
        previous_value_set_version = ValueSetVersion.load(previous_version_uuid)
        new_value_set_version = ValueSetVersion.load(new_version_uuid)

        previous_value_set_version.expand()
        new_value_set_version.expand()

        conn = get_db()
        removed_codes_query = conn.execute(
            text(
                """
                select distinct code_schema, code_simple, code_jsonb, display, system from value_sets.expansion_member_data
                where expansion_uuid = :previous_expansion
                EXCEPT
                select distinct code_schema, code_simple, code_jsonb, display, system from value_sets.expansion_member_data
                where expansion_uuid = :new_expansion
                order by display asc
                """
            ),
            {
                "previous_expansion": previous_value_set_version.expansion_uuid,
                "new_expansion": new_value_set_version.expansion_uuid,
            },
        )
        removed_codes = [
            {
                "code_schema": x.code_schema,
                "code_simple": x.code_simple,
                "code_jsonb": x.code_jsonb,
                "display": x.display,
                "system": x.system,
            }
            for x in removed_codes_query
        ]

        added_codes_query = conn.execute(
            text(
                """
                select distinct code_schema, code_simple, code_jsonb, display, system from value_sets.expansion_member_data
                where expansion_uuid = :new_expansion
                EXCEPT
                select distinct code_schema, code_simple, code_jsonb, display, system from value_sets.expansion_member_data
                where expansion_uuid = :previous_expansion
                order by display asc
                """
            ),
            {
                "previous_expansion": previous_value_set_version.expansion_uuid,
                "new_expansion": new_value_set_version.expansion_uuid,
            },
        )
        added_codes = [
            {
                "code_schema": x.code_schema,
                "code_simple": x.code_simple,
                "code_jsonb": x.code_jsonb,
                "display": x.display,
                "system": x.system,
            }
            for x in added_codes_query
        ]

        return {"removed_codes": removed_codes, "added_codes": added_codes}

    def update(self, status=None):
        if status is None:
            return

        if status == "active":
            raise BadRequest(
                f"Versions can not be set to active in this manner. Go through publication proces instead."
            )

        conn = get_db()
        conn.execute(
            text(
                """
            update value_sets.value_set_version
            set status = :new_status
            where uuid = :uuid
            """
            ),
            {"new_status": status, "uuid": str(self.uuid)},
        )

    def version_set_status_active(self):
        """
        This method updates the status of the value set version, identified by its UUID, to 'active'.
        """

        conn = get_db()
        conn.execute(
            text(
                """
                    UPDATE value_sets.value_set_version
                    SET status=:status
                    WHERE uuid=:version_uuid
                    """
            ),
            {
                "status": "active",
                "version_uuid": self.uuid,
            },
        )

    def retire_and_obsolete_previous_version(self):
        """
        This method updates the status of previous value set versions based on the current version's UUID and value set's UUID.
        It sets the status of previously 'active' versions to 'retired' and the status of 'pending', 'in progress', and 'reviewed'
        versions to 'obsolete'.
        """

        conn = get_db()
        conn.execute(
            text(
                """
                update value_sets.value_set_version
                set status = 'retired'
                where status = 'active'
                and value_set_uuid =:value_set_uuid
                and value_set_version.uuid !=:version_uuid
                """
            ),
            {"value_set_uuid": self.value_set.uuid, "version_uuid": self.uuid},
        )
        conn.execute(
            text(
                """
                update value_sets.value_set_version
                set status = 'obsolete'
                where status in ('pending','in progress','reviewed')
                and value_set_uuid =:value_set_uuid
                and value_set_version.uuid !=:version_uuid
                """
            ),
            {"value_set_uuid": self.value_set.uuid, "version_uuid": self.uuid},
        )

    def contains_content_from_terminology(self, terminology_version_uuid):
        """
        Safely returns True or False
        """
        self.expand()

        # Check for rules
        for rule_group in self.rule_groups:
            for terminology, rules in rule_group.rules.items():
                if terminology.uuid == terminology_version_uuid and rules:
                    return True

        # Check expansion for codes
        try:
            terminology = Terminology.load(terminology_version_uuid)
            for code in self.expansion:
                if (
                    code.system == terminology.fhir_uri
                    and code.version == terminology.version
                ):
                    return True
        except NotFoundException:
            pass

        # Nothing from this terminology
        return False

    def lookup_terminologies_in_value_set_version(self) -> List[Terminology]:
        """
        This method scans through an expansion set and collects unique terminologies that are defined
        in the set. Terminologies are distinguished by a combination of their system and version.

        The expansion set (self.expansion) should be an iterable collection of codes, where each code
        has a 'system' and a 'version' attribute.

        Returns:
            A list of unique Terminology objects found in the expansion set.
        """
        if not self.expansion:
            self.expand()

        terminologies: Dict[Tuple[str, str], Terminology] = dict()

        for code in self.expansion:
            key = (code.system, code.version)
            try:
                terminology = Terminology.load_by_fhir_uri_and_version_from_cache(
                    fhir_uri=code.system, version=code.version
                )
            except NotFoundException:
                raise DataIntegrityError(
                    f"No terminology found with fhir_uri: {code.system} and version: {code.version}."
                    + f" This caused a failure to look up terminologies in the value set: {self.value_set.title}"
                    + f" version: {self.version} "
                )
            if key not in terminologies:
                terminologies[key] = terminology

        return list(terminologies.values())

    @classmethod
    def create_new_version_from_specified_previous(
        cls,
        version_uuid,
        new_version_description=None,
        new_terminology_version_uuid=None,
    ):
        """
        Raises any database exceptions to the caller
        """
        # Load the input version of the value set
        input_version = cls.load(version_uuid)

        # Create a new version of the value set with the same rules as the input version
        new_version_uuid = uuid.uuid4()
        new_version_number = input_version.version + 1

        # Save the new version to the database
        conn = get_db()
        try:
            conn.execute(
                text(
                    """  
                    INSERT INTO value_sets.value_set_version  
                    (uuid, effective_start, effective_end, value_set_uuid, status, description, created_date, version)  
                    VALUES  
                    (:new_version_uuid, :effective_start, :effective_end, :value_set_uuid, :status, :description, :created_date, :version)  
                    """
                ),
                {
                    "new_version_uuid": new_version_uuid,
                    "effective_start": input_version.effective_start,
                    "effective_end": input_version.effective_end,
                    "value_set_uuid": input_version.value_set.uuid,
                    "status": "pending",
                    "description": new_version_description or input_version.description,
                    "created_date": datetime.now(),
                    "version": new_version_number,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Copy rules from input version to new version
        try:
            conn.execute(
                text(
                    """  
                    INSERT INTO value_sets.value_set_rule  
                    (position, description, property, operator, value, include, terminology_version, value_set_version, rule_group)  
                    SELECT position, description, property, operator, value, include,  
                    COALESCE(:new_terminology_version_uuid, terminology_version), :new_version_uuid, rule_group  
                    FROM value_sets.value_set_rule  
                    WHERE value_set_version = :input_version_uuid  
                    """
                ),
                {
                    "input_version_uuid": version_uuid,
                    "new_version_uuid": new_version_uuid,
                    "new_terminology_version_uuid": new_terminology_version_uuid,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

        # Return the new ValueSetVersion object
        return cls.load(new_version_uuid)


@dataclass
class ExplicitlyIncludedCode:
    """
    These are codes that are explicitly added to an intensional value set.
    """

    code: Code
    value_set_version: ValueSetVersion
    review_status: str
    uuid: uuid = field(default=uuid.uuid4())

    def save(self):
        """
        Persist newly created object to database.
        Raises any database exceptions to the caller
        """
        conn = get_db()

        try:
            conn.execute(
                text(
                    """
                    insert into value_sets.explicitly_included_code
                    (uuid, vs_version_uuid, code_uuid, review_status)
                    values
                    (:uuid, :vs_version_uuid, :code_uuid, :review_status)
                    """
                ),
                {
                    "uuid": self.uuid,
                    "vs_version_uuid": self.value_set_version.uuid,
                    "code_uuid": self.code.custom_terminology_code_uuid,
                    "review_status": self.review_status,
                },
            )
        except Exception as e:
            conn.rollback()
            raise e

    def serialize(self):
        return {
            "uuid": self.uuid,
            "review_status": self.review_status,
            "value_set_version_uuid": self.value_set_version.uuid,
            "code": self.code.serialize(),
        }

    @classmethod
    def load_all_for_vs_version(cls, vs_version: ValueSetVersion):
        conn = get_db()

        code_data = conn.execute(
            text(
                """
                select eic.uuid as explicit_uuid, code.uuid as code_uuid, code.code, code.display, tv.fhir_uri as system_uri, tv.terminology as system_name, tv.version, eic.review_status
                from value_sets.explicitly_included_code eic
                join custom_terminologies.code
                on eic.code_uuid = code.uuid
                join terminology_versions tv
                on code.terminology_version_uuid = tv.uuid
                where vs_version_uuid=:vs_version_uuid
                """
            ),
            {"vs_version_uuid": vs_version.uuid},
        )

        results = []
        for x in code_data:
            code = Code(
                system=x.system_uri,
                version=x.version,
                code=x.code,
                display=x.display,
                custom_terminology_code_uuid=x.code_uuid,
            )

            explicity_code_inclusion = cls(
                code=code,
                value_set_version=vs_version,
                review_status=x.review_status,
                uuid=x.explicit_uuid,
            )
            results.append(explicity_code_inclusion)

        return results


# Clarification: this stand-alone method is deliberately not part of the above class
def execute_rules(rules_json):
    """
    This function will receive a single JSON encoded rule group and execute it to provide output. It can be used on the front end to preview the output of a rule group

    Sample input JSON:
    [
      {
        "property": "code",
        "operator": "in",
        "value": [{"category_name": "Endovascular Revascularization Open or Percutaneous, Transcatheter* (Arteries and Veins)", "range": " 37220-37239,37246-37249"}, {"category_name": "Venous, Direct or With Catheter  (Arteries and Veins)", "range": "34401-34490"}, {"category_name": "Endovascular Repair of Abdominal Aorta and/or Iliac Arteries*  (Arteries and Veins)", "range": "34701-34834"}],
        "include": true,
        "terminology_version": "6c6219c8-5ef3-11ec-8f16-acde48001122"
      }
    ]
    """
    conn = get_db()

    # Lookup terminology names
    terminology_versions_query = conn.execute(
        text(
            """
        select * from terminology_versions
        """
        )
    )
    terminology_versions = [x for x in terminology_versions_query]

    uuid_to_name_map = {str(x.uuid): x for x in terminology_versions}

    rules = []
    for rule in rules_json:
        terminology_name = uuid_to_name_map.get(
            rule.get("terminology_version")
        ).terminology
        fhir_uri = uuid_to_name_map.get(rule.get("terminology_version")).fhir_uri
        terminology_version = uuid_to_name_map.get(rule.get("terminology_version"))
        rule_property = rule.get("property")
        operator = rule.get("operator")
        value = rule.get("value")
        include = rule.get("include")

        if terminology_name == "ICD-10 CM":
            rule = ICD10CMRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )
        elif terminology_name == "SNOMED CT":
            rule = SNOMEDRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )
        elif terminology_name == "RxNorm":
            rule = RxNormRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )
        elif terminology_name == "LOINC":
            rule = LOINCRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )
        elif terminology_name == "CPT":
            rule = CPTRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )
        elif terminology_name == "ICD-10 PCS":
            rule = ICD10PCSRule(
                None,
                None,
                None,
                rule_property,
                operator,
                value,
                include,
                None,
                fhir_uri,
                terminology_version,
            )

        rules.append(rule)

    for rule in rules:
        rule.execute()

    include_rules = [x for x in rules if x.include is True]
    exclude_rules = [x for x in rules if x.include is False]

    terminology_set = include_rules.pop(0).results
    for x in include_rules:
        terminology_set = terminology_set.intersection(x.results)

    for x in exclude_rules:
        remove_set = terminology_set.intersection(x.results)
        terminology_set = terminology_set - remove_set

    return [x.serialize() for x in list(terminology_set)]


def value_sets_terminology_update_report(terminology_fhir_uri, exclude_version):
    """
     Identifies value sets which include the specified terminology and returns the most recent version of the value set
    @param terminology_fhir_uri: terminology involved in the update e.g. http://snomed.info/sct
    @param exclude_version: newest version of the terminology e.g.
    @return:
    """
    if terminology_fhir_uri is None:
        raise BadRequestWithCode(
            "ValueSet.value_sets_terminology_update_report.no_term_input",
            "No terminology URI was input",
        )

    conn = get_db()

    ready_for_update = []
    latest_version_not_active = []
    already_updated = []

    value_sets = ValueSet.load_all_value_set_metadata(active_only=False)

    # Iterate through all value sets
    for vs in value_sets:
        value_set_uuid = vs.get("uuid")
        value_set_name = vs.get("name")
        value_set_title = vs.get("title")

        versions_metadata = ValueSet.load_version_metadata(
            value_set_uuid
        )  # always returns a list, even if empty
        if len(versions_metadata) == 0:
            raise NotFoundException(
                f"No versions found for Value Set with UUID: {value_set_uuid}"
            )
        # sort the versions and get the most recent even thought versions response is ordered desc
        sorted_versions = sorted(
            versions_metadata, key=lambda x: x.get("version"), reverse=True
        )

        most_recent_version_metadata = sorted_versions[0]

        most_recent_version_uuid = most_recent_version_metadata.get("uuid")

        most_recent_version_rules_query = conn.execute(
            text(
                """
                select distinct em.system, em.version, em.expansion_uuid from value_sets.expansion_member em
                join value_sets.expansion ex on em.expansion_uuid=ex.uuid
                join value_sets.value_set_version vsv on ex.vs_version_uuid=vsv.uuid
                where vsv.uuid=:version_uuid
                """
            ).bindparams(version_uuid=most_recent_version_uuid)
        )
        result_set = most_recent_version_rules_query.fetchall()
        # there may be no rules, that is not an error

        # Perform checks to determine which category this value set should be classified under in the report
        exclude_value_set = False
        value_set_contains_terminology = False
        status_not_active = False

        if most_recent_version_metadata.get("status") != "active":
            status_not_active = True

        for item in result_set:
            if item.system == terminology_fhir_uri:
                value_set_contains_terminology = True
            if exclude_version is not None:
                if (
                    item.system == terminology_fhir_uri
                    and item.version == exclude_version
                ):
                    exclude_value_set = True

        # Classify to already updated group
        if exclude_value_set is True:
            item_dict = {
                "value_set_uuid": value_set_uuid,
                "name": value_set_name,
                "title": value_set_title,
                "most_recent_version_status": most_recent_version_metadata.get(
                    "status"
                ),
            }
            already_updated.append(item_dict)
            continue

        # Classify to latest version not active group
        if status_not_active is True and value_set_contains_terminology is True:
            item_dict = {
                "value_set_uuid": value_set_uuid,
                "name": value_set_name,
                "title": value_set_title,
            }
            latest_version_not_active.append(item_dict)
            continue

        if value_set_contains_terminology is True:
            item_dict = {
                "value_set_uuid": value_set_uuid,
                "name": value_set_name,
                "title": value_set_title,
            }
            ready_for_update.append(item_dict)
    return {
        "ready_for_update": ready_for_update,
        "latest_version_not_active": latest_version_not_active,
        "already_updated": already_updated,
    }


# # This takes too long to run as an endpoint and should be a Databricks notebook
def perform_terminology_update_for_all_value_sets(
    old_terminology_version_uuid,
    new_terminology_version_uuid,
):
    """
    Raises NotFoundException if Terminology.load fails on either version
    """
    new_terminology_version = Terminology.load(new_terminology_version_uuid)

    # Get value sets to update
    report_for_update = value_sets_terminology_update_report(
        terminology_fhir_uri=new_terminology_version.fhir_uri,
        exclude_version=new_terminology_version.version,
    )

    value_sets_to_update = [
        x.get("value_set_uuid") for x in report_for_update.get("ready_for_update")
    ]

    value_set_statuses = defaultdict(list)

    # Build a dictionary giving the name, title, and uuid of each value set updated in the loop
    for vs_uuid in value_sets_to_update:
        value_set = ValueSet.load(vs_uuid)
        status = value_set.perform_terminology_update(
            old_terminology_version_uuid,
            new_terminology_version_uuid,
            effective_start=str(new_terminology_version.effective_start),
            effective_end=str(new_terminology_version.effective_end),
            description=f"F",
        )
        value_set_statuses[status].append(
            {"name": value_set.name, "title": value_set.title, "uuid": value_set.uuid}
        )

    # Generate the JSON report
    json_report = {
        status: [
            {
                "name": vs["name"],
                "title": vs["title"],
                "uuid": vs["uuid"],
            }
            for vs in value_set_statuses[status]
        ]
        for status in value_set_statuses
    }

    return json_report
