import math
import datetime
import json
from dataclasses import dataclass, field
import re
import requests
import concurrent.futures
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import date, datetime, timedelta
from dateutil import parser
import werkzeug
from werkzeug.exceptions import BadRequest, NotFound
from decouple import config
from sqlalchemy.sql.expression import bindparam
from app.models.codes import Code

from app.models.concept_maps import DeprecatedConceptMap
from app.models.terminologies import Terminology
from app.database import get_db, get_elasticsearch
from flask import current_app
from app.helpers.oci_helper import (
    oci_authentication,
    folder_path_for_oci,
    folder_in_bucket,
    pre_export_validate,
    save_to_object_store,
    version_set_status_active,
    get_object_type_from_db,
    get_object_type_from_object_store,
    check_for_prerelease_in_published,
    set_up_object_store,
)
from app.helpers.db_helper import db_cursor
import pandas as pd
import numpy as np
from pandas import json_normalize

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
        self.position = uuid
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
        elif self.property == "display" and self.operator == "in":
            self.display_rule()

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
    # # Deprecating because we prefer ECL
    # def direct_child(self):
    #   conn = get_db()
    #   query = ""

    #   if self.property == 'concept':
    #     # Lookup UUIDs for provided codes
    #     codes = self.value.split(',')

    #     # Get all descendants of the provided codes through a recursive query
    #     query = """
    #     select * from snomedct.relationship_f rel
    #     join snomedct.description_f descr
    #     on descr.conceptid=rel.sourceid
    #     and descr.typeid='900000000000003001'
    #     where destinationid in :codes
    #     and rel.typeid='116680003'
    #     """
    #     # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/

    #   results_data = conn.execute(
    #     text(
    #       query
    #     ).bindparams(bindparam('codes', expanding=True)), {
    #       'codes': codes
    #     }
    #   )
    #   results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
    #   self.results = set(results)

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
        # json_value = json.loads(self.value)
        term_type = self.value.replace(",", " ")

        # Calls the getAllConceptsByTTY API
        payload = {"tty": term_type}
        tty_member_request = requests.get(
            f"{RXNORM_BASE_URL}allconcepts.json", params=payload
        )

        # Extracts a list of RxCUIs from the JSON response
        # rxcuis = self.json_extract(tty_member_request.json(), "rxcui")

        # Calls the concept property RxNorm API
        # concept_properties = []
        # with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
        #     results = pool.map(self.load_rxnorm_properties, rxcuis)
        #     for result in results:
        #         concept_properties.append(result)
        #
        # # Making a final list of RxNorm codes
        # final_rxnorm_codes = []
        # for item in concept_properties:
        #     properties = item.get("properties")
        #     result_term_type = properties.get("tty")
        #     display = properties.get("name")
        #     code = properties.get("rxcui")
        #     final_rxnorm_codes.append(
        #         Code(
        #             self.fhir_system,
        #             self.terminology_version.version,
        #             code,
        #             display,
        #         )
        #     )
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.get("rxcui"),
                x.get("name"),
            )
            for x in tty_member_request.json().get("minConceptGroup").get("minConcept")
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
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.loinc_num,
                x.long_common_name,
            )
            for x in results_data
        ]
        self.results = set(results)

    @property
    def split_value(self):
        """
        ReTool saves arrays like this: {"Alpha-1-Fetoprotein","Alpha-1-Fetoprotein Ab","Alpha-1-Fetoprotein.tumor marker"}
        Sometimes, we also save arrays like this: Alpha-1-Fetoprotein,Alpha-1-Fetoprotein Ab,Alpha-1-Fetoprotein.tumor marker

        This function will handle both formats and output a python list of strings
        """
        new_value = self.value
        if new_value[:1] == "{" and new_value[-1:] == "}":
            new_value = new_value[1:]
            new_value = new_value[:-1]
        new_value = new_value.split(",")
        new_value = [(x[1:] if x[:1] == '"' else x) for x in new_value]
        new_value = [(x[:-1] if x[-1:] == '"' else x) for x in new_value]
        return new_value

    def code_rule(self):
        query = """
    select * from loinc.code
    where loinc_num in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def display_rule(self):
        # Cannot use "ilike any(...)" because thats Postgres specific
        conn = get_db()

        query = f"""
    select * from loinc.code
    where lower(long_common_name) like '{self.split_value[0].lower()}'"""

        if self.split_value[1:]:
            for item in self.split_value[1:]:
                query += f""" or lower(long_common_name) like {item} """

        query += """ and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """

        results_data = conn.execute(
            text(query), {"terminology_version_uuid": self.terminology_version.uuid}
        )
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.loinc_num,
                x.long_common_name,
            )
            for x in results_data
        ]
        self.results = set(results)

    def method_rule(self):
        query = """
    select * from loinc.code
    where method_typ in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def timing_rule(self):
        query = """
    select * from loinc.code
    where time_aspct in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def system_rule(self):
        query = """
    select * from loinc.code
    where system in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def component_rule(self):
        query = """
    select * from loinc.code
    where component in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def scale_rule(self):
        query = """
    select * from loinc.code
    where scale_typ in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def property_rule(self):
        query = """
    select * from loinc.code
    where property in :value
    and status = 'ACTIVE'
    and terminology_version_uuid=:terminology_version_uuid
    order by long_common_name
    """
        self.loinc_rule(query)

    def class_type_rule(self):
        query = """
            select * from loinc.code
            where classtype in :value
            and status = 'ACTIVE'
            and terminology_version_uuid=:terminology_version_uuid
            order by long_common_name
            """
        self.loinc_rule(query)

    def order_observation_rule(self):
        query = """
            select * from loinc.code
            where order_obs in :value
            and status = 'ACTIVE'
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
        results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.loinc_num,
                x.long_common_name,
            )
            for x in results_data
        ]
        self.results = set(results)


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
        except:
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

    def display_regex(self):
        """Process CPT rules where property=display and operator=regex, where we are string matching to displays"""
        es = get_elasticsearch()

        results = es.search(
            query={
                "simple_query_string": {
                    "fields": ["display"],
                    "query": self.value,
                }
            },
            index="cpt_codes",
            size=MAX_ES_SIZE,
        )

        search_results = [x.get("_source") for x in results.get("hits").get("hits")]
        final_results = [
            Code(
                self.fhir_system,
                self.terminology_version.version,
                x.get("code"),
                x.get("display"),
            )
            for x in search_results
        ]
        self.results = set(final_results)

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
    def include_entire_code_system(self):
        conn = get_db()
        query = """
        select * from custom_terminologies.code 
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

    def display_regex(self):
        conn = get_db()
        query = """
        select * from custom_terminologies.code 
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
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)

    def code_rule(self):
        conn = get_db()
        query = """
        select * from custom_terminologies.code
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
        results = [
            Code(self.fhir_system, self.terminology_version.version, x.code, x.display)
            for x in results_data
        ]
        self.results = set(results)


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
        use_case_uuid=None,
    ):
        conn = get_db()
        vs_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                insert into value_sets.value_set
                (uuid, name, title, publisher, contact, description, immutable, experimental, purpose, type, use_case_uuid)
                values
                (:uuid, :name, :title, :publisher, :contact, :value_set_description, :immutable, :experimental, :purpose, :vs_type, :use_case_uuid)
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
                "use_case_uuid": use_case_uuid,
            },
        )
        conn.execute(text("commit"))
        new_version_uuid = uuid.uuid4()
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
        conn.execute(text("commit"))
        return cls.load(vs_uuid)

    @classmethod
    def load(cls, vs_uuid):
        conn = get_db()
        vs_data = conn.execute(
            text(
                """
            select * from value_sets.value_set where uuid=:uuid
            """
            ),
            {"uuid": vs_uuid},
        ).first()

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
        )
        return value_set

    def serialize(self):
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
        # reject if has version
        if vs_version_data is not None:
            raise BadRequest(
                "ValueSet version is not eligible for deletion because there is an associated version"
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

        return [
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
            }
            for x in results
        ]

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
    def load_most_recent_active_version(cls, uuid):
        conn = get_db()
        query = text(
            """
            select * from value_sets.value_set_version
            where value_set_uuid = :uuid
            and status='active'
            order by version desc
            limit 1
            """
        )
        results = conn.execute(query, {"uuid": uuid})
        recent_version = results.first()
        if recent_version is None:
            raise BadRequest(
                f"No active published version of ValueSet with UUID: {uuid}"
            )
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
        conn = get_db()
        # create new value set uuid
        new_vs_uuid = uuid.uuid4()
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

        # create version in the newly duplicated value set
        new_version_uuid = uuid.uuid4()
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

        # Copy rules from original value set most recent active version into new duplicate version
        if current_app.config["MOCK_DB"] is False:
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

        return new_vs_uuid

    def create_new_version_from_previous(self, effective_start, effective_end, description):
        """
        This will identify the most recent version of the value set and clone it, incrementing the version by 1, to create a new version
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

        # Create new version
        new_version_uuid = uuid.uuid4()
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

        # Copy rules from previous version to new version
        if current_app.config["MOCK_DB"] == "False":
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

        # Copy over mapping inclusions
        if current_app.config["MOCK_DB"] == "False":
            conn.execute(
                text(
                    """
                    insert into value_sets.mapping_inclusion
                    (concept_map_uuid, relationship_types, match_source_or_target, concept_map_name, vs_version_uuid)
                    select concept_map_uuid, relationship_types, match_source_or_target, concept_map_name, :new_value_set_version_uuid from value_sets.mapping_inclusion
                    where vs_version_uuid=:previous_version_uuid
                    """
                ), {
                    "new_value_set_version_uuid": str(new_version_uuid),
                    "previous_version_uuid": str(most_recent_vs_version.uuid)
                }
            )

        # Copy over explicitly included codes
        if current_app.config["MOCK_DB"] == "False":
            conn.execute(
                text(
                    """
                    insert into value_sets.explicitly_included_code
                    (vs_version_uuid, code_uuid, review_status)
                    select :new_value_set_version_uuid, code_uuid, review_status from value_sets.explicitly_included_code
                    where vs_version_uuid = :previous_version_uuid
                    """
                ), {
                    "new_value_set_version_uuid": str(new_version_uuid),
                    "previous_version_uuid": str(most_recent_vs_version.uuid)
                }
            )

        return new_version_uuid


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
            select * 
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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
                    x.uuid,
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

            for rule in rules:
                rule.execute()

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

    @classmethod
    def create(
        cls,
        efective_start,
        effective_end,
        value_set_uuid,
        status,
        description,
        created_date,
        version,
        comments,
    ):
        conn = get_db()
        vsv_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                insert into value_sets.value_set_version
                (uuid, efective_start, effective_end, value_set_uuid, status, description, created_date, version, comments)
                values
                (:uuid, :efective_start, :effective_end, :value_set_uuid, :status, :description, :created_date, :version, :comments)
                """
            ),
            {
                "uuid": vsv_uuid,
                "efective_start": efective_start,
                "effective_end": effective_end,
                "value_set_uuid": value_set_uuid,
                "status": status,
                "description": description,
                "created_date": created_date,
                "version": version,
                "comments": comments,
            },
        )
        conn.execute(text("commit"))
        return cls.load(vsv_uuid)

    @classmethod
    def load(cls, uuid):
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
            raise NotFound(f"Value Set Version with uuid {uuid} not found")

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

        if current_app.config["MOCK_DB"] != 'True':
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

    def expand(self, force_new=False):
        if force_new is True:
            return self.create_expansion()

        if self.expansion_already_exists():
            return self.load_current_expansion()
        else:
            return self.create_expansion()

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
        self.expansion_uuid = expansion_metadata.uuid

        # print(expansion_metadata.timestamp, type(expansion_metadata.timestamp))
        self.expansion_timestamp = expansion_metadata.timestamp
        if isinstance(self.expansion_timestamp, str):
            self.expansion_timestamp = parser.parse(self.expansion_timestamp)

        query = conn.execute(
            text(
                """
            select * from value_sets.expansion_member
            where expansion_uuid = :expansion_uuid
            """
            ),
            {"expansion_uuid": self.expansion_uuid},
        )

        for x in query:
            self.expansion.add(Code(x.system, x.version, x.code, x.display))

    def save_expansion(self, report=None):
        conn = get_db()
        self.expansion_uuid = uuid.uuid1()

        # Create a new expansion entry in the value_sets.expansion table
        current_time_string = (
            datetime.now()
        )  # + timedelta(days=1) # Must explicitly create this, since SQLite can't use now()
        self.expansion_timestamp = current_time_string
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

        if self.expansion:
            conn.execute(
                expansion_member.insert(),
                [
                    {
                        "expansion_uuid": str(self.expansion_uuid),
                        "code": code.code,
                        "display": code.display,
                        "system": code.system,
                        "version": code.version,
                    }
                    for code in self.expansion
                ],
            )

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
            concept_map = DeprecatedConceptMap(
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

    # def delete(self):
    #  """
    #   Deleting a value set version is only allowed if it was only in draft status and never published--typically if it was created in error.
    #   Once a value set version has been published, it must be kept indefinitely.
    #  """
    #   # Make sure value set is eligible for deletion
    #   if self.status != 'pending':
    #     raise BadRequest('ValueSet version is not eligible for deletion because its status is not `pending`')

    #   # Identify any expansions, delete their contents, then delete the expansions themselves
    #   conn = get_db()
    #   conn.execute(
    #     text(
    #        """
    #       delete from value_sets.expansion_member
    #       where expansion_uuid in
    #       (select uuid from value_sets.expansion
    #       where vs_version_uuid=:vs_version_uuid)
    #        """
    #     ), {
    #       'vs_version_uuid': self.uuid
    #     }
    #   )

    #   conn.execute(
    #     text(
    #       """ delete from value_sets.expansion
    #       where vs_version_uuid=:vs_version_uuid
    #       """
    #     ), {
    #       'vs_version_uuid': self.uuid
    #     }
    #   )

    #   # Delete associated rules for value set version
    #   conn.execute(
    #     text(
    #       """
    #       delete from value_sets.value_set_rule
    #       where value_set_version=:vs_version_uuid
    #       """
    #     ), {
    #       'vs_version_uuid': self.uuid
    #     }
    #   )

    #   # Delete value set version
    #   conn.execute(
    #     text(
    #       """
    #       delete from value_sets.value_set_version
    #       where uuid=:vs_version_uuid
    #       """
    #     ), {
    #       'vs_version_uuid': self.uuid
    #     }
    #   )

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

    def serialize(self):
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

        # for x in self.expansion:  # id will depend on system
        #     if "http://hl7.org/fhir" in x.system:
        #         rcdm_id = x.system.split("/")[-1]
        #     else:
        #         rcdm_id = self.value_set.uuid
        rcdm_id = self.value_set.uuid

        if (
            self.status == "pending"
        ):  # has a required binding (translate pending to draft)
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
                dt = datetime.strptime(date_string, "%B %dth %Y, %I:%M:%S %p")
                rcdm_date = dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
            else:
                rcdm_date = None
        else:
            rcdm_date = None

        serialized = {
            "resourceType": "ValueSet",
            "id": rcdm_id,  # for FHIR value sets, id will be a name (e.g. publication-status).  For Ronin value sets, id will be the value set uuid.
            "meta": {
                "profile": [
                    "http://projectronin.io/fhir/StructureDefinition/ronin-valueSet"
                ]
            },
            "extension": [
                {
                    "url": "http://projectronin.io/fhir/StructureDefinition/Extension/ronin-valueSetSchema",
                    "valueString": "2",
                }
            ],
            "url": f"http://projectronin.io/fhir/ValueSet/{rcdm_id}",  # specific to the overall value set; suffix matching the id field exactly
            "version": str(self.version),  # Version must be a string
            "name": rcdm_name,  # name has to match [A-Z]([A-Za-z0-9_]){0,254}
            "status": rcdm_status,  # has a required binding (translate pending to draft)  (draft, active, retired, unknown)
            "experimental": self.value_set.experimental,
            "date": rcdm_date,  # the date the status was set to active
            "description": (self.value_set.description or "")
            + " "
            + (self.description or ""),
            "purpose": self.value_set.purpose,
            "expansion": {
                "identifier": f"urn:uuid:{self.expansion_uuid}",
                "timestamp": self.expansion_timestamp.strftime("%Y-%m-%d")
                if self.expansion_timestamp is not None
                else None,
                "total": len(self.expansion),  # total number of codes in the expansion
                "contains": [x.serialize() for x in self.expansion],
            },
        }

        if self.value_set.type == "extensional":
            all_extensional_codes = []
            for terminology, codes in self.extensional_codes.items():
                all_extensional_codes += codes
            serialized["expansion"]["contains"] = [
                x.serialize() for x in all_extensional_codes
            ]
            if (
                current_app.config["MOCK_DB"] is False
            ):  # Postgres-specific code, skip during tests
                # timestamp derived from date version was last updated
                serialized["expansion"][
                    "timestamp"
                ] = self.extensional_vs_time_last_modified().strftime("%Y-%m-%d")
                # expansion UUID derived from a hash of when timestamp was last updated and the UUID of the ValueSets terminology version from `public.terminology_versions`
                serialized["additionalData"]["expansion_uuid"] = uuid.uuid3(
                    namespace=uuid.UUID("{e3dbd59c-aa26-11ec-b909-0242ac120002}"),
                    name=str(self.extensional_vs_time_last_modified()),
                )

        serialized_exclude = self.serialize_exclude()
        if serialized_exclude:
            serialized["compose"]["exclude"] = serialized_exclude

        # if self.value_set.type == 'extensional': serialized.pop('expansion')

        return serialized

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
        """Persist newly created object to database"""
        conn = get_db()

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
                "code_uuid": self.code.uuid,
                "review_status": self.review_status,
            },
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "review_status": self.review_status,
            "value_set_version_uuid": self.value_set_version.uuid,
            "code": self.code.serialize(with_system_name=True),
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
                system_name=x.system_name,
                version=x.version,
                code=x.code,
                display=x.display,
                uuid=x.code_uuid,
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
