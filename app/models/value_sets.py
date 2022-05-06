import math
import json
import requests
import concurrent.futures
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import date, datetime, timedelta
from dateutil import parser
import werkzeug
from werkzeug.exceptions import BadRequest, NotFound

from sqlalchemy.sql.expression import bindparam
from app.models.codes import Code
from app.models.concept_maps import DeprecatedConceptMap
from app.models.terminologies import Terminology
from app.database import get_db, get_elasticsearch
from flask import current_app
import pandas as pd
import numpy as np
from pandas import json_normalize

ECL_SERVER_PATH = "https://snowstorm.prod.projectronin.io"
SNOSTORM_LIMIT = 500

# RXNORM_BASE_URL = "https://rxnav.nlm.nih.gov/REST/"
RXNORM_BASE_URL = "https://rxnav.prod.projectronin.io/REST/"

MAX_ES_SIZE = 1000

metadata = MetaData()
expansion_member = Table('expansion_member', metadata,
  Column('expansion_uuid', UUID, nullable=False),
  Column('code', String, nullable=False),
  Column('display', String, nullable=False),
  Column('system', String, nullable=False),
  Column('version', String, nullable=False),
  schema='value_sets'
)

class VSRule:
  def __init__(self, uuid, position, description, prop, operator, value, include, value_set_version, fhir_system, terminology_version):
    self.uuid = uuid
    self.position = uuid
    self.description = description
    self.property = prop
    self.operator = operator
    self.value = value
    self.include = include
    if self.include == 1: self.include = True
    if self.include == 0: self.include = False
    self.value_set_version = value_set_version
    self.terminology_version = terminology_version
    self.fhir_system = fhir_system
    
    self.results = set()
  
  def execute(self):
    if self.operator == 'descendent-of':
      self.descendent_of()
    elif self.operator == 'self-and-descendents':
      self.self_and_descendents()
    elif self.operator == 'direct-child':
      self.direct_child()
    elif self.operator == 'is-a':
      self.direct_child()
    elif self.operator == 'in' and self.property == 'concept':
      self.concept_in()
    elif self.operator == 'in-section':
      self.in_section()
    elif self.operator == 'in-chapter':
      self.in_chapter()
    elif self.operator == 'has-body-system':
      self.has_body_system()
    elif self.operator == 'has-root-operation':
      self.has_root_operation()
    elif self.operator == 'has-body-part':
      self.has_body_part()
    elif self.operator == 'has-qualifer':
      self.has_qualifier()
    elif self.operator == 'has-approach':
      self.has_approach()
    elif self.operator == "has-device":
      self.has_device()              

    if self.property == 'code' and self.operator == 'in':
      self.code_rule()
    if self.property == 'display' and self.operator == 'regex':
      self.display_regex()
    elif self.property == 'display' and self.operator == 'in':
      self.display_rule()

    # RxNorm Specific
    if self.property == 'SAB':
      self.rxnorm_source()
    if self.property == 'TTY':
      self.rxnorm_term_type()
    if self.property in ['SY', 'SIB', 'RN', 'PAR', 'CHD', 'RB', 'RO']: 
      self.rxnorm_relationship() 
    if self.property in ['permuted_term_of', 'has_quantified_form', 'constitutes', 'has_active_moiety', 'doseformgroup_of', 'ingredients_of', 'precise_active_ingredient_of', 'has_product_monograph_title', 'sort_version_of', 'precise_ingredient_of', 'has_part', 'reformulation_of', 'has_precise_ingredient', 'has_precise_active_ingredient', 'mapped_from', 'included_in', 'has_inactive_ingredient', 'has_ingredients', 'active_moiety_of', 'is_modification_of', 'isa', 'has_form', 'has_member', 'consists_of', 'form_of', 'has_entry_version', 'part_of', 'dose_form_of', 'has_print_name', 'contained_in', 'mapped_to', 'has_ingredient', 'has_basis_of_strength_substance', 'has_doseformgroup', 'has_tradename', 'basis_of_strength_substance_of', 'has_dose_form', 'inverse_isa', 'has_sort_version', 'has_active_ingredient', 'product_monograph_title_of', 'member_of', 'quantified_form_of', 'contains', 'includes', 'active_ingredient_of', 'entry_version_of', 'inactive_ingredient_of', 'reformulated_to', 'has_modification', 'ingredient_of', 'has_permuted_term', 'tradename_of', 'print_name_of']:
      self.rxnorm_relationship_type()
    if self.property == 'term_type_within_class':
      self.term_type_within_class()

    # SNOMED
    if self.property == 'ecl':
      self.ecl_query()

    # LOINC
    if self.property == 'property':
      self.property_rule()
    elif self.property == 'timing':
      self.timing_rule()
    elif self.property == 'system':
      self.system_rule()
    elif self.property == 'component':
      self.component_rule()
    elif self.property == 'scale':
      self.scale_rule()
    elif self.property == 'method':
      self.method_rule()
      
  def direct_child(self):
    pass
  
  def descendent_of(self):
    pass

  def serialize(self):
    return {
      "property": self.property,
      "op": self.operator,
      "value": self.value
    }

class ICD10CMRule(VSRule):
  def direct_child(self):
    pass

  def code_rule(self):
    conn = get_db()
    query = ""
    
    if self.property == 'code':
      # Lookup UUIDs for provided codes
      codes = self.value.replace(' ', '').split(',')
      
      # Get all descendants of the provided codes through a recursive query
      query = """
      select code, display from icd_10_cm.code 
      where code in :codes 
      and version_uuid=:version_uuid
      order by code
      """
      # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/
      
    converted_query = text(
        query
      ).bindparams(bindparam('codes', expanding=True))

    results_data = conn.execute(
      converted_query, {
        'codes': codes,
        'version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
    self.results = set(results)

  def self_and_descendents(self):
    conn = get_db()
    query = ""
    
    if self.property == 'code':
      # Lookup UUIDs for provided codes
      codes = self.value.replace(' ', '').split(',')
      
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
      
    converted_query = text(
        query
      ).bindparams(bindparam('codes', expanding=True))

    results_data = conn.execute(
      converted_query, {
        'codes': codes,
        'version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
    self.results = set(results)
  
  def descendent_of(self):
    conn = get_db()
    query = ""
    
    if self.property == 'code':
      # Lookup UUIDs for provided codes
      codes = self.value.split(',')
      
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
      
    converted_query = text(
        query
      ).bindparams(bindparam('codes', expanding=True))

    results_data = conn.execute(
      converted_query, {
        'codes': codes,
        'version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
    self.results = set(results)

  def in_section(self):
    conn = get_db()

    query = """
      select * from icd_10_cm.code
      where section_uuid=:section_uuid
      and version_uuid = :version_uuid
    """

    results_data = conn.execute(
      text(
        query
      ), {
        'section_uuid': self.value,
        'version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
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
      text(
        query
      ), {
        'chapter_uuid': self.value,
        'version_uuid' : self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
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

    results_data = conn.execute(
      text(
        query
      ), {
        'value': self.value
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.conceptid, x.term) for x in results_data]
    self.results = set(results)

  def ecl_query(self):
    offset = 0
    self.results = set()
    results_complete = False

    while results_complete is False:
      branch = "MAIN"
      r = requests.get(f"{ECL_SERVER_PATH}/{branch}/{self.terminology_version.version}/concepts", params={
        'ecl': self.value,
        'limit': SNOSTORM_LIMIT,
        'offset': offset
      })

      if 'error' in r.json():
        raise BadRequest(r.json().get('message'))

      # Handle pagination
      total_results = r.json().get("total")
      pages = int(math.ceil(total_results / SNOSTORM_LIMIT))
      offset += SNOSTORM_LIMIT
      if offset >= pages * SNOSTORM_LIMIT:
        results_complete = True

      # Add data to results
      data = r.json().get("items")
      results = [Code(self.fhir_system, self.terminology_version.version, x.get("conceptId"), x.get("fsn").get("term")) for x in data]
      self.results.update(set(results))

class RxNormRule(VSRule):
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
    return requests.get(f'{RXNORM_BASE_URL}rxcui/{rxcui}/properties.json?').json()

  def load_additional_members_of_class(self, rxcui):
    data = requests.get(f'{RXNORM_BASE_URL}rxcui/{rxcui}/allrelated.json?').json()
    return self.json_extract(data,'rxcui')

  def term_type_within_class(self):
    json_value = json.loads(self.value)
    rela_source = json_value.get('rela_source')
    class_id = json_value.get('class_id')
    term_type = json_value.get('term_type')
    
    # This calls the RxClass API to get its members 
    payload = {'classId': class_id, 'relaSource': rela_source}
    class_request = requests.get(f'{RXNORM_BASE_URL}rxclass/classMembers.json?', params=payload)
    
    # Extracts a list of RxCUIs from the JSON response
    rxcuis = self.json_extract(class_request.json(),'rxcui')

    # Calls the related info RxNorm API to get additional members of the drug class      
    related_rxcuis = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
      results = pool.map(self.load_additional_members_of_class, rxcuis)
      for result in results:
        related_rxcuis.append(result)

    # Appending RxCUIs to the first list of RxCUIs and removing empty RxCUIs
    flat_list = [item for sublist in related_rxcuis for item in sublist]
    de_duped_list = list(set(flat_list))
    if '' in de_duped_list:
      de_duped_list.remove('')
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
      properties = item.get('properties')
      result_term_type = properties.get('tty')
      display = properties.get('name')
      code = properties.get('rxcui')
      if result_term_type in term_type:
        final_rxnorm_codes.append(Code(self.fhir_system, self.terminology_version.version, code, display))

    self.results = set(final_rxnorm_codes)

  def rxnorm_source(self):
    conn = get_db()
    query = text("""
      Select RXCUI, str from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY' 
      and RXCUI in (select RXCUI from "rxnormDirty".rxnconso where SAB in :value)
    """).bindparams(bindparam('value', expanding=True))

    value = self.value.split(',')

    results_data = conn.execute(query, {
      'value': value
    })

    results = [Code(self.fhir_system, self.terminology_version.version, x.rxcui, x.str) for x in results_data]
    self.results = set(results)

  def rxnorm_term_type(self):
    conn = get_db()
    query = text("""
      Select RXCUI, str from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY' 
      and TTY in :value
    """).bindparams(bindparam('value', expanding=True))

    value = self.value.split(',')

    results_data = conn.execute(query, {
      'value': value
    })

    results = [Code(self.fhir_system, self.terminology_version.version, x.rxcui, x.str) for x in results_data]
    self.results = set(results)


  def rxnorm_relationship(self):
    conn = get_db()
    if self.value[:4] == 'CUI:':
      query = text(""" Select RXCUI, STR from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY'  
      and (RXCUI in (select RXCUI from "rxnormDirty".rxnconso where RXCUI in (select RXCUI1 from "rxnormDirty".rxnrel where REL = :rel and RXCUI2 in :value)))""") 
    else:
      query = text(""" Select RXCUI, STR from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY'  
      and (RXCUI in (select RXCUI from "rxnormDirty".rxnconso where RXAUI in (select RXAUI1 from "rxnormDirty".rxnrel where REL = :rel and RXAUI2 in :value)))""") 

    query = query.bindparams(bindparam('value', expanding=True))

    value = self.value[4:].split(',') 

    results_data = conn.execute(query, {
      'value': value, 
      'rel': self.property
    })

    results = [Code(self.fhir_system, self.terminology_version.version, x.rxcui, x.str) for x in results_data]
    self.results = set(results)

  def rxnorm_relationship_type(self):
    conn = get_db()
    if self.value[:4] == 'CUI:':
      query = text(""" Select RXCUI, STR from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY'  
      and (RXCUI in (select RXCUI from "rxnormDirty".rxnconso where RXCUI in (select RXCUI1 from "rxnormDirty".rxnrel where RELA = :rel and RXCUI2 in :value)))""") 
    else:
      query = text(""" Select RXCUI, STR from "rxnormDirty".rxnconso where SAB = 'RXNORM' and TTY <> 'SY'  
      and (RXCUI in (select RXCUI from "rxnormDirty".rxnconso where RXAUI in (select RXAUI1 from "rxnormDirty".rxnrel where RELA = :rel and RXAUI2 in :value)))""") 

    query = query.bindparams(bindparam('value', expanding=True))

    value = self.value[4:].split(',') 

    results_data = conn.execute(query, {
      'value': value, 
      'rel': self.property
    })

    results = [Code(self.fhir_system, self.terminology_version.version, x.rxcui, x.str) for x in results_data]
    self.results = set(results)

class LOINCRule(VSRule):

  def loinc_rule(self, query):
    conn = get_db()

    converted_query = text(
        query
      ).bindparams(bindparam('value', expanding=True))

    results_data = conn.execute(
      converted_query, 
      {
        'value': self.split_value,
        'terminology_version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.loinc_num, x.long_common_name) for x in results_data]
    self.results = set(results)

  @property
  def split_value(self):
    """
    ReTool saves arrays like this: {"Alpha-1-Fetoprotein","Alpha-1-Fetoprotein Ab","Alpha-1-Fetoprotein.tumor marker"}
    Sometimes, we also save arrays like this: Alpha-1-Fetoprotein,Alpha-1-Fetoprotein Ab,Alpha-1-Fetoprotein.tumor marker

    This function will handle both formats and output a python list of strings
    """
    new_value = self.value
    if new_value[:1] == '{' and new_value[-1:] == '}': 
      new_value = new_value[1:]
      new_value = new_value[:-1]
    new_value = new_value.split(',')
    new_value = [(x[1:] if x[:1]=='"' else x) for x in new_value]
    new_value = [(x[:-1] if x[-1:]=='"' else x) for x in new_value]
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
      text(query), {
        'terminology_version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.loinc_num, x.long_common_name) for x in results_data]
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

class ICD10PCSRule(VSRule):

  def icd_10_pcs_rule(self, query):
    conn = get_db()

    converted_query = text(
        query
    ).bindparams(bindparam('value', expanding=True))

    results_data = conn.execute(
      converted_query,
      {
        'value': self.value,
        'version_uuid': self.terminology_version.uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.display) for x in results_data]
    self.results = set(results)

  def in_section(self):
    query = """
    select * from icd_10_pcs.code
    where section = :value
    and version_uuid = :version_uuid
    """
    self.icd_10_pcs_rule(query)

  def has_body_system(self):
    query = """
    select * from icd_10_pcs.code
    where body_system = :value
    and version_uuid = :version_uuid
    """
    self.icd_10_pcs_rule(query)

  def has_root_operation(self):
    query = """
    select * from icd_10_pcs.code
    where root_operation = :value
    and version_uuid = :version_uuid
    """
    self.icd_10_pcs_rule(query)

  def has_body_part(self):
    query = """
    select * from icd_10_pcs.code
    where body_part = :value
    and version_uuid = :version_uuid
    """
    self.icd_10_pcs_rule(query)
  
  def has_approach(self):
    query = """
    select * from icd_10_pcs.code
    where approach = :value
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
    where qualifier = :value
    and version_uuid = :version_uuid
    """
    self.icd_10_pcs_rule(query)
   


class CPTRule(VSRule):
  @staticmethod
  def parse_cpt_retool_array(retool_array):
    array_string_copy = retool_array
    array_string_copy = array_string_copy[1:]
    array_string_copy = array_string_copy[:-1]
    array_string_copy = '[' + array_string_copy + ']'
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
    """ Process CPT rules where property=code and operator=in, where we are selecting codes from a range """
    parsed_value = self.parse_input_array(self.value)

    # Input may be list of dicts with a 'range' key, or may be list of ranges directly
    if type(parsed_value[0]) == dict:
      ranges = [x.get('range') for x in parsed_value]
    else:
      ranges = [x for x in parsed_value]

    # Since each range in the above array may include multiple ranges, we need to re-join them and then split them apart
    ranges = ','.join(ranges)
    ranges = ranges.replace(' ', '')
    ranges = ranges.split(',')

    where_clauses = []

    for x in ranges:
      if '-' in x:
        start, end = x.split('-')
        start_number, start_letter = self.parse_code_number_and_letter(start)
        end_number, end_letter = self.parse_code_number_and_letter(end)

        if start_letter != end_letter:
          raise Exception(f'Letters in CPT code range do not match: {start_letter} and {end_letter}')

        where_clauses.append(f"(code_number between {start_number} and {end_number} and code_letter {'=' if start_letter is not None else 'is'} {start_letter if start_letter is not None else 'null'})")
      else:
        code_number, code_letter = self.parse_code_number_and_letter(x)
        where_clauses.append(f"(code_number={code_number} and code_letter {'=' if code_letter is not None else 'is'} {code_letter if code_letter is not None else 'null'})")

    query = "select * from cpt.code where " + ' or '.join(where_clauses)

    conn = get_db()
    results_data = conn.execute(
      text(query)
    )
    results = [Code(self.fhir_system, self.terminology_version.version, x.code, x.long_description) for x in results_data]
    self.results = set(results)

  def display_regex(self):
    """ Process CPT rules where property=display and operator=regex, where we are string matching to displays """
    es = get_elasticsearch()

    results = es.search(
      query={
        "simple_query_string": {
          "fields": ["display"],
          "query": self.value,
          }
        },
      index="cpt_codes",
      size=MAX_ES_SIZE
    )

    search_results = [x.get('_source') for x in results.get('hits').get('hits')]
    final_results = [Code(self.fhir_system, self.terminology_version.version, x.get('code'), x.get('display')) for x in search_results]
    self.results = set(final_results)
    

# class GroupingValueSetRule(VSRule):
#   def most_recent_active_version(self):
#     version = ValueSet.load_most_recent_active_version(name)
#     version.expand()
#     results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
#     self.results = set(results)

#   def specific_version(self):
#     pass

class ValueSet:
  def __init__(self, uuid, name, title, publisher, contact, description, immutable, experimental, purpose, vs_type, synonyms={}):
    self.uuid = uuid
    self.name = name
    self.title = title
    self.publisher = publisher
    self.contact = contact
    self.description = description
    self.immutable = immutable
    self.experimental = experimental
    if self.experimental == 1: self.experimental = True
    if self.experimental == 0: self.experimental = False
    self.purpose = purpose
    self.type = vs_type
    self.synonyms=synonyms
  
  @classmethod
  def load(cls, uuid):
    conn = get_db()
    vs_data = conn.execute(text(
      """
      select * from value_sets.value_set where uuid=:uuid
      """
    ), {
      'uuid': uuid
    }).first()

    synonym_data = conn.execute(text(
      """
      select context, synonym
      from resource_synonyms
      where resource_uuid=:uuid
      """
    ), {
      'uuid': uuid
    })
    synonyms = {x.context: x.synonym for x in synonym_data}
    
    value_set = cls(vs_data.uuid, 
               vs_data.name, vs_data.title, vs_data.publisher, 
               vs_data.contact, vs_data.description, 
               vs_data.immutable, vs_data.experimental, vs_data.purpose, vs_data.type,
               synonyms)
    return value_set

  @classmethod
  def load_all_value_set_metadata(cls, active_only=True):
    conn = get_db()

    if active_only is True:
      results = conn.execute(text(
        """
        select * from value_sets.value_set
        where uuid in 
        (select value_set_uuid from value_sets.value_set_version
        where status='active')
        """
      ))
    else:
      results = conn.execute(text(
        """
        select * from value_sets.value_set
        where uuid in 
        (select value_set_uuid from value_sets.value_set_version)
        """
      ))

    return [
      {
        'uuid': x.uuid,
        'name': x.name,
        'title': x.title,
        'publisher': x.publisher,
        'contact': x.contact,
        'description': x.description,
        'immutable': x.immutable,
        'experimental': x.experimental,
        'purpose': x.purpose,
        'type': x.type
      } for x in results
    ]

  @classmethod
  def load_all_value_sets_by_status(cls, status):
    conn = get_db()
    query = text(
      """
      select uuid from value_sets.value_set_version
      where status in :status
      """
      ).bindparams(bindparam('status', expanding=True))
    results = conn.execute(query, {
        'status': status
      })

    return [ValueSetVersion.load(x.uuid) for x in results]

  @classmethod
  def name_to_uuid(cls, identifier):
    """ Returns the UUID for a ValueSet, given either a name or UUID"""
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
        ), {
          'name': identifier
        }
      ).first()
      return result.uuid

  @classmethod
  def load_version_metadata(cls, uuid):
    conn = get_db()
    results = conn.execute(text(
      """
      select * from value_sets.value_set_version
      where value_set_uuid = :uuid
      order by version desc
      """
    ), {
      'uuid': uuid
    })
    return [
      {
        'uuid': x.uuid,
        'effective_start': x.effective_start,
        'effective_end': x.effective_end,
        'status': x.status,
        'description': x.description,
        'created_date': x.created_date,
        'version': x.version
      } for x in results
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
    results = conn.execute(query, {
      'uuid': uuid
    })
    recent_version = results.first()
    if recent_version is None:
      raise BadRequest(f'No active published version of ValueSet with UUID: {uuid}')
    return ValueSetVersion.load(recent_version.uuid)

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

    terminologies = Terminology.load_terminologies_for_value_set_version(self.vs_version_uuid)
    
    rules_data = conn.execute(text(
      """
      select * 
      from value_sets.value_set_rule 
      join terminology_versions
      on terminology_version=terminology_versions.uuid
      where value_set_version=:vs_version
      and rule_group=:rule_group
      """
    ), {
      'vs_version': self.vs_version_uuid,
      'rule_group': self.rule_group_id
    })
    
    for x in rules_data:
      terminology = terminologies.get(x.terminology_version)
      rule = None
      
      if terminology.name == "ICD-10 CM":
        rule = ICD10CMRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, terminologies.get(x.terminology_version))
      elif terminology.name == "SNOMED CT":
        rule = SNOMEDRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, terminologies.get(x.terminology_version))
      elif terminology.name == "RxNorm":
        rule = RxNormRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, terminologies.get(x.terminology_version))
      elif terminology.name == "LOINC":
        rule = LOINCRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, terminologies.get(x.terminology_version))
      elif terminology.name == "CPT":
        rule = CPTRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, terminologies.get(x.terminology_version))
        
      if terminology in self.rules:
        self.rules[terminology].append(rule)
      else:
        self.rules[terminology] = [rule]
  
  # Move execute, so that the logic previously kept at a version level is now at a rule group level
  def generate_expansion(self):
    self.expansion = set()
    terminologies = self.rules.keys()
    expansion_report = f"EXPANDING RULE GROUP {self.rule_group_id}\n"
    
    for terminology in terminologies:
      expansion_report += f"\nProcessing rules for terminology {terminology.name} version {terminology.version}\n"

      rules = self.rules.get(terminology)
      
      for rule in rules: rule.execute()
        
      include_rules = [x for x in rules if x.include is True]
      exclude_rules = [x for x in rules if x.include is False]

      expansion_report += "\nInclusion Rules\n"
      for x in include_rules:
        expansion_report += f"{x.description}, {x.property}, {x.operator}, {x.value}\n"
      expansion_report += "\nExclusion Rules\n"
      for x in exclude_rules:
        expansion_report += f"{x.description}, {x.property}, {x.operator}, {x.value}\n"
      
      terminology_set = include_rules.pop(0).results
      # todo: if it's a grouping value set, we should use union instead of intersection
      for x in include_rules: terminology_set = terminology_set.intersection(x.results)

      expansion_report += "\nIntersection of Inclusion Rules\n"
      
      # .join w/ a list comprehension used for performance reasons
      expansion_report += "".join([f"{x.code}, {x.display}, {x.system}, {x.version}\n" for x in terminology_set])

      for x in exclude_rules: 
        remove_set = terminology_set.intersection(x.results)
        terminology_set = terminology_set - remove_set

        expansion_report += f"\nProcessing Exclusion Rule: {x.description}, {x.property}, {x.operator}, {x.value}\n"
        expansion_report += "The following codes were removed from the set:\n"
        # for removed in remove_set:
        #   expansion_report += f"{removed.code}, {removed.display}, {removed.system}, {removed.version}\n"
        expansion_report += "".join([f"{removed.code}, {removed.display}, {removed.system}, {removed.version}\n" for removed in remove_set])
        
      self.expansion = self.expansion.union(terminology_set)

      expansion_report += f"\nThe expansion will contain the following codes for the terminology {terminology.name}:\n"
      
      # .join w/ a list comprehension used for performance reasons
      expansion_report += "".join([f"{x.code}, {x.display}, {x.system}, {x.version}\n" for x in terminology_set])
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
      serialized.append({
        'system': key.fhir_uri,
        'version': key.version,
        'filter': serialized_rules
      })
    return serialized

  def serialize_exclude(self):
    exclude_rules = self.exclude_rules
    terminology_keys = exclude_rules.keys()
    serialized = []
    
    for key in terminology_keys:
      rules = exclude_rules.get(key)
      serialized_rules = [x.serialize() for x in rules]
      serialized.append({
        'system': key.fhir_uri,
        'version': key.version,
        'filter': serialized_rules
      })

    return serialized
  
  # Move include and exclude rule properties to here
  @property
  def include_rules(self):
    keys = self.rules.keys()
    include_rules = {}
    
    for key in keys:
      rules_for_terminology = self.rules.get(key)
      include_rules_for_terminology = [x for x in rules_for_terminology if x.include is True]
      if include_rules_for_terminology:
        include_rules[key] = include_rules_for_terminology
    
    return include_rules

  @property
  def exclude_rules(self):
    keys = self.rules.keys()
    exclude_rules = {}
    
    for key in keys:
      rules_for_terminology = self.rules.get(key)
      exclude_rules_for_terminology = [x for x in rules_for_terminology if x.include is False]
      if exclude_rules_for_terminology:
        exclude_rules[key] = exclude_rules_for_terminology
    
    return exclude_rules


class ValueSetVersion:
  def __init__(self, uuid, effective_start, effective_end, version, value_set, status, description):
    self.uuid = uuid
    self.effective_start = effective_start
    self.effective_end = effective_end
    self.version = version
    self.value_set = value_set
    self.status = status
    self.description = description
    self.version = version
    self.expansion_uuid = None
    
    # self.rules = {}
    self.rule_groups = []
    self.expansion = set()
    self.expansion_timestamp = None
    self.extensional_codes = {}
  
  @classmethod
  def load(cls, uuid):
    conn = get_db()
    vs_version_data = conn.execute(text(
      """
      select * from value_sets.value_set_version where uuid=:uuid
      """
    ), {
      'uuid': uuid
    }).first()

    if vs_version_data is None: raise NotFound(f'Value Set Version with uuid {uuid} not found')
    
    value_set = ValueSet.load(vs_version_data.value_set_uuid)
    
    value_set_version = cls(vs_version_data.uuid, 
               vs_version_data.effective_start, 
               vs_version_data.effective_end, 
               vs_version_data.version, 
               value_set, 
               vs_version_data.status, 
               vs_version_data.description)
    value_set_version.load_rules()

    if value_set.type == 'extensional':
      extensional_members_data = conn.execute(text(
        """
        select * from value_sets.extensional_member
        join terminology_versions tv
        on terminology_version_uuid=tv.uuid
        where vs_version_uuid=:uuid
        """
      ), {
        'uuid': uuid
      })
      extensional_data = [x for x in extensional_members_data]

      for item in extensional_data:
        code = Code(item.fhir_uri, item.version, item.code, item.display)

        if (item.fhir_uri, item.version) not in value_set_version.extensional_codes:
          value_set_version.extensional_codes[(item.fhir_uri, item.version)] = [code]
        else:
          value_set_version.extensional_codes[(item.fhir_uri, item.version)].append(code)

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
      ), {
        'vs_version_uuid': self.uuid
      }
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
    query = conn.execute(text(
      """
      select * from value_sets.expansion
      where vs_version_uuid=:version_uuid
      order by timestamp desc
      """
      ), {
        'version_uuid': self.uuid
      })
    if query.first() is not None:
      return True
    return False

  def load_current_expansion(self):
    conn = get_db()

    expansion_metadata = conn.execute(text(
      """
      select uuid, timestamp from value_sets.expansion
      where vs_version_uuid=:version_uuid
      order by timestamp desc
      limit 1
      """
    ), {
      'version_uuid': self.uuid
    }).first()
    self.expansion_uuid = expansion_metadata.uuid

    # print(expansion_metadata.timestamp, type(expansion_metadata.timestamp))
    self.expansion_timestamp = expansion_metadata.timestamp
    if isinstance(self.expansion_timestamp, str):
      self.expansion_timestamp = parser.parse(self.expansion_timestamp)

    query = conn.execute(text(
      """
      select * from value_sets.expansion_member
      where expansion_uuid = :expansion_uuid
      """
    ), {
      'expansion_uuid': self.expansion_uuid
    })

    for x in query:
      self.expansion.add(
        Code(
          x.system, x.version, x.code, x.display
        )
      )

  def save_expansion(self, report=None):
    conn = get_db()
    self.expansion_uuid = uuid.uuid1()

    # Create a new expansion entry in the value_sets.expansion table
    current_time_string = datetime.now() # + timedelta(days=1) # Must explicitly create this, since SQLite can't use now()
    self.expansion_timestamp = current_time_string
    conn.execute(text(
      """
      insert into value_sets.expansion
      (uuid, vs_version_uuid, timestamp, report)
      values
      (:expansion_uuid, :version_uuid, :curr_time, :report)
      """
    ), {
      'expansion_uuid': str(self.expansion_uuid),
      'version_uuid': str(self.uuid),
      'report': report,
      'curr_time': current_time_string
    })

    conn.execute(expansion_member.insert(), [{
      'expansion_uuid': str(self.expansion_uuid),
      'code': code.code,
      'display': code.display,
      'system': code.system,
      'version': code.version
    } for code in self.expansion])

  def create_expansion(self):
    self.expansion = set()
    expansion_report_combined = ""

    for rule_group in self.rule_groups:
      expansion, expansion_report = rule_group.generate_expansion()
      self.expansion = self.expansion.union(expansion)
      expansion_report_combined += expansion_report

    self.process_mapping_inclusions()

    if self.value_set.type == 'intensional':
      self.save_expansion(report=expansion_report_combined)

  def parse_mapping_inclusion_retool_array(self, retool_array):
    array_string_copy = retool_array
    array_string_copy = array_string_copy[1:]
    array_string_copy = array_string_copy[:-1]
    array_string_copy = '[' + array_string_copy + ']'
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
      ), {
        'version_uuid': self.uuid
      }
    )
    mapping_inclusions = [x for x in mapping_inclusions_query]

    for inclusion in mapping_inclusions:
      print("Inclusion", inclusion)
      # Load appropriate concept maps
      allowed_relationship_types = self.parse_mapping_inclusion_retool_array(inclusion.relationship_types)
      concept_map = DeprecatedConceptMap(None, allowed_relationship_types, inclusion.concept_map_name)
      
      if inclusion.match_source_or_target == 'source':
        mappings = concept_map.source_code_to_target_map
      elif inclusion.match_source_or_target == 'target':
        mappings = concept_map.target_code_to_source_map

      # Identify mapped codes and insert into expansion
      codes_to_add_to_expansion = []
      for item in self.expansion:
        if item.code in mappings:
          print("Adding codes", mappings[item.code])
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
      ), {
        'vs_version_uuid': str(self.uuid)
      }
    )
    result = last_modified_query.first()
    return result.timestamp

  def serialize_include(self):
    if self.value_set.type == 'extensional':
      keys = self.extensional_codes.keys()
      serialized = []

      for key in keys:
        terminology = key[0]
        version = key[1]
        serialized_codes = [x.serialize(with_system_and_version=False) for x in self.extensional_codes.get(key)]

        serialized.append({
          'system': terminology,
          'version': version,
          'concept': serialized_codes
        })
      
      return serialized

    elif self.value_set.type == 'intensional':
      serialized = []
      for group in self.rule_groups:
        serialized_rules = group.serialize_include()
        for rule in serialized_rules: serialized.append(rule)
      return serialized

  def serialize_exclude(self):
    if self.value_set.type == 'intensional':
      serialized = []
      for item in [x.serialize_exclude() for x in self.rule_groups]:
        if item != []: 
          for rule in item: serialized.append(rule)
      return serialized
    
    else: # No exclude for extensional
      return []

  def serialize(self):
    serialized = {
      # "url": self.value_set.url,
      "id": self.value_set.uuid,
      "name": self.value_set.name,
      "title": self.value_set.title,
      "publisher": self.value_set.publisher,
      "contact": [{
        'name': self.value_set.contact}],
      "description": self.value_set.description + ' ' + self.description,
      "immutable": self.value_set.immutable,
      "experimental": self.value_set.experimental,
      "purpose": self.value_set.purpose,
      "version": str(self.version), # Version must be a string
      "status": self.status,
      "expansion": {
        "contains": [x.serialize() for x in self.expansion],
        "timestamp": self.expansion_timestamp.strftime("%Y-%m-%d") if self.expansion_timestamp is not None else None
      },
      "compose": {
        "include": self.serialize_include()
      },
      "resourceType": "ValueSet",
      "additionalData": {  # Place to put custom values that aren't part of the FHIR spec
        "effective_start": self.effective_start,
        "effective_end": self.effective_end,
        "version_uuid": self.uuid,
        "value_set_uuid": self.value_set.uuid,
        "expansion_uuid": self.expansion_uuid,
        "synonyms": self.value_set.synonyms
      }
    }

    if self.value_set.type == 'extensional':
      all_extensional_codes = []
      for terminology, codes in self.extensional_codes.items():
        all_extensional_codes += codes
      serialized['expansion']['contains'] = [x.serialize() for x in all_extensional_codes]
      if current_app.config['MOCK_DB'] is False: # Postgres-specific code, skip during tests
        # timestamp derived from date version was last updated
        serialized['expansion']['timestamp'] = self.extensional_vs_time_last_modified().strftime("%Y-%m-%d")
        # expansion UUID derived from a hash of when timestamp was last updated and the UUID of the ValueSets terminology version from `public.terminology_versions`
        serialized['additionalData']['expansion_uuid'] = uuid.uuid3(namespace=uuid.UUID('{e3dbd59c-aa26-11ec-b909-0242ac120002}'), name=str(self.extensional_vs_time_last_modified()))

    serialized_exclude = self.serialize_exclude()
    if serialized_exclude:
      serialized['compose']['exclude'] = serialized_exclude

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
      ), {
        'expansion_uuid': expansion_uuid
      }
    ).first()
    return result.report

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
  terminology_versions_query = conn.execute(text(
    """
    select * from terminology_versions
    """
  ))
  terminology_versions = [x for x in terminology_versions_query]

  uuid_to_name_map = {str(x.uuid): x for x in terminology_versions}

  rules = []
  for rule in rules_json:
    terminology_name = uuid_to_name_map.get(rule.get('terminology_version')).terminology
    fhir_uri = uuid_to_name_map.get(rule.get('terminology_version')).fhir_uri
    terminology_version = uuid_to_name_map.get(rule.get('terminology_version'))
    rule_property = rule.get('property')
    operator = rule.get('operator')
    value = rule.get('value')
    include = rule.get('include')

    if terminology_name == "ICD-10 CM":
      rule = ICD10CMRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)
    elif terminology_name == "SNOMED CT":
      rule = SNOMEDRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)
    elif terminology_name == "RxNorm":
      rule = RxNormRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)
    elif terminology_name == "LOINC":
      rule = LOINCRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)
    elif terminology_name == "CPT":
      rule = CPTRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)
    elif terminology_name == "ICD-10 PCS":
      rule = ICD10PCSRule(None, None, None, rule_property, operator, value, include, None, fhir_uri, terminology_version)  

    rules.append(rule)

  for rule in rules: rule.execute()
        
  include_rules = [x for x in rules if x.include is True]
  exclude_rules = [x for x in rules if x.include is False]

  terminology_set = include_rules.pop(0).results
  for x in include_rules: terminology_set = terminology_set.intersection(x.results)

  for x in exclude_rules: 
        remove_set = terminology_set.intersection(x.results)
        terminology_set = terminology_set - remove_set

  return [x.serialize() for x in list(terminology_set)]
