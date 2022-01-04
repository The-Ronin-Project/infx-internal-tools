import requests
from sqlalchemy import create_engine, text
from uuid import uuid1

from sqlalchemy.sql.expression import bindparam
from app.models.codes import Code
from app.database import get_db

ECL_SERVER_PATH = "https://snowstorm.prod.projectronin.io/MAIN/concepts"

class VSRule:
  def __init__(self, uuid, position, description, prop, operator, value, include, value_set_version, fhir_system, terminology_version, terminology_version_uuid=None):
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
    self.terminology_version_uuid = terminology_version_uuid
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

    # RxNorm Specific
    if self.property == 'SAB':
      self.rxnorm_source()
    if self.property == 'TTY':
      self.rxnorm_term_type()
    if self.property in ['SY', 'SIB', 'RN', 'PAR', 'CHD', 'RB', 'RO']: 
      self.rxnorm_relationship() 
    if self.property in ['permuted_term_of', 'has_quantified_form', 'constitutes', 'has_active_moiety', 'doseformgroup_of', 'ingredients_of', 'precise_active_ingredient_of', 'has_product_monograph_title', 'sort_version_of', 'precise_ingredient_of', 'has_part', 'reformulation_of', 'has_precise_ingredient', 'has_precise_active_ingredient', 'mapped_from', 'included_in', 'has_inactive_ingredient', 'has_ingredients', 'active_moiety_of', 'is_modification_of', 'isa', 'has_form', 'has_member', 'consists_of', 'form_of', 'has_entry_version', 'part_of', 'dose_form_of', 'has_print_name', 'contained_in', 'mapped_to', 'has_ingredient', 'has_basis_of_strength_substance', 'has_doseformgroup', 'has_tradename', 'basis_of_strength_substance_of', 'has_dose_form', 'inverse_isa', 'has_sort_version', 'has_active_ingredient', 'product_monograph_title_of', 'member_of', 'quantified_form_of', 'contains', 'includes', 'active_ingredient_of', 'entry_version_of', 'inactive_ingredient_of', 'reformulated_to', 'has_modification', 'ingredient_of', 'has_permuted_term', 'tradename_of', 'print_name_of']:
      self.rxnorm_relationship_type()
    if self.property == 'ecl':
      self.ecl_query()
      
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

class ICD10PCSRule(VSRule):
  def direct_child(self):
      return super().direct_child()

class ICD10CMRule(VSRule):
  def direct_child(self):
    pass

  def self_and_descendents(self):
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
        'version_uuid': self.terminology_version_uuid
      }
    )
    results = [Code(self.fhir_system, self.terminology_version, x.code, x.display) for x in results_data]
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
        where code in :codes)
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
        'codes': codes
      }
    )
    results = [Code(self.fhir_system, self.terminology_version, x.code, x.display) for x in results_data]
    print('results', results)
    self.results = set(results)
      
class SNOMEDRule(VSRule):
  def direct_child(self):
    conn = get_db()
    query = ""
    
    if self.property == 'concept':
      # Lookup UUIDs for provided codes
      codes = self.value.split(',')
      
      # Get all descendants of the provided codes through a recursive query
      query = """
      select * from snomedct.relationship_f rel
      join snomedct.description_f descr
      on descr.conceptid=rel.sourceid
      and descr.typeid='900000000000003001'
      where destinationid in :codes 
      and rel.typeid='116680003'
      """
      # See link for tutorial in recursive queries: https://www.cybertec-postgresql.com/en/recursive-queries-postgresql/
      
    results_data = conn.execute(
      text(
        query
      ).bindparams(bindparam('codes', expanding=True)), {
        'codes': codes
      }
    )
    results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
    self.results = set(results)

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
    results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
    self.results = set(results)

  def ecl_query(self):
    r = requests.get(ECL_SERVER_PATH, params={
      'ecl': self.value
    })
    data = r.json().get("items")
    results = [Code(self.fhir_system, self.terminology_version, x.get("conceptId"), x.get("fsn").get("term")) for x in data]
    self.results = set(results)

class RxNormRule(VSRule):
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

    results = [Code(self.fhir_system, self.terminology_version, x.rxcui, x.str) for x in results_data]
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

    results = [Code(self.fhir_system, self.terminology_version, x.rxcui, x.str) for x in results_data]
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

    results = [Code(self.fhir_system, self.terminology_version, x.rxcui, x.str) for x in results_data]
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

    results = [Code(self.fhir_system, self.terminology_version, x.rxcui, x.str) for x in results_data]
    self.results = set(results)

# class GroupingValueSetRule(VSRule):
#   def most_recent_active_version(self):
#     version = ValueSet.load_most_recent_active_version(name)
#     version.expand()
#     results = [Code(self.fhir_system, self.terminology_version, x.conceptid, x.term) for x in results_data]
#     self.results = set(results)

#   def specific_version(self):
#     pass

class ValueSet:
  def __init__(self, uuid, name, title, publisher, contact, description, immutable, experimental, purpose, vs_type):
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
    
    value_set = cls(vs_data.uuid, 
               vs_data.name, vs_data.title, vs_data.publisher, 
               vs_data.contact, vs_data.description, 
               vs_data.immutable, vs_data.experimental, vs_data.purpose, vs_data.type)
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
        where status='Active')
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
  def load_version_metadata(cls, name):
    conn = get_db()
    results = conn.execute(text(
      """
      select * from value_sets.value_set_version
      where value_set_uuid in
      (select uuid from value_sets.value_set
      where name=:vs_name)
      order by version desc
      """
    ), {
      'vs_name': name
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
  def load_most_recent_active_version(cls, name):
    conn = get_db()
    query = text(
      """
      select * from value_sets.value_set_version
      where value_set_uuid in
      (select uuid from value_sets.value_set
      where name=:vs_name)
      and status='Active'
      order by version desc
      limit 1
      """
      )
    results = conn.execute(query, {
      'vs_name': name
    })
    recent_version = results.first()
    return ValueSetVersion.load(recent_version.uuid)


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
    
    self.rules = {}
    self.expansion = set()
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
        code = Code(item.terminology, item.version, item.code, item.display)

        if (item.fhir_uri, item.version) not in value_set_version.extensional_codes:
          value_set_version.extensional_codes[(item.fhir_uri, item.version)] = [code]
        else:
          value_set_version.extensional_codes[(item.fhir_uri, item.version)].append(code)

    return value_set_version
  
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

  def load_rules(self):
    """
    Rules will be structured as a dictionary where each key is a terminology 
    and the value is a list of rules for that terminology within this value set version.
    """
    conn = get_db()
    
    rules_data = conn.execute(text(
      """
      select * 
      from value_sets.value_set_rule 
      join terminology_versions
      on terminology_version=terminology_versions.uuid
      where value_set_version=:vs_version
      """
    ), {
      'vs_version': self.uuid
    })
    
    for x in rules_data:
      terminology = x.terminology
      rule = None
      
      if terminology == "ICD-10 CM":
        rule = ICD10CMRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, x.version, terminology_version_uuid=x.terminology_version)
      elif terminology == "SNOMED CT":
        rule = SNOMEDRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, x.version, terminology_version_uuid=x.terminology_version)
      elif terminology == "RxNorm":
        rule = RxNormRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self, x.fhir_uri, x.version, terminology_version_uuid=x.terminology_version)
        
      if terminology in self.rules:
        self.rules[terminology].append(rule)
      else:
        self.rules[terminology] = [rule]

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
    query = conn.execute(text(
      """
      select * from value_sets.expansion_member
      where expansion_uuid in
      (select uuid from value_sets.expansion
      where vs_version_uuid=:version_uuid
      order by timestamp desc
      limit 1)
      """
    ), {
      'version_uuid': self.uuid
    })

    for x in query:
      self.expansion.add(
        Code(
          x.system, x.version, x.code, x.display
        )
      )

  def save_expansion(self):
    conn = get_db()
    expansion_uuid = uuid1()

    # Create a new expansion entry in the value_sets.expansion table
    conn.execute(text(
      """
      insert into value_sets.expansion
      (uuid, vs_version_uuid, timestamp)
      values
      (:expansion_uuid, :version_uuid, now())
      """
    ), {
      'expansion_uuid': expansion_uuid,
      'version_uuid': self.uuid
    })

    # Save contents of self.expansion to new expansion
    for code in self.expansion:
      conn.execute(text(
        """
        insert into value_sets.expansion_member
        (expansion_uuid, code, display, system, version)
        values
        (:expansion_uuid, :code, :display, :system, :version)
        """
      ), {
        'expansion_uuid': expansion_uuid,
        'code': code.code,
        'display': code.display,
        'system': code.system,
        'version': code.version
      })

  def create_expansion(self):
    self.expansion = set()
    terminologies = self.rules.keys()
    
    for terminology in terminologies:
      rules = self.rules.get(terminology)
      
      for rule in rules: rule.execute()
        
      include_rules = [x for x in rules if x.include is True]
      exclude_rules = [x for x in rules if x.include is False]

      # print("")
      # print("INCLUSION RULES")
      # print("")
      # for x in include_rules:
      #   print(x.description, x.property, x.operator, x.value)
      #   print([y.__hash__() for y in x.results])
      
      terminology_set = include_rules.pop(0).results
      # todo: if it's a grouping value set, we should use union instead of intersection
      for x in include_rules: terminology_set = terminology_set.intersection(x.results)

      for x in exclude_rules: 
        remove_set = terminology_set.intersection(x.results)
        terminology_set = terminology_set - remove_set
        
      self.expansion = self.expansion.union(terminology_set)

    if self.value_set.type == 'intensional':
      self.save_expansion()
    
    return self.expansion

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
      include_rules = self.include_rules
      terminology_keys = include_rules.keys()
      serialized = []
      
      for key in terminology_keys:
        rules = include_rules.get(key)
        serialized_rules = [x.serialize() for x in rules]
        serialized.append({
          'system': key,
          'filter': serialized_rules
        })

      return serialized

  def serialize(self):
    serialized = {
      # "url": self.value_set.url,
      "identifier": self.value_set.uuid,
      "name": self.value_set.name,
      "title": self.value_set.title,
      "publisher": self.value_set.publisher,
      "contact": self.value_set.contact,
      "description": self.value_set.description + ' ' + self.description,
      "immutable": self.value_set.immutable,
      "experimental": self.value_set.experimental,
      "purpose": self.value_set.purpose,
      "effective_start": self.effective_start,
      "effective_end": self.effective_end,
      "version": self.version,
      "version_uuid": self.uuid,
      "status": self.status,
      "expansion": {
        "contains": [x.serialize() for x in self.expansion]
      },
      "compose": {
        "include": self.serialize_include(),
        "exclude": None
      }
    }

    if self.value_set.type == 'extensional': serialized.pop('expansion')

    return serialized
