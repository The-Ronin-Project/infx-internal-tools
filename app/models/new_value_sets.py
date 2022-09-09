from dataclasses import dataclass, field
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, bindparam
from uuid import UUID
from app.models.terminologies import Terminology
from app.models.codes import Code
from app.database import get_db


@dataclass
class UseCase:
    uuid: UUID
    name: str
    description: str
    status: str
    point_of_contact: str = field(default=None)
    point_of_contact_email: str = field(default=None)
    jira_ticket: str = field(default=None)

    def serialize(self):
        return {
            'uuid': self.uuid,
            'name': self.name,
            'description': self.description,
            'status': self.status,
            'point_of_contact': self.point_of_contact,
            'point_of_contact_email': self.point_of_contact_email,
            'jira_ticket': self.jira_ticket
        }


class ValueSet:
    def __init__(
            self, uuid, name, title, publisher, contact, description, immutable, experimental, purpose, vs_type,
            synonyms={}, use_case=None, rule_groups=[], extensional_codes=[]
    ) -> None:

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
        self.synonyms = synonyms
        self.use_case = use_case

        self.rule_groups = []
        self.expansion = set()
        self.expansion_timestamp = None
        self.extensional_codes = []

    @classmethod
    def create(cls):
        pass

    # Do we need a different method for loading a historical version vs. current working version
    @classmethod
    def load(cls):
        pass

    def duplicate(self):
        """ Copy the current working version to create a new version """

    def delete(self):
        pass

    def serialize(self):
        pass

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
            "use_case": self.use_case.serialize()
        }


# re-implement rule group to load from appropriate table(s) rather than use a rule_group column in value_set_version
class Rule:
    uuid: UUID
    description: str
    include: bool

    def serialize(self):
        return {
            "uuid": self.uuid,
            "description": self.description,
            "include": self.include
        }


@dataclass
class RuleVariant:
    uuid: UUID
    rule: Rule
    description: str
    property: str
    operator: str
    value: str

    def execute(self):
        pass # should be an abstract method

    def serialize(self):
        return {
            'uuid': self.uuid,
            'description': self.description,
            'property': self.property,
            'operator': self.operator,
            'value': self.value
        }


class ICD10CMRuleVariant(RuleVariant):
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

class RuleGroup:
    uuid: UUID
    rule: Rule
    description: str
    include: bool

    def serialize(self):
        return{
            "uuid": self.uuid,
            "rule": self.rule,
            "description": self.description,
            "include": self.include
        }


def execute_rules(rules_input):
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
          #NewVS doesn't have terminology_version, access through variant_terminology_link table via variant uuid
          "variant_uuid": ""
        }
      ]
      """
    conn = get_db()

    # Lookup terminology names
    terminology_versions_query = conn.execute(text(
        """
        select terminology_version, ptv.* from value_sets_new.variant_terminology_link vtl
        join public.termilnology_versions ptv
        on vtl.terminology_version_uuid = ptv.uuid
        where varient_uuid = :variant_uuid
        """
    ))
    terminology_versions = [x for x in terminology_versions_query]

    uuid_to_name_map = {str(x.uuid): x for x in terminology_versions}

    rules = []
    for rule in rules_input:
        terminology_name = uuid_to_name_map.get(rule.get('terminology_version')).terminology
        fhir_uri = uuid_to_name_map.get(rule.get('terminology_version')).fhir_uri
        terminology_version = uuid_to_name_map.get(rule.get('terminology_version'))
        rule_property = rule.get('property')
        operator = rule.get('operator')
        value = rule.get('value')
        include = rule.get('include')

        if terminology_name == "ICD-10 CM":
            rule = ICD10CMRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                               terminology_version)
        elif terminology_name == "SNOMED CT":
            rule = SNOMEDRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                              terminology_version)
        elif terminology_name == "RxNorm":
            rule = RxNormRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                              terminology_version)
        elif terminology_name == "LOINC":
            rule = LOINCRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                             terminology_version)
        elif terminology_name == "CPT":
            rule = CPTRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                           terminology_version)
        elif terminology_name == "ICD-10 PCS":
            rule = ICD10PCSRule(None, None, None, rule_property, operator, value, include, None, fhir_uri,
                                terminology_version)
        elif terminology.fhir_terminology == True:
            rule = FHIRRule(x.uuid, x.position, x.description, x.property, x.operator, x.value, x.include, self,
                            x.fhir_uri, terminologies.get(x.terminology_version))

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
    # return "Execute Rules"


class ExtensionalCode:
    pass
    """ Wraps a Code with additional metadata needed to include in a value set (ex. review status) """


class Expansion:
    pass


class RuleGroupExpansion(Expansion):
    pass


class ValueSetExpansion(Expansion):
    pass

# Terminology Version
