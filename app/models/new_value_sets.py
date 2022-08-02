from dataclasses import dataclass, field
from uuid import UUID
from app.models.terminologies import Terminology

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
        self.synonyms=synonyms
        self.use_case = use_case

        self.rule_groups = []
        self.expansion = set()
        self.expansion_timestamp = None
        self.extensional_codes = []

    @classmethod
    def create():
        pass

    # Do we need a different method for loading a historical version vs. current working version
    @classmethod
    def load():
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
class RuleGroup:
    pass

class Rule:
    pass

@dataclass
class RuleVariant:
    uuid: UUID
    rule: Rule
    description: str
    property: str
    operator: str
    value: str

    def serialize(self):
        return {
            'uuid': self.uuid,
            'description': self.description,
            'property': self.property,
            'operator': self.operator,
            'value': self.value
        }


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
