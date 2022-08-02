import re
import json

import hashlib
import uuid
from app.app import create_app 
from app.models.new_value_sets import ValueSet, UseCase, RuleVariant, Rule

app = create_app()

def create_mock_use_case():
    return UseCase(
        uuid = uuid.UUID("1d3c8917-87db-4ceb-b915-04f6b5ff8423"),
        name="Test Use Case",
        description="test use case",
        status="active",
        point_of_contact="Theresa Aguilar",
    )

def test_serialize_rule_variant():
    rule_variant = RuleVariant(
        uuid=uuid.UUID("26124e93-2b4a-42f4-8043-7b7c5b624c1a"),
        rule = create_mock_rule(),
        description = "test rule variant",
        property="code",
        operator="=",
        value="C50"
    )
    serialized = rule_variant.serialize()

    assert serialized.get('uuid') == uuid.UUID('26124e93-2b4a-42f4-8043-7b7c5b624c1a')
    assert serialized.get('description') == "test rule variant"
    assert serialized.get('property') == "code"
    assert serialized.get('operator') == "="
    assert serialized.get('value') == "C50"

def create_mock_rule():
    return Rule()

# def test_serialize_rule():
#     rule = Rule()

def test_serialize_value_set():
    use_case = create_mock_use_case()

    test_vs = ValueSet(
        uuid = uuid.UUID("6d3f4761-6891-4971-8d82-41b068065a6b"),
        name = 'Test Value Set',
        title = 'Test Value Set',
        publisher = 'Project Ronin',
        contact = 'Theresa Aguilar',
        description = 'for testing',
        immutable = False,
        experimental = True,
        purpose = 'testing',
        vs_type = 'intensional',
        synonyms=None,
        use_case = use_case,
    )

    serialized = test_vs.serialize()
    print(serialized)

    assert serialized.get('name') == "Test Value Set"
    assert serialized.get('publisher') == "Project Ronin"
    assert serialized.get('experimental') == True
    assert serialized.get('purpose') == 'testing'

