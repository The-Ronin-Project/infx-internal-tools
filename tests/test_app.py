import re
import pytest
import hashlib
from app import app 

def test_extensional_vs():
    app.app.config['MOCK_DB'] = True
    response = app.app.test_client().get('/ValueSet/987ffe8a-27a8-11ec-9621-0242ac130002/$expand')
    # assert response.json.get('version') == "1.0"
    # assert 'url' in response.json
    assert response.json.get('title') == "Extensional Value Set Test"
    assert response.json.get('status') == "draft"
    assert 'purpose' in response.json
    assert 'publisher' in response.json
    assert 'name' in response.json
    assert response.json.get('immutable') == False
    assert 'id' in response.json
    assert response.json.get('experimental') == True
    assert 'effective_start' in response.json
    assert 'effective_end' in response.json
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    compose = response.json.get('compose')
    include = compose.get('include')[0]

    assert include.get('system') == 'http://snomed.info/sct'
    assert len(include.get('concept')) == 2

def test_intensional_vs_rxnorm():
    app.app.config['MOCK_DB'] = True
    response = app.app.test_client().get('/ValueSet/64c5d2c2-2857-11ec-9621-0242ac130002/$expand')
    assert 'version' in response.json
    # assert 'url' in response.json
    assert 'title' in response.json
    assert 'status' in response.json
    assert 'purpose' in response.json
    assert 'publisher' in response.json
    assert 'name' in response.json
    assert 'immutable' in response.json
    assert 'id' in response.json
    assert 'experimental' in response.json
    assert 'effective_start' in response.json
    assert 'effective_end' in response.json
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    compose = response.json.get('compose')
    include = compose.get('include')[0]

    assert include.get('system') == 'http://www.nlm.nih.gov/research/umls/rxnorm'
    assert 'expansion' in response.json
    assert 'contains' in response.json.get('expansion')

def test_intensional_vs_icd_snomed():
    app.app.config['MOCK_DB'] = True
    response = app.app.test_client().get('/ValueSet/f38a3352-214c-11ec-9621-0242ac130002/$expand?force_new=true')
    assert 'version' in response.json
    # assert 'url' in response.json
    assert 'title' in response.json
    assert 'status' in response.json
    assert 'purpose' in response.json
    assert 'publisher' in response.json
    assert 'name' in response.json
    assert 'immutable' in response.json
    assert 'id' in response.json
    assert 'experimental' in response.json
    assert 'effective_start' in response.json
    assert 'effective_end' in response.json
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    assert 'expansion' in response.json
    assert 'contains' in response.json.get('expansion')

def test_expansion_report():
    app.app.config['MOCK_DB'] = True
    response = app.app.test_client().get('/ValueSets/expansions/3257aed4-6da1-11ec-bd74-aa665a30495f/report')
    print('hex digest', hashlib.md5(response.data).hexdigest())
    assert hashlib.md5(response.data).hexdigest() == "ca5613af2d0a65e32d7505849fd1c1d2"

def test_survey_export():
    app.app.config['MOCK_DB'] = True
    response = app.app.test_client().get('/surveys/34775510-1267-11ec-b9a3-77c9d91ff3f2?organization_uuid=866632f0-ff85-11eb-9f47-ffa6d132f8a4')
    # print(hashlib.md5(response.data).hexdigest())
    assert hashlib.md5(response.data).hexdigest() == "d1addd7f781c6e0d732e75cd83b1c76b"
