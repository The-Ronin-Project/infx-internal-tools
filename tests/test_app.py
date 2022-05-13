import re
import json
import pytest
import hashlib
from app.app import create_app 
app = create_app()
def test_extensional_vs():
    app.config['MOCK_DB'] = True
    response = app.test_client().get('/ValueSet/987ffe8a-27a8-11ec-9621-0242ac130002/$expand')
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
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    additional_data = response.json.get('additionalData')
    assert 'effective_start' in additional_data
    assert 'effective_end' in additional_data
    assert 'expansion_uuid' in additional_data
    assert 'version_uuid' in additional_data

    compose = response.json.get('compose')
    include = compose.get('include')[0]

    assert include.get('system') == 'http://snomed.info/sct'
    assert len(include.get('concept')) == 2

def test_intensional_vs_rxnorm():
    app.config['MOCK_DB'] = True
    # Load RxNorm value set
    response = app.test_client().get('/ValueSet/64c5d2c2-2857-11ec-9621-0242ac130002/$expand')
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
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    compose = response.json.get('compose')
    include = compose.get('include')[0]

    # Validate there are not any exclusion rules
    assert 'exclude' not in compose

    assert include.get('system') == 'http://www.nlm.nih.gov/research/umls/rxnorm'
    assert 'expansion' in response.json
    assert 'contains' in response.json.get('expansion')

def test_loinc_valueset():
    response = app.test_client().get('/ValueSet/c5ac2d30-83b4-11ec-9a73-9942f9fcf805/$expand?force_new=true')
    print(response.json)
    assert 'name' in response.json
    assert 'expansion' in response.json
    assert len(response.json.get('expansion').get('contains')) == 10

def test_intensional_vs_icd_snomed():
    app.config['MOCK_DB'] = True
    # Load breast-cancer value set
    response = app.test_client().get('/ValueSet/c447c800-6343-11ec-9b51-4fc98501ea85/$expand')
    print(response)
    print(response.json)
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
    assert 'contact' in response.json
    assert 'description' in response.json
    assert 'compose' in response.json

    # Validate the synonyms are loaded
    additional_data = response.json.get('additionalData')
    synonyms = additional_data.get('synonyms')
    assert synonyms.get("TEST") == "Breast Cancer"

    # Validate that there are exclusion rules
    compose = response.json.get('compose')
    assert 'exclude' in compose
    exclude = compose.get('exclude')
    assert len(exclude[0].get('filter')) > 3

    assert 'expansion' in response.json
    assert 'contains' in response.json.get('expansion')

def test_expansion_report():
    app.config['MOCK_DB'] = True
    response = app.test_client().get('/ValueSets/expansions/3257aed4-6da1-11ec-bd74-aa665a30495f/report')
    # print('hex digest', hashlib.md5(response.data).hexdigest())
    assert hashlib.md5(response.data).hexdigest() == "ca5613af2d0a65e32d7505849fd1c1d2"

def test_survey_export():
    app.config['MOCK_DB'] = True
    response = app.test_client().get('/surveys/34775510-1267-11ec-b9a3-77c9d91ff3f2?organization_uuid=866632f0-ff85-11eb-9f47-ffa6d132f8a4')
    print(hashlib.md5(response.data).hexdigest())
    assert hashlib.md5(response.data).hexdigest() == "d8d184f61545f542f2a42c7064a90148"

def test_execute_rules_directly():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [{
            "property": "component",
            "operator": "in",
            "value": "{\"Alpha-1-Fetoprotein\"}",
            "include": True,
            "terminology_version": "7c19e704-19d9-412b-90c3-79c5fb99ebe8"
            }]
        ),
        content_type='application/json'
        )
    assert len(response.json) == 6

def test_icd_10_cm_in_section():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
                "property": "code",
                "operator": "in-section",
                "value": "d66586d4-5ed0-11ec-8f1f-00163e90ea35",
                "include": True,
                "terminology_version": "1ea19640-63e6-4e1b-b82f-be444ba395b4"
            }
        ]
        ),
        content_type='application/json'
        )
    assert len(response.json) == 32

def test_icd_10_cm_in_chapter():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
                "property": "code",
                "operator": "in-chapter",
                "value": "3f830074-5ed1-11ec-8f1f-00163e90ea35",
                "include": True,
                "terminology_version": "1ea19640-63e6-4e1b-b82f-be444ba395b4"
            }
        ]
        ),
        content_type='application/json'
        )
    assert len(response.json) == 931

def test_icd_10_pcs_has_body_system():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-body-system",
                "value": [" Eye "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    assert len(response.json) == 1290

def test_icd_10_pcs_has_root_operation():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-root-operation",
                "value": [" Magnetic Resonance Imaging (MRI) "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    print(response.json)  
    assert len(response.json) == 421

def test_icd_10_pcs_has_device():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-device",
                "value": [" Unenhanced and Enhanced "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    assert len(response.json) ==  314

def test_icd_10_pcs_has_body_part():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-body-part",
                "value": [" Spinal Canal "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    assert len (response.json) == 152

def test_icd_10_pcs_has_approach():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-approach",
                "value": [" High Osmolar "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    assert len(response.json) == 581
    
def test_icd_10_pcs_has_qualifier():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
               "property": "code",
                "operator": "has-qualifier",
                "value": [" Atrium"],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a" 
            }
        ]    
        ),
        content_type='application/json'
        )
    assert len(response.json) == 18

def test_icd_10_pcs_multi_rule():
    app.config['MOCK_DB'] = True
    response = app.test_client().post(
        '/ValueSets/rule_set/execute',
        data = json.dumps(
        [
            {
                "property": "code",
                 "operator": "in-section",
                 "value": ["Medical and Surgical "],
                 "include": True,
                 "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
            },
            {
                "property": "code",
                "operator": "has-body-system",
                "value": [" Central Nervous System and Cranial Nerves "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
            },
            {
                "property": "code",
                "operator": "has-root-operation",
                "value": [" Bypass "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
             },
             {
                "property": "code",
                "operator": "has-body-part",
                "value": [" Spinal Canal "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
            },
            {
                "property": "code",
                "operator": "has-approach",
                "value": [" Open "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
             },
             {
                "property": "code",
                "operator": "has-device",
                "value": [" Autologous Tissue Substitute "],
                "include": True,
                "terminology_version": "60f15a17-973e-4987-ad71-22777eac994a"
             }
        ]
        ),
        content_type='application/json'
        )
    assert len(response.json) == 5

def test_create_new_version_value_set():
    """ This test will create a new version of a value set and then delete it """
    app.config['MOCK_DB'] = True

    metadata = app.test_client().get(
        '/ValueSets/bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb/versions/'
    )
    num_versions = len(metadata.json)

    response = app.test_client().post(
        '/ValueSets/bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb/versions/new',
        data = json.dumps({
            'effective_start': '2022-01-01',
            'effective_end': '2022-12-31',
            'description': 'test version'
        }),
        content_type='application/json'
    )
    new_version_uuid = response.text
    
    metadata = app.test_client().get(
        '/ValueSets/bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb/versions/'
    )
    new_num_versions = len(metadata.json)

    assert num_versions + 1 == new_num_versions

    # Now delete the new version
    app.test_client().delete(
        f'/ValueSets/bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb/versions/{new_version_uuid}'
    )

    metadata = app.test_client().get(
        '/ValueSets/bfcb8eb0-6343-11ec-bd13-cbbf4db9fbeb/versions/'
    )
    new_num_versions = len(metadata.json)

    assert num_versions == new_num_versions

def test_concept_map_load():
    app.config['MOCK_DB'] = True
    concept_map = app.test_client().get(
        '/ConceptMaps/cbe12636-102f-4ab0-9616-a8684c9f2a21'
    )
    assert len(concept_map.json.get('group')[0].get('element')) == 313
