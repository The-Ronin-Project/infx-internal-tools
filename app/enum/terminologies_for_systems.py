from enum import Enum


class TerminologiesForSystems(Enum):
    """
    UUID values for terminologies for use by INFX Systems.
    Use these to generate dynamic lists of value set version UUIDs, value set UUIDs, flexible registry UUIDs, etc.

    There are 5 public.terminology_versions rows safe to use in tests that also pass checks to allow codes to be created
    - for queries to discover some additional terminology rows, use steps in tests/terminologies/test_terminologies.py
    ```
    terminology                    version     uri                 is_standard is_fhir effective_start  _end
    "Test ONLY: fake/fhir_uri"         "3"    "fake/fhir_uri"            false false   "2023-04-12"
    "Test ONLY: http://test_test.com"  "1"    "http://test_test.com"     false false                    "2023-09-01"
    "Test ONLY: Duplicate Insert Test" "1"    ..."/duplicateInsertTest"  false false   "2023-11-09"     "2030-11-16"
    "Test ONLY: Mock FHIR Terminology" "1.0.0" ..."/ronin/mock_fhir"     false true	   "2023-04-05"
    "Test ONLY: Mock Standard Term"... "1.0.0" ..."/ronin/mock_standard" true  false   "2023-02-26"
    ```
    ```
    terminology                        uuid                                   variable            purpose
    "Test ONLY: fake/fhir_uri"         "d2ae0de5-0168-4f54-924a-1f79cf658939" safe_term_uuid_fake has no expiry date
    "Test ONLY: http://test_test.com"  "3c9ed300-0cb8-47af-8c04-a06352a14b8d" safe_term_uuid_test expiry date has passed
    "Test ONLY: Duplicate Insert Test" "d14cbd3a-aabe-4b26-b754-5ae2fbd20949" safe_term_uuid_dupl has future expiry date
    "Test ONLY: Mock FHIR Terminology" "34eb844c-ffff-4462-ad6d-48af68f1e8a1" safe_term_uuid_fhir fhir_terminology true
    "Test ONLY: Mock Standard Term"... "c96200d7-9e30-4a0c-b98e-22d0ff146a99" safe_term_uuid_std  is_standard true
    ```
    """
