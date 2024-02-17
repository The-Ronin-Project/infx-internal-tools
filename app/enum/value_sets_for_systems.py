from enum import Enum


class ValueSetsForSystems(Enum):
    """
    UUID values for value sets for use by INFX Systems.
    Use these to generate dynamic lists of value set version UUIDs, value set UUIDs, flexible registry UUIDs, etc.

    There are 8 value_sets.value_set rows safe to use in tests.
    ```
    uuid                                    title                                           description
    "ca75b03c-1763-44fd-9bfa-4fe015ff809c"	"Test ONLY: Mirth Validation Test Observations" "For automated test in Mirth"
    "c7c37780-e727-42f6-9d1b-d823d75171ad"	"Test ONLY: Test August 29"                     "no map + diabetes"
    "50ead103-a8c9-4aae-b5f0-f1e51b264323"	"Test ONLY: Test Condition Incremental Load"... "testing source terminology for condition incremental load"
    "236b88af-40c2-4d59-b319-a5e68865afdc"	"Test ONLY: test fhir and vs description"       "the value set description goes here"
    "ccba9765-66ee-4742-a656-4e37d0811958"	"Test ONLY: Test Observation Incremental"...	"Observations for Incremental Load testing"
    "fc82ec39-7b9f-4d74-9a34-adf86db1a50f"	"Test ONLY: Automated Testing Value Set"	    "For automated testing in infx-internal-tools"
    "b5f97703-abf3-4fc0-aa49-f8851a3fced4"	"Test ONLY: Test ValueSet for diffs"            "This valueset will have a small number of codes for diff check"
    "477195c0-8a91-11ec-ac15-073d0cb083df"	"Test ONLY: Testing Value Set test. Yay"        "Various codes and code systems test"
    "e49af176-189f-4536-8231-e58a261ed36d"	"Test ONLY: Concept Map Versioning Target"...   made up target concepts and inactive codes" (has no ValueSetVersion)
    ```
    """
