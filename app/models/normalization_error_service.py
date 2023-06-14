import requests
from typing import Dict, Tuple, List
from enum import Enum

from app.models.models import Organization
from app.models.codes import Code
from app.models.data_ingestion_registry import DataNormalizationRegistry
from app.concept_maps.models import ConceptMapVersion


class ResourceType(Enum):
    OBSERVATION = "Observation"
    CONDITION = "Condition"
    MEDICATION = "Medication"
    TELECOM_USE = "Practitioner.telecom.use"  # Only in for testing until we have a real data type live


def load_concepts_from_errors() -> Dict[Tuple[Organization, ResourceType], List[Code]]:
    """
   Loads and processes a list of errors to extract specific concepts from them.

   This function parses each error and identifies the relevant concepts. These concepts are
   then grouped by the originating organization and the type of resource they belong to. The
   results are returned as a dictionary, where each key is a tuple of an organization and a
   resource type, and each value is a list of concepts associated with that key.

   Returns:
       Dict[Tuple[Organization, ResourceType], List[Concept]]: A dictionary mapping tuples of
       organization and resource type to lists of concepts extracted from the errors.
   """
    pass


def lookup_concept_map_version_for_resource_type(resource_type: ResourceType,
                                                 organization: Organization) -> 'ConceptMapVersion':
    """
    Returns the specific ConceptMapVersion currently in use for normalizing data with the specified resource_type and organization
    :param resource_type:
    :param organization:
    :return:
    """
    # Load the data normalization registry
    registry = DataNormalizationRegistry()
    registry.load_entries()

    # Filter based on resource type
    filtered_registry = [
        registry_entry for registry_entry in registry.entries if registry_entry.data_element == resource_type.value
    ]

    # First, we will check to see if there's an organization-specific entry to use
    # If not, we will fall back to checking for a tenant-agnostic entry
    concept_map_version = None

    organization_specific = [
        registry_entry for registry_entry in filtered_registry if registry_entry.tenant_id == organization.id
    ]
    if len(organization_specific) > 0:
        concept_map_version = organization_specific[0].concept_map.most_recent_active_version

    # Falling back to check for tenant agnostic entry
    tenant_agnostic = [registry_entry for registry_entry in filtered_registry if registry_entry.tenant_id is None]
    if len(tenant_agnostic) > 0:
        concept_map_version = tenant_agnostic[0].concept_map.most_recent_active_version

    # If nothing is found, raise an appropriate error
    if concept_map_version is None:
        raise Exception("No appropriate registry entry found")

    return concept_map_version
