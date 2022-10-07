import json
from dataclasses import dataclass

from decouple import config
from sqlalchemy import text
import uuid
from typing import List, cast
import app.models.concept_maps
from app.database import get_db
from app.helpers.oci_auth import oci_authentication


@dataclass
class DNRegistryEntry:
    uuid: uuid.UUID
    resource_type: str
    data_element: str
    tenant_id: str
    original_data_element: str
    concept_map: app.models.concept_maps.ConceptMap

    def serialize(self):
        return {
            'uuid': str(self.uuid),
            'resource_type': self.resource_type,
            'data_element': self.data_element,
            'tenant_id': self.tenant_id,
            'original_data_element': self.original_data_element,
            'concept_map_uuid': str(self.concept_map.uuid),
            'version': self.concept_map.most_recent_active_version.version,
            'filename': f'/ConceptMaps/v1/{self.concept_map.uuid}/{self.concept_map.most_recent_active_version.version}.json'
        }


@dataclass
class DataNormalizationRegistry:
    entries: List[DNRegistryEntry] = None

    def __post_init__(self):
        if self.entries is None:
            self.entries = []

    def load_entries(self):
        conn = get_db()
        query = conn.execute(
            text(
                """
                select * from data_ingestion.registry
                """
            )
        )

        for item in query:
            self.entries.append(
                DNRegistryEntry(
                    uuid=item.uuid,
                    resource_type=item.resource_type,
                    data_element=item.data_element,
                    tenant_id=item.tenant_id,
                    original_data_element=item.original_data_element,
                    concept_map=app.models.concept_maps.ConceptMap(item.concept_map_uuid)
                )
            )

    def serialize(self):
        return [x.serialize() for x in self.entries]

    @staticmethod
    def publish_to_object_store(registry):
        object_storage_client = oci_authentication()
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data
        object_storage_client.put_object(
            namespace,
            bucket_name,
            'DataNormalizationRegistry/v1/registry-draft.json',
            json.dumps(registry, indent=2).encode("utf-8"),
        )
        return registry


