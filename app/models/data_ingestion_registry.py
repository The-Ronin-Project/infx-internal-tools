import json
import datetime
import uuid
import app.models.concept_maps
import app.models.value_sets

from dataclasses import dataclass
from decouple import config
from sqlalchemy import text
from typing import List, cast, Optional
from app.database import get_db
from app.helpers.oci_helper import oci_authentication
from datetime import datetime
from dateutil import tz

DATA_NORMALIZATION_REGISTRY_SCHEMA_VERSION = 2

@dataclass
class DNRegistryEntry:
    class DNRegistryEntry:
        """
        A class representing a single entry in the Data Normalization Registry.
        """
    resource_type: str
    data_element: str
    tenant_id: str
    source_extension_url: str
    registry_uuid: str
    registry_entry_type: str
    profile_url: str
    concept_map: Optional[app.models.concept_maps.ConceptMap] = None
    value_set: Optional[app.models.value_sets.ValueSet] = None

    def serialize(self):
        """
        Serialize the DNRegistryEntry object into a dictionary.
        """
        serialized = {
            "registry_uuid": str(self.registry_uuid),
            "resource_type": self.resource_type,
            "data_element": self.data_element,
            "tenant_id": self.tenant_id,
            "source_extension_url": self.source_extension_url,
            "registry_entry_type": self.registry_entry_type,
            "profile_url": self.profile_url
        }
        if self.registry_entry_type == 'value_set':
            value_set_version = self.value_set.load_most_recent_active_version(self.value_set.uuid).version
            serialized['value_set_name'] = self.value_set.name
            serialized['value_set_uuid'] = str(self.value_set.uuid)
            serialized['version'] = value_set_version
            serialized['filename'] = f"ValueSets/v{app.models.value_sets.VALUE_SET_SCHEMA_VERSION}/published/{self.value_set.uuid}/{value_set_version}.json"
        if self.registry_entry_type == 'concept_map':
            serialized['concept_map_name'] = self.concept_map.name
            serialized['concept_map_uuid'] = str(self.concept_map.uuid)
            serialized['version'] = self.concept_map.most_recent_active_version.version
            serialized[
                'filename'] = f"ConceptMaps/v1/published/{self.concept_map.uuid}/{self.concept_map.most_recent_active_version.version}.json"
        return serialized



@dataclass
class DataNormalizationRegistry:
    """
    A class representing the Data Normalization Registry containing multiple DNRegistryEntry objects.
    """
    entries: List[DNRegistryEntry] = None

    def __post_init__(self):
        if self.entries is None:
            self.entries = []

    def load_entries(self):
        """
        Load all entries from the data_ingestion.registry in the database.
        """
        conn = get_db()
        query = conn.execute(
            text(
                """
                select * from data_ingestion.registry
                """
            )
        )

        for item in query:
            if item.type == 'concept_map':
                self.entries.append(
                    DNRegistryEntry(
                        resource_type=item.resource_type,
                        data_element=item.data_element,
                        tenant_id=item.tenant_id,
                        source_extension_url=item.source_extension_url,
                        registry_uuid=item.registry_uuid,
                        profile_url=item.profile_url,
                        registry_entry_type=item.type,
                        concept_map=app.models.concept_maps.ConceptMap(
                            item.concept_map_uuid
                        )
                    )
                )
            elif item.type == 'value_set':
                self.entries.append(
                    DNRegistryEntry(
                        resource_type=item.resource_type,
                        data_element=item.data_element,
                        tenant_id=item.tenant_id,
                        source_extension_url=item.source_extension_url,
                        registry_uuid=item.registry_uuid,
                        profile_url=item.profile_url,
                        registry_entry_type=item.type,
                        value_set=app.models.value_sets.ValueSet.load(
                            item.value_set_uuid
                        )
                    )
                )
            else:
                raise Exception("Only value_set and concept_map are recognized registry types")

    def serialize(self):
        """
        Serialize the DataNormalizationRegistry object into a list of dictionaries.
        """
        return [x.serialize() for x in self.entries]

    @staticmethod
    def publish_to_object_store(registry):
        """
        Publish the Data Normalization Registry to the Object Storage.
        """
        object_storage_client = oci_authentication()
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data
        object_storage_client.put_object(
            namespace,
            bucket_name,
            f"DataNormalizationRegistry/v{DATA_NORMALIZATION_REGISTRY_SCHEMA_VERSION}/registry.json",
            json.dumps(registry, indent=2).encode("utf-8"),
        )
        return registry

    @staticmethod
    def get_oci_last_published_time():
        """
        Retrieve the last modified timestamp of the Data Normalization Registry file in the Object Storage.
        Returns the timestamp as a string.
        """
        object_storage_client = oci_authentication()
        namespace = object_storage_client.get_namespace().data
        bucket_name = config("OCI_CLI_BUCKET")
        bucket_item = object_storage_client.get_object(
            namespace, bucket_name, f"DataNormalizationRegistry/v{DATA_NORMALIZATION_REGISTRY_SCHEMA_VERSION}/registry.json"
        )
        return bucket_item.headers["last-modified"]

    @staticmethod
    def convert_gmt_time(object_time):
        """
        function to convert last modified string timestamp from GMT to PST time
        @param object_time: string timestamp
        @type object_time: string
        @return: dictionary containing converted time
        @rtype: dictionary
        """
        # set timezones that are being worked with
        from_zone = tz.gettz("GMT")
        to_zone = tz.gettz("US/Pacific")
        # set format of gmt timestamp
        gmt = datetime.strptime(object_time, "%a, %d %b %Y %H:%M:%S GMT")
        # tell datetime object its GMT
        gmt = gmt.replace(tzinfo=from_zone)
        # convert time zone to PST
        pacific_time = str(gmt.astimezone(to_zone))
        return {"last_modified": pacific_time + " PST"}
