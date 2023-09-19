import datetime
import json
from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil import tz
from decouple import config
from sqlalchemy import text

import app.concept_maps.models
import app.value_sets.models
from app.database import get_db
from app.helpers.oci_helper import oci_authentication


@dataclass
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
    # Full path avoids circular import of ConceptMap and ValueSet
    concept_map: "app.concept_maps.models.ConceptMap" = None
    value_set: "app.value_sets.models.ValueSet" = None

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
            "profile_url": self.profile_url,
        }
        if self.registry_entry_type == "value_set":
            value_set_version = self.value_set.load_most_recent_active_version(
                self.value_set.uuid
            ).version
            serialized["value_set_name"] = self.value_set.name
            serialized["value_set_uuid"] = str(self.value_set.uuid)
            serialized["version"] = value_set_version
            serialized[
                "filename"
            ] = f"{app.value_sets.models.ValueSet.object_storage_folder_name}/v{app.value_sets.models.ValueSet.database_schema_version}/published/{self.value_set.uuid}/{value_set_version}.json"
        if self.registry_entry_type == "concept_map":
            concept_map_version = self.concept_map.most_recent_active_version.version
            serialized["concept_map_name"] = self.concept_map.name
            serialized["concept_map_uuid"] = str(self.concept_map.uuid)
            serialized["version"] = concept_map_version
            serialized[
                "filename"
            ] = f"{app.concept_maps.models.ConceptMap.object_storage_folder_name}/v{app.concept_maps.models.ConceptMap.database_schema_version}/published/{self.concept_map.uuid}/{concept_map_version}.json"
        return serialized


@dataclass
class DataNormalizationRegistry:
    """
    A class representing the Data Normalization Registry containing multiple DNRegistryEntry objects.
    """
    entries: List[DNRegistryEntry] = None
    database_schema_version = 3
    object_storage_folder_name = "DataNormalizationRegistry"
    object_storage_file_name = "registry.json"
    object_storage_diff_name = "registry_diff.json"

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
            if item.type == "concept_map":
                self.entries.append(
                    DNRegistryEntry(
                        resource_type=item.resource_type,
                        data_element=item.data_element,
                        tenant_id=item.tenant_id,
                        source_extension_url=item.source_extension_url,
                        registry_uuid=item.registry_uuid,
                        profile_url=item.profile_url,
                        registry_entry_type=item.type,
                        concept_map=app.concept_maps.models.ConceptMap(
                            item.concept_map_uuid,
                            load_mappings_for_most_recent_active=False,
                        ),
                    )
                )
            elif item.type == "value_set":
                self.entries.append(
                    DNRegistryEntry(
                        resource_type=item.resource_type,
                        data_element=item.data_element,
                        tenant_id=item.tenant_id,
                        source_extension_url=item.source_extension_url,
                        registry_uuid=item.registry_uuid,
                        profile_url=item.profile_url,
                        registry_entry_type=item.type,
                        value_set=app.value_sets.models.ValueSet.load(
                            item.value_set_uuid
                        ),
                    )
                )
            else:
                raise Exception(
                    "Only value_set and concept_map are recognized registry types"
                )

    def serialize(self):
        """
        Serialize the DataNormalizationRegistry object into a list of dictionaries.
        """
        return [x.serialize() for x in self.entries]

    @staticmethod
    def publish_to_object_store(registry, filename):
        """
        Publish the Data Normalization Registry to the Object Storage.
        @param registry: data to publish
        @param filename: Caller sets the file name only. Application controls the path.
        """
        object_storage_client = oci_authentication()
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data
        object_storage_client.put_object(
            namespace,
            bucket_name,
            f"{DataNormalizationRegistry.object_storage_folder_name}/v{DataNormalizationRegistry.database_schema_version}/{filename}",
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
            namespace,
            bucket_name,
            f"{DataNormalizationRegistry.object_storage_folder_name}/v{DataNormalizationRegistry.database_schema_version}/{DataNormalizationRegistry.object_storage_file_name}",
        )
        return bucket_item.headers["last-modified"]

    @staticmethod
    def get_last_published_registry():
        object_storage_client = oci_authentication()
        namespace = object_storage_client.get_namespace().data
        bucket_name = config("OCI_CLI_BUCKET")
        bucket_item = object_storage_client.get_object(
            namespace,
            bucket_name,
            f"{DataNormalizationRegistry.object_storage_folder_name}/v{DataNormalizationRegistry.database_schema_version}/{DataNormalizationRegistry.object_storage_file_name}",
        )
        return bucket_item.data.json()

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

    @staticmethod
    def publish_data_normalization_registry():
        """Publish the data normalization registry and the diff from previous version"""
        previous_version = DataNormalizationRegistry.get_last_published_registry()

        current_registry = DataNormalizationRegistry()
        current_registry.load_entries()
        current_registry_serialized = current_registry.serialize()
        newly_published_version = DataNormalizationRegistry.publish_to_object_store(
            current_registry_serialized, DataNormalizationRegistry.object_storage_file_name
        )

        diff_version = get_incremented_versions_and_update(
            previous_version, newly_published_version
        )
        DataNormalizationRegistry.publish_to_object_store(
            diff_version, DataNormalizationRegistry.object_storage_diff_name
        )

        return newly_published_version


def get_incremented_versions_and_update(old_data, new_data):
    """
    Identify entries in the new_data list where the version has been incremented compared to old_data.
    Adds an 'old_version' key to these entries in the new_data list.

    Args:
        old_data (list): The list of dictionaries representing the old data.
        new_data (list): The list of dictionaries representing the new data.

    Returns:
        list: A list of dictionaries from new_data that have incremented versions.
    """

    # Convert each list to a dictionary with a tuple key for easy comparison
    old_dict = {
        (
            entry["data_element"],
            entry["tenant_id"],
            entry["source_extension_url"],
            entry["registry_entry_type"],
            entry["profile_url"],
        ): entry["version"]
        for entry in old_data
    }

    incremented_entries = []

    for entry in new_data:
        key = (
            entry["data_element"],
            entry["tenant_id"],
            entry["source_extension_url"],
            entry["registry_entry_type"],
            entry["profile_url"],
        )
        if key in old_dict and entry["version"] > old_dict[key]:
            # Save the old version
            entry["old_version"] = old_dict[key]
            incremented_entries.append(entry)

    return incremented_entries
