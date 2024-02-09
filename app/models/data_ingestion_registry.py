import datetime
import json
from dataclasses import dataclass
from datetime import datetime
from typing import List

from dateutil import tz
from decouple import config
from oci.exceptions import ServiceError
from sqlalchemy import text

import app.concept_maps.models
import app.value_sets.models
from app.database import get_db
from app.errors import BadRequestWithCode
from app.helpers.oci_helper import oci_authentication
from app.helpers.oci_helper import is_oci_write_disabled

import logging
LOGGER = logging.getLogger()


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

    def serialize(
        self,
        concept_map_schema_version: int = None,
        value_set_schema_version: int = None,
    ):
        """
        Serialize the DNRegistryEntry object into a dictionary. It is either a concept map or a value set.
        @param concept_map_schema_version: Format to use in serialization for concept_map entries. Caller may accept the
        default, or input a choice between the current ConceptMap.database_schema_version (such as 3) and
        ConceptMap.next_schema_version (such as 4). If None or not supplied, next_schema_version is used.
        @param value_set_schema_version: Similar but for value sets.
        @return: dictionary representing the DNRegistryEntry.
        @raise BadRequestWithCode if a concept_map_schema_version value is provided, but turns out to be invalid.
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

            # Determine the output schema version
            if value_set_schema_version is None:
                value_set_schema_version = (
                    app.value_sets.models.ValueSet.next_schema_version
                )
            else:
                if (
                    value_set_schema_version
                    != app.value_sets.models.ValueSet.next_schema_version
                    and (
                        value_set_schema_version
                        != app.value_sets.models.ValueSet.database_schema_version
                    )
                ):
                    raise BadRequestWithCode(
                        "DNRegistryEntry.serialize.BadValueSetSchemaVersion",
                        f"ConceptMap output format schema version {value_set_schema_version} is not supported",
                    )

            # Prepare the DNRegistryEntry
            value_set_version = self.value_set.load_most_recent_active_version(
                self.value_set.uuid
            ).version
            filepath = f"{app.value_sets.models.ValueSet.object_storage_folder_name}/v{value_set_schema_version}/published/{self.value_set.uuid}/{value_set_version}.json"
            serialized["value_set_name"] = self.value_set.name
            serialized["value_set_uuid"] = str(self.value_set.uuid)
            serialized["version"] = value_set_version
            serialized["filename"] = filepath
        if self.registry_entry_type == "concept_map":

            # Determine the output schema version
            if concept_map_schema_version is None:
                concept_map_schema_version = (
                    app.concept_maps.models.ConceptMap.next_schema_version
                )
            else:
                if (
                    concept_map_schema_version
                    != app.concept_maps.models.ConceptMap.next_schema_version
                    and (
                        concept_map_schema_version
                        != app.concept_maps.models.ConceptMap.database_schema_version
                    )
                ):
                    raise BadRequestWithCode(
                        "DNRegistryEntry.serialize.BadConceptMapSchemaVersion",
                        f"ConceptMap output format schema version {concept_map_schema_version} is not supported",
                    )

            # Prepare the DNRegistryEntry
            concept_map_version = self.concept_map.most_recent_active_version.version
            filepath = f"{app.concept_maps.models.ConceptMap.object_storage_folder_name}/v{concept_map_schema_version}/published/{self.concept_map.uuid}/{concept_map_version}.json"
            serialized["concept_map_name"] = self.concept_map.name
            serialized["concept_map_uuid"] = str(self.concept_map.uuid)
            serialized["version"] = concept_map_version
            serialized["filename"] = filepath
        return serialized


@dataclass
class DataNormalizationRegistry:
    """
    A class representing the Data Normalization Registry containing multiple DNRegistryEntry objects.

    Attributes:
        object_storage_folder_name (str): "DataNormalizationRegistry" folder name for OCI storage, for easy retrieval.
        database_schema_version (int): The current output schema version for DataNormalizationRegistry JSON files in OCI
        next_schema_version (int): The pending next output schema version number. When database_schema_version and
            next_schema_version are equal (such as 3 and 3), serialize and publish functions create and store output
            in OCI for this one schema only (in this case /DataNormalizationRegistry/v3). When different
            (such as 3 and 4), serialize and publish create and store output in OCI for both versions at once
            (/DataNormalizationRegistry/v3 and /DataNormalizationRegistry/v4).
            This supplies consumer teams with OCI files in both formats, until all are able to consume the new schema.
            To cut off the old schema output, set database_schema_version to the next_schema_version (in this case 4).
        object_storage_file_name (str): File name for OCI storage, for easy retrieval during storage and read utilities.
        object_storage_diff_name (str): Diff file in OCI storage, for easy retrieval during storage and read utilities.
        entries (list):
    """

    entries: List[DNRegistryEntry] = None
    database_schema_version = 4
    next_schema_version = 5
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

    def serialize(
        self,
        concept_map_schema_version: int = None,
        value_set_schema_version: int = None,
    ):
        """
        Serialize the DataNormalizationRegistry object into a list of entries. Each entry is a concept map or value set.
        The schema version for the DataNormalizationRegistry itself does not affect the format of any list entry.
        @param concept_map_schema_version: Format to use in serialization for concept_map entries. Caller may accept the
        default, or input a choice between the current ConceptMap.database_schema_version (such as 3) and
        ConceptMap.next_schema_version (such as 4). If None or not supplied, next_schema_version is used.
        @param value_set_schema_version: Similar but for value sets.
        @return: object structure representing the DNRegistryEntry and conforming to the specified schema versions.
        @raise BadRequestWithCode if a concept_map_schema_version value is provided, but turns out to be invalid.
        """
        return [
            x.serialize(concept_map_schema_version, value_set_schema_version)
            for x in self.entries
        ]

    @staticmethod
    def publish_to_object_store(registry, filepath):
        """
        Publish a Data Normalization Registry to the Object Storage.
        This function is passive. Details about schema version etc. are resolved by the caller and provided in inputs.
        @param registry: data to publish
        @param filepath: Caller sets the full path and filename in OCI.
        """
        if is_oci_write_disabled():
            LOGGER.info("OCI write operations are disabled.")
            return {"message": "Not pushed to bucket, OCI write operations are disabled"}
        object_storage_client = oci_authentication()
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data
        object_storage_client.put_object(
            namespace,
            bucket_name,
            filepath,
            json.dumps(registry, indent=2).encode("utf-8"),
        )
        return registry

    @staticmethod
    def get_oci_last_published_time():
        """
        Retrieve the last modified timestamp of the Data Normalization Registry file in the Object Storage.
        Timestamp of the registry in the folder for the most recent data schema format is returned.
        Returns the timestamp as a string.
        """
        filepath = f"{DataNormalizationRegistry.object_storage_folder_name}/v{DataNormalizationRegistry.next_schema_version}/{DataNormalizationRegistry.object_storage_file_name}"
        object_storage_client = oci_authentication()
        namespace = object_storage_client.get_namespace().data
        bucket_name = config("OCI_CLI_BUCKET")
        bucket_item = object_storage_client.get_object(
            namespace,
            bucket_name,
            filepath,
        )
        return bucket_item.headers["last-modified"]

    @staticmethod
    def get_last_published_registry(filepath: str = None):
        """
        The registry at the indicated filepath is returned. If filepath is None or omitted,
        returns the last published registry in the most recent data schema format.
        @param filepath: Full path and filename in OCI.
        """
        if filepath is None:
            filepath = f"{DataNormalizationRegistry.object_storage_folder_name}/v{DataNormalizationRegistry.next_schema_version}/{DataNormalizationRegistry.object_storage_file_name}"
        object_storage_client = oci_authentication()
        namespace = object_storage_client.get_namespace().data
        bucket_name = config("OCI_CLI_BUCKET")
        bucket_item = object_storage_client.get_object(
            namespace,
            bucket_name,
            filepath,
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

    @classmethod
    def publish_data_normalization_registry_output(
        cls,
        current_registry,
        norm_registry_schema_version: int,
        concept_map_schema_version: int,
        value_set_schema_version: int,
    ):
        """
        Helper method for calls to publish_data_normalization_registry using different combinations of schema version.
        """
        registry_serialized = current_registry.serialize(
            concept_map_schema_version, value_set_schema_version
        )
        filepath = f"{DataNormalizationRegistry.object_storage_folder_name}/v{norm_registry_schema_version}"
        full_filepath = (
            f"{filepath}/{DataNormalizationRegistry.object_storage_file_name}"
        )
        newly_published_version = DataNormalizationRegistry.publish_to_object_store(
            registry_serialized,
            f"{filepath}/{DataNormalizationRegistry.object_storage_file_name}",
        )
        try:
            previous_version = DataNormalizationRegistry.get_last_published_registry(
                full_filepath
            )
            diff_version = get_incremented_versions_and_update(
                previous_version, newly_published_version
            )
            DataNormalizationRegistry.publish_to_object_store(
                diff_version,
                f"{filepath}/{DataNormalizationRegistry.object_storage_diff_name}",
            )
        except ServiceError:
            pass

        return newly_published_version

    @classmethod
    def publish_data_normalization_registry(cls):
        """
        Publish the data normalization registry and the diff from previous version.
        If DataNormalizationRegistry.database_schema_version and DataNormalizationRegistry.next_schema_version are
        different (such as 3 and 4) publish registry and diff files to both output folders in OCI, in this case
        /DataNormalizationRegistry/v3 and /DataNormalizationRegistry/v4.
        """
        # Step 1: Get the data.
        current_registry = DataNormalizationRegistry()
        current_registry.load_entries()

        # Step 2: Output DataNormalizationRegistry.database_schema_version, which may be the same as next_schema_version
        newly_published_version = cls.publish_data_normalization_registry_output(
            current_registry,
            DataNormalizationRegistry.database_schema_version,
            app.concept_maps.models.ConceptMap.database_schema_version,
            app.value_sets.models.ValueSet.database_schema_version,
        )

        # Step 3: Also output DataNormalizationRegistry.next_schema_version, if different from database_schema_version
        if (
            DataNormalizationRegistry.database_schema_version
            != DataNormalizationRegistry.next_schema_version
        ):
            newly_published_version = cls.publish_data_normalization_registry_output(
                current_registry,
                DataNormalizationRegistry.next_schema_version,
                app.concept_maps.models.ConceptMap.next_schema_version,
                app.value_sets.models.ValueSet.next_schema_version,
            )

        # Step 4: Done
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
