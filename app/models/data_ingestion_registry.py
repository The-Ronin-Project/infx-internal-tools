import json
import datetime
import uuid
import app.models.concept_maps

from dataclasses import dataclass
from decouple import config
from sqlalchemy import text
from typing import List, cast
from app.database import get_db
from app.helpers.oci_helper import oci_authentication
from datetime import datetime
from dateutil import tz


@dataclass
class DNRegistryEntry:
    resource_type: str
    data_element: str
    tenant_id: str
    source_extension_url: str
    registry_uuid: str
    concept_map: app.models.concept_maps.ConceptMap

    def serialize(self):
        return {
            "resource_type": self.resource_type,
            "data_element": self.data_element,
            "tenant_id": self.tenant_id,
            "concept_map_uuid": str(self.concept_map.uuid),
            "version": self.concept_map.most_recent_active_version.version,
            "filename": f"ConceptMaps/v1/published/{self.concept_map.uuid}"
            f"/{self.concept_map.most_recent_active_version.version}.json",
            "source_extension_url": self.source_extension_url,
            "registry_uuid": str(self.registry_uuid),
            "concept_map_name": self.concept_map.name,
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
                    resource_type=item.resource_type,
                    data_element=item.data_element,
                    tenant_id=item.tenant_id,
                    source_extension_url=item.source_extension_url,
                    registry_uuid=item.registry_uuid,
                    concept_map=app.models.concept_maps.ConceptMap(
                        item.concept_map_uuid
                    ),
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
            "DataNormalizationRegistry/v1/registry.json",
            json.dumps(registry, indent=2).encode("utf-8"),
        )
        return registry

    @staticmethod
    def get_oci_last_published_time():
        """
        function to get the last modified time from registry file
        @return: timestamp string
        @rtype: string
        """
        object_storage_client = oci_authentication()
        namespace = object_storage_client.get_namespace().data
        bucket_name = config("OCI_CLI_BUCKET")
        bucket_item = object_storage_client.get_object(
            namespace, bucket_name, "DataNormalizationRegistry/v1/registry.json"
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
