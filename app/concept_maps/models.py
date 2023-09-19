import datetime
import uuid
import json
import hashlib
import re

from cachetools.func import ttl_cache

import app.models.codes
import app.value_sets.models
from elasticsearch.helpers import bulk
from sqlalchemy import text
from dataclasses import dataclass
from uuid import UUID
from typing import Optional, List
from app.database import get_db, get_elasticsearch
from app.helpers.oci_helper import set_up_object_store
from app.helpers.simplifier_helper import publish_to_simplifier
from app.models.codes import Code
import app.models.data_ingestion_registry
from app.terminologies.models import (
    Terminology,
    terminology_version_uuid_lookup,
    load_terminology_version_with_cache,
)
from operator import itemgetter
import itertools
from app.models.use_case import load_use_case_by_value_set_uuid

CONCEPT_MAPS_SCHEMA_VERSION = 3


# Function for checking if we have a coding array string that used to be JSON
def is_coding_array(source_code_string):
    return source_code_string.strip().startswith(
        "[{"
    ) or source_code_string.strip().startswith("{[{")


# This is from when we used `scrappyMaps`. It's used for mapping inclusions and
# can be removed as soon as that has been ported to the new maps.
# TODO remove this after Alex and Ben have updated the ED model to use concept mapps for ED utilization
# Todo: wait until we've updated (or deprecated) mapping inclusions in value sets to disable this
class DeprecatedConceptMap:
    def __init__(self, uuid, relationship_types, concept_map_name):
        self.uuid = uuid
        self.relationship_types = relationship_types

        self.concept_map_name = concept_map_name

        self.mappings = []
        self.load_mappings()

    def load_mappings(self):
        conn = get_db()
        mapping_query = conn.execute(
            text(
                """
                select *
                from "scrappyMaps".map_table
                where "mapsetName" = :map_set_name
                and "targetConceptDisplay" != 'null'
                """
            ),
            {
                "map_set_name": self.concept_map_name,
                # 'relationship_codes': self.relationship_types
            },
        )
        source_system = None
        source_version = None
        target_system = None
        target_version = None
        self.mappings = [
            (
                app.models.codes.Code(
                    source_system,
                    source_version,
                    x.sourceConceptCode,
                    x.sourceConceptDisplay,
                ),
                app.models.codes.Code(
                    target_system,
                    target_version,
                    x.targetConceptCode,
                    x.targetConceptDisplay,
                ),
                x.relationshipCode,
            )
            for x in mapping_query
        ]

    @property
    def source_code_to_target_map(self):
        result = {}
        for item in self.mappings:
            if item[2] not in self.relationship_types:
                continue
            code = item[0].code.strip()
            mapped_code_object = item[1]
            if code not in result:
                result[code] = [mapped_code_object]
            else:
                result[code].append(mapped_code_object)
        return result

    @property
    def target_code_to_source_map(self):
        result = {}
        for item in self.mappings:
            if item[2] not in self.relationship_types:
                continue
            code = item[1].code.strip()
            mapped_code_object = item[0]
            if code not in result:
                result[code] = [mapped_code_object]
            else:
                result[code].append(mapped_code_object)
        return result


@dataclass
class ConceptMapSettings:
    auto_advance_after_mapping: bool
    auto_fill_search_bar: bool
    show_target_codes_in_mapping_interface: bool

    def serialize(self):
        return {key: value for key, value in self.__dict__.items()}

    @classmethod
    def load(cls, concept_map_uuid):
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map
                where uuid=:concept_map_uuid
                """
            ),
            {"concept_map_uuid": concept_map_uuid},
        ).first()
        return ConceptMapSettings(
            auto_advance_after_mapping=bool(data.auto_advance_mapping),
            auto_fill_search_bar=bool(data.auto_fill_search),
            show_target_codes_in_mapping_interface=bool(data.show_target_codes),
        )

    @classmethod
    @ttl_cache()
    def load_from_cache(cls, concept_map_uuid):
        return cls.load(concept_map_uuid)

    @classmethod
    def load_by_concept_map_version_uuid(cls, concept_map_version_uuid):
        conn = get_db()
        data = conn.execute(
            text(
                """
                SELECT
                    cm.*
                FROM
                    concept_maps.concept_map_version cmv
                JOIN
                    concept_maps.concept_map cm ON cmv.concept_map_uuid = cm.uuid
                WHERE
                    cmv.uuid = :concept_map_version_uuid
                """
            ),
            {"concept_map_version_uuid": concept_map_version_uuid},
        ).first()
        return ConceptMapSettings(
            auto_advance_after_mapping=bool(data.auto_advance_mapping),
            auto_fill_search_bar=bool(data.auto_fill_search),
            show_target_codes_in_mapping_interface=bool(data.show_target_codes),
        )

    @classmethod
    @ttl_cache(ttl=60)  # Caching for 1min to speed up concept map versioning
    def load_by_concept_map_version_uuid_from_cache(cls, concept_map_version_uuid):
        return cls.load_by_concept_map_version_uuid(concept_map_version_uuid)


# This is the new maps system
class ConceptMap:
    """
    Class that represents a FHIR ConceptMap resource, which provides mappings between concepts in different code systems

    Attributes:
        uuid (str): The UUID of the concept map.
        name (str): The name of the concept map.
        title (str): The title of the concept map.
        description (str): The description of the concept map.
        use_case_uuid (str): The UUID of the use case associated with the concept map.
        publisher (str): The name of the publisher of the concept map.
        experimental (bool): Whether the concept map is experimental or not.
        author (str): The author of the concept map.
        created_date (datetime): The date when the concept map was created.
        include_self_map (bool): Whether to include the self-map in the concept map or not.
        source_value_set_uuid (str): The UUID of the source value set used in the concept map.
        target_value_set_uuid (str): The UUID of the target value set used in the concept map.
        most_recent_active_version (str): The UUID of the most recent active version of the concept map.
    """

    def __init__(self, uuid, load_mappings_for_most_recent_active: bool = True):
        self.uuid = uuid
        self.name = None
        self.title = None
        self.description = None
        self.use_case_uuid = None
        self.publisher = None
        self.experimental = None
        self.author = None
        self.created_date = None
        self.include_self_map = None
        self.source_value_set_uuid = None
        self.target_value_set_uuid = None

        self.most_recent_active_version = None
        self.settings = None

        self.load_data(
            load_mappings_for_most_recent_active=load_mappings_for_most_recent_active
        )

    def load_data(self, load_mappings_for_most_recent_active: bool = True):
        """
        runs sql query to get all information related to the concept map
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map
                where uuid=:concept_map_uuid
                """
            ),
            {"concept_map_uuid": self.uuid},
        ).first()

        self.title = data.title
        self.name = data.name
        self.description = data.description
        self.use_case_uuid = data.use_case_uuid
        self.publisher = data.publisher
        self.experimental = data.experimental
        self.author = data.author
        self.created_date = data.created_date
        self.source_value_set_uuid = data.source_value_set_uuid
        self.target_value_set_uuid = data.target_value_set_uuid

        self.settings = ConceptMapSettings(
            auto_advance_after_mapping=bool(data.auto_advance_mapping),
            auto_fill_search_bar=bool(data.auto_fill_search),
            show_target_codes_in_mapping_interface=bool(data.show_target_codes),
        )

        version = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version
                where concept_map_uuid=:concept_map_uuid
                and status='active'
                order by version desc
                limit 1
                """
            ),
            {"concept_map_uuid": self.uuid},
        ).first()
        if version is not None:
            self.most_recent_active_version = ConceptMapVersion(
                version.uuid,
                concept_map=self,
                load_mappings=load_mappings_for_most_recent_active,
            )
        else:
            self.most_recent_active_version = None

    def get_most_recent_version(self, active_only=False, load_mappings=True):
        conn = get_db()
        if active_only:
            query = """
                select * from concept_maps.concept_map_version
                where concept_map_uuid=:concept_map_uuid
                and status='active'
                order by version desc
                limit 1
                """
        else:
            query = """
                select * from concept_maps.concept_map_version
                where concept_map_uuid=:concept_map_uuid
                order by version desc
                limit 1
            """

        version_data = conn.execute(
            text(query),
            {"concept_map_uuid": self.uuid},
        ).first()
        return ConceptMapVersion(version_data.uuid, load_mappings=load_mappings)

    @classmethod
    def concept_map_metadata(cls, cm_uuid):
        """
        This function executes a sql query to get the concept map based on the uuid passed in.
        :param cm_uuid: concept map uuid
        :return: tuple of metadata related to the given concept map uuid
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map
                where uuid=:uuid
                """
            ),
            {"uuid": cm_uuid},
        ).first()
        return data

    @classmethod
    def initial_concept_map_creation(
        cls,
        name,
        title,
        publisher,
        author,
        use_case_uuid,
        cm_description,
        experimental,
        source_value_set_uuid,
        target_value_set_uuid,
        cm_version_description,
        source_value_set_version_uuid,
        target_value_set_version_uuid,
    ):
        """
        This function creates a brand-new concept map and concept map version 1 and inserts the source concept value set version codes, displays and systems into the source concept table.
        :param name: string concept map name
        :param title: string concept map title
        :param publisher: string hard coded Project Ronin
        :param author: string autofill as retool current user
        :param use_case_uuid: uuid use case
        :param cm_description: string concept map description
        :param experimental: boolean
        :param source_value_set_uuid: uuid value set
        :param target_value_set_uuid: uuid value set
        :param cm_version_description: string concept map version description
        :param source_value_set_version_uuid: uuid value set version
        :param target_value_set_version_uuid: uuid value set version
        :return: tuple of metadata related to the given concept map uuid
        """
        conn = get_db()
        cm_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                insert into concept_maps.concept_map
                (uuid, name, title, publisher, author, description, experimental, use_case_uuid, created_date, source_value_set_uuid, target_value_set_uuid)
                values
                (:uuid, :name, :title, :publisher, :author, :description, :experimental, :use_case_uuid, :created_date, :source_value_set_uuid, :target_value_set_uuid)
                """
            ),
            {
                "uuid": cm_uuid,
                "name": name,
                "title": title,
                "publisher": publisher,
                "author": author,
                "description": cm_description,
                "created_date": datetime.datetime.now(),
                "experimental": experimental,
                "use_case_uuid": use_case_uuid,
                "source_value_set_uuid": source_value_set_uuid,
                "target_value_set_uuid": target_value_set_uuid,
            },
        )

        cm_version_uuid = uuid.uuid4()
        conn.execute(
            text(
                """
                insert into concept_maps.concept_map_version
                (uuid, concept_map_uuid, description, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
                values
                (:uuid, :concept_map_uuid, :description, :status, :created_date, :version, :source_value_set_version_uuid, :target_value_set_version_uuid)
                """
            ),
            {
                "concept_map_uuid": cm_uuid,
                "uuid": cm_version_uuid,
                "description": cm_version_description,
                "status": "pending",
                "created_date": datetime.datetime.now(),
                "version": 1,
                "source_value_set_version_uuid": source_value_set_version_uuid,
                "target_value_set_version_uuid": target_value_set_version_uuid,
            },
        )
        cls.insert_source_concepts_for_mapping(
            cm_version_uuid, source_value_set_version_uuid
        )
        cls.index_targets(cm_version_uuid, target_value_set_version_uuid)
        return cls(cm_uuid)

    @classmethod
    def insert_source_concepts_for_mapping(
        cls, cmv_uuid, source_value_set_version_uuid
    ):
        """
        This function gets and inserts the codes, displays and systems from the source value set version AND a concept map version uuid, into the source_concept table for mapping.
        :param cmv_uuid: uuid concept map version
        :param source_value_set_version_uuid: uuid source value set version
        :return: none, the items are simply inserted into the concept_maps.source_concepts table
        """
        conn = get_db()
        # Populate concept_maps.source_concept
        conn.execute(
            text(
                """
                insert into concept_maps.source_concept
                (uuid, code, display, system, map_status, concept_map_version_uuid, custom_terminology_uuid)
                select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid, custom_terminology_uuid from value_sets.expansion_member
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where expansion_uuid in 
                (select uuid from value_sets.expansion
                where vs_version_uuid=:source_value_set_version_uuid
                order by timestamp desc
                limit 1)
                """
            ),
            {
                "source_value_set_version_uuid": source_value_set_version_uuid,
                "concept_map_version_uuid": cmv_uuid,
            },
        )

    @staticmethod
    def index_targets(
        concept_map_version_uuid: uuid.UUID, target_value_set_version_uuid: uuid.UUID
    ):
        """
        Indexes the target concepts for the given concept map version and target value set version in Elasticsearch.

        Args:
            concept_map_version_uuid (str): The UUID of the concept map version.
            target_value_set_version_uuid (str): The UUID of the target value set version.
        """
        es = get_elasticsearch()

        def gendata():
            vs_version = app.value_sets.models.ValueSetVersion.load(
                target_value_set_version_uuid
            )
            vs_version.expand()
            for concept in vs_version.expansion:
                terminology_version_uuid = terminology_version_uuid_lookup(
                    concept.system, concept.version
                )
                document = {
                    "_id": (str(concept_map_version_uuid) + str(concept.code)),
                    "_index": "target_concepts_for_mapping",
                    "code": concept.code,
                    "display": concept.display,
                    "concept_map_version_uuid": str(concept_map_version_uuid),
                    "terminology_version_uuid": terminology_version_uuid,
                }
                yield document

        bulk(es, gendata())

    def serialize(self):
        """
        This function serializes a concept map object
        :return: Serialized concept map metadata
        """
        return {
            "uuid": str(self.uuid),
            "name": self.name,
            "title": self.title,
            "publisher": self.publisher,
            "author": self.author,
            "description": self.description,
            "created_date": self.created_date,
            "experimental": self.experimental,
            "use_case_uuid": self.use_case_uuid,
            "source_value_set_uuid": str(self.source_value_set_uuid),
            "target_value_set_uuid": str(self.target_value_set_uuid),
            "settings": self.settings.serialize(),
        }


class ConceptMapVersion:
    def __init__(self, uuid, concept_map=None, load_mappings: bool = True):
        self.uuid = uuid
        self.concept_map = None
        self.description = None
        self.comments = None
        self.status = None
        self.created_date = None
        self.published_date = None
        self.version = None
        self.allowed_target_terminologies = []
        self.mappings = {}
        self.url = None
        self.source_value_set_version_uuid = None
        self.target_value_set_version_uuid = None

        self.load_data(concept_map=concept_map, load_mappings=load_mappings)

    def load_data(self, concept_map: ConceptMap = None, load_mappings: bool = True):
        """
        runs sql query to return all information related to specified concept map version, data returned is used to
        set class attributes
        :rtype: object
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version
                where uuid=:version_uuid
                """
            ),
            {"version_uuid": self.uuid},
        ).first()

        if concept_map is None:
            self.concept_map = ConceptMap(data.concept_map_uuid)
        else:
            self.concept_map = concept_map

        self.description = data.description
        self.comments = data.comments
        self.status = data.status
        self.created_date = data.created_date
        self.version = data.version
        self.published_date = data.published_date
        self.source_value_set_version_uuid = data.source_value_set_version_uuid
        self.target_value_set_version_uuid = data.target_value_set_version_uuid
        self.load_allowed_target_terminologies()
        if load_mappings:
            self.load_mappings()
        # self.generate_self_mappings()

    @classmethod
    def load_by_concept_map_uuid_and_version(cls, concept_map_uuid, version):
        """
        Receives a concept_map_uuid and version and returns the appropriate ConceptMapVersion, if it exists
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                SELECT * FROM concept_maps.concept_map_version
                WHERE concept_map_uuid=:concept_map_uuid
                AND version=:version
                """
            ),
            {"concept_map_uuid": concept_map_uuid, "version": version},
        ).first()
        if data:
            concept_map_version = ConceptMapVersion(data.uuid)
            return concept_map_version
        else:
            return None

    def load_allowed_target_terminologies(self):
        """
        runs query to get target terminology related to concept map version, called from the load method above.
        Data returned is looped through and appended to the allowed_target_terminologies attribute list
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version_terminologies
                where concept_map_version_uuid=:concept_map_version_uuid
                and context='target_terminology'
                """
            ),
            {"concept_map_version_uuid": self.uuid},
        )
        for item in data:
            terminology_version_uuid = item.terminology_version_uuid
            self.allowed_target_terminologies.append(
                Terminology.load(terminology_version_uuid)
            )

    # def generate_self_mappings(self):
    #     """
    #     if self_map flag in db is true, generate the self_mappings here
    #     :rtype: mappings dictionary
    #     """
    #     if self.concept_map.include_self_map is True:
    #         for target_terminology in self.allowed_target_terminologies:
    #             target_terminology.load_content()
    #             for code in target_terminology.codes:
    #                 self.mappings[code] = [
    #                     Mapping(
    #                         source=code,
    #                         relationship=MappingRelationship.load_by_code("equivalent"),
    #                         target=code,  # self is equivalent to self
    #                         mapping_comments="Auto-generated self map",
    #                     )
    #                 ]

    def load_mappings(self):
        conn = get_db()
        query = """
            select concept_relationship.uuid as mapping_uuid, concept_relationship.author, concept_relationship.review_status, concept_relationship.mapping_comments,
            source_concept.uuid as source_concept_uuid, source_concept.code as source_code, source_concept.display as source_display, source_concept.system as source_system, 
            tv_source.version as source_version, tv_source.fhir_uri as source_fhir_uri,
            source_concept.comments as source_comments, source_concept.additional_context as source_additional_context, source_concept.map_status as source_map_status,
            source_concept.assigned_mapper as source_assigned_mapper, source_concept.assigned_reviewer as source_assigned_reviewer, source_concept.no_map,
            source_concept.reason_for_no_map, source_concept.mapping_group as source_mapping_group, source_concept.previous_version_context as source_previous_version_context,
            relationship_codes.code as relationship_code, source_concept.map_status,
            concept_relationship.target_concept_code, concept_relationship.target_concept_display,
            concept_relationship.target_concept_system_version_uuid as target_system,
            tv_target.version as target_version, tv_target.fhir_uri as target_fhir_uri,
            ctc.depends_on_property, ctc.depends_on_system, ctc.depends_on_value, ctc.depends_on_display
            from concept_maps.source_concept
            left join concept_maps.concept_relationship
            on source_concept.uuid = concept_relationship.source_concept_uuid
            left join custom_terminologies.code ctc
            on source_concept.custom_terminology_uuid = ctc.uuid
            join concept_maps.relationship_codes
            on relationship_codes.uuid = concept_relationship.relationship_code_uuid
            join terminology_versions as tv_source
            on cast(tv_source.uuid as uuid) = cast(source_concept.system as uuid)
            join terminology_versions as tv_target
            on tv_target.uuid = concept_relationship.target_concept_system_version_uuid
            where source_concept.concept_map_version_uuid=:concept_map_version_uuid
            and concept_relationship.review_status = 'reviewed'
            """

        results = conn.execute(
            text(query),
            {
                "concept_map_version_uuid": self.uuid,
            },
        )

        for item in results:
            source_code = SourceConcept(
                uuid=item.source_concept_uuid,
                code=item.source_code,
                display=item.source_display,
                system=load_terminology_version_with_cache(item.source_system),
                comments=item.source_comments,
                additional_context=item.source_additional_context,
                map_status=item.source_map_status,
                assigned_mapper=item.source_assigned_mapper,
                assigned_reviewer=item.source_assigned_reviewer,
                no_map=item.no_map,
                reason_for_no_map=item.reason_for_no_map,
                mapping_group=item.source_mapping_group,
                previous_version_context=item.source_previous_version_context,
                concept_map_version_uuid=self.uuid,
                depends_on_property=item.depends_on_property,
                depends_on_system=item.depends_on_system,
                depends_on_value=item.depends_on_value,
                depends_on_display=item.depends_on_display,
            )
            target_code = Code(
                item.target_fhir_uri,
                item.target_version,
                item.target_concept_code,
                item.target_concept_display,
            )
            relationship = MappingRelationship.load_by_code_from_cache(
                item.relationship_code
            )  # this needs optimization

            mapping = Mapping(
                source_code,
                relationship,
                target_code,
                mapping_comments=item.mapping_comments,
                author=item.author,
                uuid=item.mapping_uuid,
                review_status=item.review_status,
                reason_for_no_map=item.reason_for_no_map,
            )
            if source_code in self.mappings:
                self.mappings[source_code].append(mapping)
            else:
                self.mappings[source_code] = [mapping]

    def mapping_draft(self):
        """
        This function runs a query to retrieve concepts for an in-progress concept map. This should include everything,
        the end user can filter the result as they choose.
        :return: A data list and field names list
        """
        conn = get_db()
        query = """
            select sc.*, cr.*, rc.display as relationship_display from concept_maps.source_concept sc
            left join concept_maps.concept_relationship cr on sc.uuid=cr.source_concept_uuid
            left join concept_maps.relationship_codes rc on rc.uuid=cr.relationship_code_uuid
            WHERE sc.concept_map_version_uuid=:concept_map_version_uuid
            """
        results = conn.execute(
            text(query),
            {
                "concept_map_version_uuid": self.uuid,
            },
        )
        # Create an empty list to hold the data
        data = []
        # Iterate over the query results to populate the data list
        for row in results:
            data.append(
                {
                    "Source Code": row[1],
                    "Source Display": row[2],
                    "Relationship": row[31],
                    "Target Code": row[20],
                    "Target Display": row[21],
                    "Review Status": row[17],
                    "Map Status": row[6],
                    "Mapping Comments": row[18],
                    "Comments": row[4],
                    "Additional Context": row[5],
                    "No-Map": row[10],
                    "No-Map Reason": row[11],
                    "Mapping Group": row[12],
                    "Mapper": row[25],
                    "Reviewer": row[30],
                    "Review Comment": row[29],
                }
            )

        fieldnames = [
            "Source Code",
            "Source Display",
            "Relationship",
            "Target Code",
            "Target Display",
            "Review Status",
            "Map Status",
            "Mapping Comments",
            "Comments",
            "Additional Context",
            "No-Map",
            "No-Map Reason",
            "Mapping Group",
            "Mapper",
            "Reviewer",
            "Review Comment",
        ]

        return data, fieldnames

    def version_set_status_active(self):
        """
        Sets the status of this concept map version to 'active' in the database.

        This method updates the status of this instance of a concept map version in the
        'concept_maps.concept_map_version' table in the database, setting it to 'active'.
        """

        conn = get_db()
        conn.execute(
            text(
                """
                    UPDATE concept_maps.concept_map_version
                    SET status=:status
                    WHERE uuid=:version_uuid
                    """
            ),
            {
                "status": "active",
                "version_uuid": self.uuid,
            },
        )
        self.status = "active"

    def set_publication_date(self):
        conn = get_db()
        conn.execute(
            text(
                """
                UPDATE concept_maps.concept_map_version
                SET published_date=now()
                where uuid=:uuid
                """
            ),
            {
                "uuid": self.uuid,
            },
        )
        self.published_date = datetime.datetime.now()

    def retire_and_obsolete_previous_version(self):
        """
        Updates the status of previous versions of a concept map in the database.

        This method sets the status of previous 'active' versions of the same concept map to 'retired',
        and sets the status of 'pending', 'in progress', and 'reviewed' versions to 'obsolete'. This is
        based on the current instance of the concept map version's UUID and the associated concept map's UUID.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Raises
        ------
        Any exceptions raised by the database operation will propagate up to the caller.

        Notes
        -----
        This method directly modifies the 'concept_maps.concept_map_version' table in the database.
        It does not return a value. The database connection is obtained from the `get_db` function.
        The 'status' field of the previous versions of the same concept map, excluding the current
        version (identified by `self.uuid`), are updated.
        """

        conn = get_db()
        conn.execute(
            text(
                """
                update concept_maps.concept_map_version
                set status = 'retired'
                where status = 'active'
                and concept_map_uuid =:concept_map_uuid
                and uuid !=:version_uuid
                """
            ),
            {"concept_map_uuid": self.concept_map.uuid, "version_uuid": self.uuid},
        )
        conn.execute(
            text(
                """
                update concept_maps.concept_map_version
                set status = 'obsolete'
                where status in ('pending','in progress','reviewed')
                and concept_map_uuid =:concept_map_uuid
                and uuid !=:version_uuid
                """
            ),
            {"concept_map_uuid": self.concept_map.uuid, "version_uuid": self.uuid},
        )

    def serialize_mappings(self):
        # Identify all the source terminology / target terminology pairings in the mappings
        source_target_pairs_set = set()
        for source_code, mappings in self.mappings.items():
            source_uri = source_code.system.fhir_uri
            source_version = source_code.system.version
            for mapping in mappings:
                target_uri = mapping.target.system
                target_version = mapping.target.version

                source_target_pairs_set.add(
                    (source_uri, source_version, target_uri, target_version)
                )

        groups = []

        # Serialize the mappings
        for (
            source_uri,
            source_version,
            target_uri,
            target_version,
        ) in source_target_pairs_set:
            elements = []
            for source_code, mappings in self.mappings.items():
                if (
                    source_code.system.fhir_uri == source_uri
                    and source_code.system.version == source_version
                ):
                    filtered_mappings = [
                        x
                        for x in mappings
                        if x.target.system == target_uri
                        and x.target.version == target_version
                    ]
                    # Only proceed if there are filtered_mappings for the current target_uri and target_version
                    if filtered_mappings:
                        # Do relevant checks on the code and display
                        source_code_code = source_code.code.rstrip()
                        source_code_display = source_code.display.rstrip()

                        # Checking to see if the display is a coding array TODO: INFX-2521 this is a temporary problem that we should fix in the future
                        if is_coding_array(source_code_display):
                            source_code_code, source_code_display = (
                                source_code_display,
                                source_code_code,
                            )

                        # We want the text string array that is supposed to be json to be formatted correctly
                        # If it's not an array it should return the original string
                        if is_coding_array(source_code_code):
                            source_code_code = transform_struct_string_to_json(
                                source_code_code
                            )
                        new_element = {
                            "id": source_code.id,
                            "code": source_code_code,
                            "display": source_code_display,
                            "target": [],
                        }

                        # Iterate through each mapping for the source and serialize it
                        for mapping in filtered_mappings:
                            if (
                                mapping.target.code == "No map"
                                and mapping.target.display == "No matching concept"
                            ):
                                comment = mapping.reason_for_no_map
                            else:
                                comment = None
                            target_serialized = {
                                "id": mapping.id,
                                "code": mapping.target.code,
                                "display": mapping.target.display,
                                "equivalence": mapping.relationship.code,
                                "comment": comment,
                            }

                            # Add dependsOn data
                            if (
                                source_code.depends_on_property
                                or source_code.depends_on_value
                            ):
                                depends_on_value = source_code.depends_on_value
                                if is_coding_array(depends_on_value):
                                    depends_on_value = transform_struct_string_to_json(
                                        depends_on_value
                                    )
                                depends_on = {
                                    "property": source_code.depends_on_property,
                                    "value": depends_on_value,
                                }
                                if source_code.depends_on_system:
                                    depends_on["system"] = source_code.depends_on_system
                                if source_code.depends_on_display:
                                    depends_on[
                                        "display"
                                    ] = source_code.depends_on_display
                                target_serialized["dependsOn"] = depends_on

                            new_element["target"].append(target_serialized)

                        elements.append(new_element)

            groups.append(
                {
                    "source": source_uri,
                    "sourceVersion": source_version,
                    "target": target_uri,
                    "targetVersion": target_version,
                    "element": elements,
                }
            )

        return groups

    def get_use_case_description(self):
        conn = get_db()
        result = conn.execute(
            text(
                """select description from project_management.use_case
                where uuid=:uuid"""
            ),
            {
                "uuid": self.concept_map.use_case_uuid,
            },
        ).first()
        if result is not None:
            return result.description
        return None

    def serialize(self, include_internal_info=False):
        pattern = r"[A-Z]([A-Za-z0-9_]){0,254}"  # name transformer
        if re.match(pattern, self.concept_map.name):  # name follows pattern use name
            rcdm_name = self.concept_map.name
        else:
            index = re.search(
                r"[a-zA-Z]", self.concept_map.name
            ).start()  # name does not follow pattern, uppercase 1st letter
            rcdm_name = (
                self.concept_map.name[:index]
                + self.concept_map.name[index].upper()
                + self.concept_map.name[index + 1 :]
            )

        serial_mappings = self.serialize_mappings()
        for mapped_object in serial_mappings:
            for nested in mapped_object["element"]:
                for item in nested["target"]:
                    if item["equivalence"] == "source-is-narrower-than-target":
                        item["equivalence"] = "wider"
                        # [on_true] if [expression] else [on_false]
                        item["comment"] = (
                            f"{item['comment']} source-is-narrower-than-target"
                            if item["comment"]
                            else "source-is-narrower-than-target"
                        )
                    elif item["equivalence"] == "source-is-broader-than-target":
                        item["equivalence"] = "narrower"
                        item["comment"] = (
                            f"{item['comment']} source-is-broader-than-target"
                            if item["comment"]
                            else "source-is-broader-than-target"
                        )
                    # Conditionally remove the comment field if it is None or empty
                    if item["comment"] is None or item["comment"] == "":
                        item.pop("comment", None)
        serialized = {
            "resourceType": "ConceptMap",
            "title": self.concept_map.title,
            "id": str(self.concept_map.uuid),
            "name": rcdm_name if self.concept_map.name is not None else None,
            "contact": [{"name": self.concept_map.author}],
            "url": f"http://projectronin.io/fhir/StructureDefinition/ConceptMap/{self.concept_map.uuid}",
            "description": self.concept_map.description,
            "purpose": self.get_use_case_description(),
            "publisher": self.concept_map.publisher,
            "experimental": self.concept_map.experimental,
            "status": self.status,
            "date": self.published_date.strftime("%Y-%m-%d")
            if self.published_date
            else None,
            "version": self.version,
            "group": serial_mappings,
            "extension": [
                {
                    "url": "http://projectronin.io/fhir/StructureDefinition/Extension/ronin-conceptMapSchema",
                    "valueString": f"{CONCEPT_MAPS_SCHEMA_VERSION}.0.0",
                }
            ]
            # For now, we are intentionally leaving out created_dates as they are not part of the FHIR spec and
            # are not required for our use cases at this time
        }
        if self.published_date is not None:
            serialized["meta"] = {
                "lastUpdated": self.published_date.strftime(
                    "%Y-%m-%dT%H:%M:%S.%f+00:00"
                )
            }

        if include_internal_info:
            serialized["internalData"] = {
                "source_value_set_uuid": self.concept_map.source_value_set_uuid,
                "source_value_set_version_uuid": self.source_value_set_version_uuid,
                "target_value_set_uuid": self.concept_map.target_value_set_uuid,
                "target_value_set_version_uuid": self.target_value_set_version_uuid,
            }
        return serialized

    def prepare_for_oci(self):
        serialized = self.serialize()
        rcdm_id = serialized.get("id")
        rcdm_url = "http://projectronin.io/ConceptMap/"
        # id will depend on publisher
        if self.concept_map.publisher == "Project Ronin":
            rcdm_id = serialized.get("id")
            rcdm_url = "http://projectronin.io/fhir/ConceptMap/"
        elif self.concept_map.publisher == "FHIR":
            rcdm_id = serialized.get("name")
            # transform rcdm_id in place
            rcdm_id = re.sub("([a-z])([A-Z])", r"\1-\2", rcdm_id).lower()
            rcdm_url = "http://hl7.org/fhir/ConceptMap/"

        if (
            self.status == "pending"
        ):  # has a required binding (translate pending to draft)
            rcdm_status = "draft"
        else:
            rcdm_status = self.status

        rcdm_date_now = datetime.datetime.now()
        rcdm_date = rcdm_date_now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        oci_serialized = {
            "id": rcdm_id,
            "url": f"{rcdm_url}{rcdm_id}",
            # specific to the overall value set; suffix matching the id field exactly
            "status": rcdm_status,
            # has a required binding (translate pending to draft)  (draft, active, retired, unknown)
            "date": rcdm_date,  # the date the status was set to active
            "meta": {
                "profile": [
                    "http://projectronin.io/fhir/StructureDefinition/ronin-conceptMap"
                ]
            },
        }

        serialized.update(oci_serialized)  # Merge oci_serialized into serialized
        serialized.pop("contact")
        serialized.pop("publisher")
        serialized.pop("title")
        initial_path = f"ConceptMaps/v{CONCEPT_MAPS_SCHEMA_VERSION}/folder/{self.concept_map.uuid}"  # folder is set in oci_helper(determined by api call)

        return serialized, initial_path

    def publish(self):
        """
        A method to complete the full publication process including pushing to OCI, Simplifier,
        Normalization Registry and setting status active.
        @return: n/a
        """
        concept_map_version = self
        (
            concept_map_to_json,
            initial_path,
        ) = self.prepare_for_oci()  # serialize the metadata

        set_up_object_store(
            concept_map_to_json, initial_path, folder="published"
        )  # sends to OCI
        self.version_set_status_active()
        self.set_publication_date()
        self.retire_and_obsolete_previous_version()
        self.to_simplifier()
        # Publish new version of data normalization registry
        app.models.data_ingestion_registry.DataNormalizationRegistry.publish_data_normalization_registry()

    def to_simplifier(self):
        """
        A method to send a concept map version to
        @return: n/a
        """
        concept_map_to_json, initial_path = self.prepare_for_oci()
        resource_id = concept_map_to_json["id"]
        resource_type = concept_map_to_json["resourceType"]  # param for Simplifier
        concept_map_to_json["status"] = "active"  # Simplifier requires status
        # Check if the 'group' key is present
        if "group" in concept_map_to_json and len(concept_map_to_json["group"]) > 0:
            group = concept_map_to_json["group"][0]

            if "element" in group:
                group["element"] = group["element"][
                    :50
                ]  # Limit the 'element' list to the top 50 entries

                for element in group["element"]:
                    if "target" in element:
                        element["target"].sort(
                            key=itemgetter("id", "code")
                        )  # Sort list of dicts by 'id' and 'code'
                        element["target"] = list(
                            target for target, _ in itertools.groupby(element["target"])
                        )  # Remove duplicates
        publish_to_simplifier(resource_type, resource_id, concept_map_to_json)

    @classmethod
    def get_active_concept_map_versions(cls) -> List["ConceptMapVersion"]:
        """
        A class method to query the database for active concept map versions.
        @return: A list on active concept maps version uuids
        """
        conn = get_db()
        data = conn.execute(
            text(
                """  
                SELECT uuid FROM concept_maps.concept_map_version  
                WHERE status = 'active'  
                """
            )
        )

        active_concept_map_versions = [cls(row.uuid) for row in data]
        return active_concept_map_versions


@dataclass
class MappingRelationship:
    uuid: UUID
    code: str
    display: str

    @classmethod
    def load(cls, uuid):
        """
        Load a mapping relationship from the database using the UUID of the mapping relationship.

        :return: The mapping relationship loaded from the database.
        """
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.relationship_codes
                where uuid=:uuid
                """
            ),
            {"uuid": uuid},
        ).first()

        return cls(uuid=data.uuid, code=data.code, display=data.display)

    @classmethod
    def load_by_code(cls, code):
        """
        Load a mapping relationship from the database by code of the mapping relationship.

        :return: The mapping relationship loaded from the database.
        """

        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.relationship_codes
                where code=:code
                """
            ),
            {"code": code},
        ).first()
        return cls(uuid=data.uuid, code=data.code, display=data.display)

    @classmethod
    @ttl_cache()
    def load_by_code_from_cache(cls, code):
        return cls.load_by_code(code)

    @classmethod
    def load_by_uuid(cls, uuid):
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.relationship_codes
                where uuid=:uuid
                """
            ),
            {"uuid": uuid},
        ).first()
        return cls(uuid=data.uuid, code=data.code, display=data.display)

    @classmethod
    @ttl_cache()
    def load_by_uuid_from_cache(cls, uuid):
        return cls.load_by_uuid(uuid)

    def serialize(self):
        """
        Prepares a JSON representation of the mapping relationship to return to the API

        :return: A dictionary containing the serialized mapping relationship.
        """
        return {"uuid": self.uuid, "code": self.code, "display": self.display}


@dataclass
class SourceConcept:
    uuid: UUID
    code: str
    display: str
    system: Terminology
    comments: Optional[str] = None
    additional_context: Optional[str] = None
    map_status: Optional[str] = None
    assigned_mapper: Optional[UUID] = None
    assigned_reviewer: Optional[UUID] = None
    no_map: Optional[bool] = None
    reason_for_no_map: Optional[str] = None
    mapping_group: Optional[str] = None
    previous_version_context: Optional[dict] = None
    concept_map_version_uuid: Optional[UUID] = None
    depends_on_property: Optional[str] = None
    depends_on_system: Optional[str] = None
    depends_on_value: Optional[str] = None
    depends_on_display: Optional[str] = None

    def __post_init__(self):
        self.code_object = Code(
            uuid=self.uuid,
            code=self.code,
            display=self.display,
            system=self.system.fhir_uri,
            version=self.system.version,
        )

    def __hash__(self):
        return hash(
            (
                self.uuid,
                self.code,
                self.display,
                self.system,
                self.depends_on_property,
                self.depends_on_system,
                self.depends_on_value,
                self.depends_on_display,
            )
        )

    @property
    def id(self):
        """
        Calculates a unique MD5 hash for the current instance.

        This property combines `code` and `display` attributes, strips off any leading or trailing white spaces,
        and then converts the combined string to an MD5 hash. Note that the combined string is encoded in UTF-8
        before generating the hash.

        Returns:
            str: A hexadecimal string representing the MD5 hash of the `code` and `display` attributes.

        Raises:
            AttributeError: If the `code` or `display` attribute is not set for the instance.
        """
        combined = (self.code.strip() + self.display.strip()).encode("utf-8")
        return hashlib.md5(combined).hexdigest()

    @classmethod
    def load(cls, source_concept_uuid: UUID) -> "SourceConcept":
        conn = get_db()
        query = text(
            """
                SELECT uuid, code, display, system, comments,
                       additional_context, map_status, assigned_mapper,
                       assigned_reviewer, no_map, reason_for_no_map,
                       mapping_group, previous_version_context, concept_map_version_uuid
                FROM concept_maps.source_concept
                WHERE uuid = :uuid
            """
        )
        result = conn.execute(query, {"uuid": str(source_concept_uuid)}).fetchone()

        if result:
            return cls(
                uuid=result.uuid,
                code=result.code,
                display=result.display,
                system=Terminology.load(result.system),
                comments=result.comments,
                additional_context=result.additional_context,
                map_status=result.map_status,
                assigned_mapper=result.assigned_mapper,
                assigned_reviewer=result.assigned_reviewer,
                no_map=result.no_map,
                reason_for_no_map=result.reason_for_no_map,
                mapping_group=result.mapping_group,
                previous_version_context=result.previous_version_context,
                concept_map_version_uuid=result.concept_map_version_uuid,
            )
        else:
            raise ValueError(f"No source concept found with UUID {source_concept_uuid}")

    def update(
        self,
        comments: Optional[str] = None,
        additional_context: Optional[str] = None,
        map_status: Optional[str] = None,
        assigned_mapper: Optional[UUID] = None,
        assigned_reviewer: Optional[UUID] = None,
        no_map: Optional[bool] = None,
        reason_for_no_map: Optional[str] = None,
        mapping_group: Optional[str] = None,
        previous_version_context: Optional[str] = None,
    ):
        conn = get_db()
        # Create a dictionary to store the column names and their corresponding new values
        updates = {}

        if comments is not None:
            updates["comments"] = comments

        if additional_context is not None:
            updates["additional_context"] = additional_context

        if map_status is not None:
            updates["map_status"] = map_status

        if assigned_mapper is not None:
            updates["assigned_mapper"] = str(assigned_mapper)

        if assigned_reviewer is not None:
            updates["assigned_reviewer"] = str(assigned_reviewer)

        if no_map is not None:
            updates["no_map"] = no_map

        if reason_for_no_map is not None:
            updates["reason_for_no_map"] = reason_for_no_map

        if mapping_group is not None:
            updates["mapping_group"] = mapping_group

        if previous_version_context is not None:
            updates["previous_version_context"] = previous_version_context

        # Generate the SQL query
        query = f"UPDATE concept_maps.source_concept SET "
        query += ", ".join(f"{column} = :{column}" for column in updates)
        query += f" WHERE uuid = :uuid"

        # Execute the SQL query
        updates["uuid"] = str(self.uuid)
        conn.execute(text(query), updates)

        # Update the instance attributes
        for column, value in updates.items():
            setattr(self, column, value)

    def serialize(self) -> dict:
        serialized_data = {
            "uuid": str(self.uuid),
            "code": self.code,
            "display": self.display,
            "system": self.system.serialize(),
            "comments": self.comments,
            "additional_context": self.additional_context,
            "map_status": self.map_status,
            "assigned_mapper": str(self.assigned_mapper)
            if self.assigned_mapper
            else None,
            "assigned_reviewer": str(self.assigned_reviewer)
            if self.assigned_reviewer
            else None,
            "no_map": self.no_map,
            "reason_for_no_map": self.reason_for_no_map,
            "mapping_group": self.mapping_group,
            "previous_version_context": self.previous_version_context,
        }
        return serialized_data


@dataclass
class Mapping:
    """
    Represents a mapping relationship between two codes, and provides methods to load, save and use the relationships.
    """

    source: SourceConcept
    relationship: MappingRelationship
    target: Code
    uuid: Optional[UUID] = None
    mapping_comments: Optional[str] = None
    author: Optional[str] = None
    conn: Optional[None] = None
    review_status: str = "ready for review"
    created_date: Optional[datetime.datetime] = None
    reviewed_date: Optional[datetime.datetime] = None
    review_comment: Optional[str] = None
    reviewed_by: Optional[str] = None
    reason_for_no_map: Optional[str] = None

    def __post_init__(self):
        self.conn = get_db()
        self.uuid = uuid.uuid4()

    @property
    def id(self):
        """
        Generates and returns an MD5 hash as a hexadecimal string.

        The hash is created using the following attributes:
        - source.id
        - relationship.code
        - target.code
        - target.display
        - target.system

        This method does not take any arguments, and it returns a string representing the hexadecimal value of the MD5 hash.

        :return: a string of hexadecimal digits representing an MD5 hash
        """
        # concatenate the required attributes into a string
        concat_str = (
            str(self.source.id)
            + str(self.source.depends_on_property).strip()
            + str(self.source.depends_on_system).strip()
            + str(self.source.depends_on_value).strip()
            + str(self.source.depends_on_display).strip()
            + self.relationship.code
            + self.target.code
            + self.target.display
            + self.target.system
        )
        # create a new md5 hash object
        hash_object = hashlib.md5()
        # update the hash object with the bytes-like object
        hash_object.update(concat_str.encode("utf-8"))
        # return the hexadecimal representation of the hash
        return hash_object.hexdigest()

    @classmethod
    def load(cls, uuid):
        """Loads a Mapping instance from the database by its UUID."""
        pass

    def save(self):
        """Saves the mapping relationship instance to the database."""
        self.conn.execute(
            text(
                """
                INSERT INTO concept_maps.concept_relationship(
                uuid, review_status, source_concept_uuid, relationship_code_uuid, target_concept_code, 
                target_concept_display, target_concept_system_version_uuid, mapping_comments, author, created_date
                ) VALUES (
                :uuid, :review_status, :source_concept_uuid, :relationship_code_uuid, :target_concept_code, 
                :target_concept_display, :target_concept_system_version_uuid, :mapping_comments, :author, :created_date
                );
                """
            ),
            {
                "uuid": self.uuid,
                "review_status": self.review_status,
                "source_concept_uuid": self.source.uuid,
                "relationship_code_uuid": self.relationship.uuid,
                "target_concept_code": self.target.code,
                "target_concept_display": self.target.display,
                "target_concept_system_version_uuid": self.target.terminology_version_uuid,
                "mapping_comments": self.mapping_comments,
                "author": self.author,
                "created_date": datetime.datetime.now(),
            },
        )

        # If auto-advance is on, mark the source_concept as ready for review
        concept_map_settings = (
            ConceptMapSettings.load_by_concept_map_version_uuid_from_cache(
                self.source.concept_map_version_uuid
            )
        )
        if concept_map_settings.auto_advance_after_mapping:
            self.source.update(map_status="ready for review")

    def serialize(self):
        """Prepares a JSON representation of the Mapping instance to return to the API."""
        return {
            "source": self.source.serialize(),
            "relationship": self.relationship.serialize(),
            "target": self.target.serialize(),
            "mapping_comments": self.mapping_comments,
            "author": self.author,
            "uuid": self.uuid,
            "review_status": self.review_status,
        }

    @staticmethod
    def update_relationship_code(mapping_uuid: UUID, new_relationship_code_uuid: UUID):
        """Updates the relationship_code_uuid for the specified mapping_uuid in the database."""
        conn = get_db()
        conn.execute(
            text(
                """  
                UPDATE concept_maps.concept_relationship  
                SET relationship_code_uuid = :new_relationship_code_uuid  
                WHERE uuid = :mapping_uuid;  
                """
            ),
            {
                "mapping_uuid": mapping_uuid,
                "new_relationship_code_uuid": new_relationship_code_uuid,
            },
        )


@dataclass
class MappingSuggestion:
    """A class representing a mapping suggestion."""

    uuid: UUID
    source_concept_uuid: UUID
    code: Code
    suggestion_source: str
    confidence: float
    timestamp: datetime.datetime = None
    accepted: bool = None

    def save(self):
        """Saves the mapping suggestion to the database"""
        conn = get_db()
        conn.execute(
            text(
                """
                insert into concept_maps.suggestion
                (uuid, source_concept_uuid, code, display, terminology_version, suggestion_source, confidence, timestamp)
                values
                (:new_uuid, :source_concept_uuid, :code, :display, :terminology_version, :suggestion_source, :confidence, now())
                """
            ),
            {
                "new_uuid": self.uuid,
                "source_concept_uuid": self.source_concept_uuid,
                "code": self.code.code,
                "display": self.code.display,
                "terminology_version": self.code.terminology_version.uuid,
                "suggestion_source": self.suggestion_source,
                "confidence": self.confidence,
            },
        )

    def serialize(self):
        """Prepares a JSON representation of the mapping suggestion to return to the API."""
        return {
            "uuid": self.uuid,
            "source_concept_uuid": self.source_concept_uuid,
            "code": self.code.serialize(),
            "suggestion_source": self.suggestion_source,
            "confidence": self.confidence,
            "timestamp": self.timestamp,
            "accepted": self.accepted,
        }


# TODO: This is a temporary function to solve a short term problem we have
def transform_struct_string_to_json(struct_string):
    # Parse the coding elements and the text that trails at the end
    # Handle different start/end characters
    if struct_string.startswith("{") and struct_string.endswith("}"):
        # Remove the outer curly braces
        input_str = struct_string[1:-1]
        # Split on '],'
        tuple_str, text_string = input_str.rsplit("],", 1)
        text_string = text_string.strip()
    elif struct_string.startswith("[") and struct_string.endswith("]"):
        # Remove the outer square brackets
        tuple_str = struct_string[1:-1]
        text_string = None
    else:
        raise ValueError("Invalid input string format")

    # Find the tuples using a regular expression
    tuple_pattern = r"\{([^}]+)\}"
    tuple_matches = re.findall(tuple_pattern, tuple_str)

    # Convert each matched tuple into a dictionary
    # We are hard coding added dictionary keys that were lost in the original transformation process
    coding_array = []
    for match in tuple_matches:
        items = match.split(", ")
        d = {
            "code": items[0],
            "display": None
            if len(items) < 2 or items[1] == "null"
            else ", ".join(items[1:-1]),
            "system": items[-1],
        }
        coding_array.append(d)

    result = {"coding": coding_array}

    if text_string is not None:
        result["text"] = text_string

    # Wrapping the result with 'valueCodeableConcept'
    result = {"valueCodeableConcept": result}

    # Convert the dictionary into a JSON string
    return json.dumps(result)


def update_comments_source_concept(source_concept_uuid, comments):
    conn = get_db()
    conn.execute(
        text(
            """
            update concept_maps.source_concept
            set comments=:comments
            where uuid=:source_concept_uuid
            """
        ),
        {"source_concept_uuid": source_concept_uuid, "comments": comments},
    )


def make_author_assigned_mapper(source_concept_uuid, author):
    conn = get_db()
    user_uuid_query = conn.execute(
        text(
            """  
            select uuid from project_management.user  
            where first_last_name = :author
            """
        ),
        {
            "author": author,
        },
    ).first()

    if user_uuid_query:
        assigned_mapper_update = (
            conn.execute(
                text(
                    """  
                update concept_maps.source_concept  
                set assigned_mapper = :user_uuid  
                where uuid = :source_concept_uuid  
                """
                ),
                {
                    "user_uuid": user_uuid_query.uuid,
                    "source_concept_uuid": source_concept_uuid,
                },
            ),
        )
        return "ok"
    else:
        return "Author not found"
