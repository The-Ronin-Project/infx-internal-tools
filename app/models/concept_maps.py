import datetime
import uuid
import functools
import json
import app.models.codes

from decouple import config
from werkzeug.exceptions import BadRequest
from sqlalchemy import text
from dataclasses import dataclass
from uuid import UUID
from typing import Optional
from app.database import get_db
from app.models.codes import Code
from app.models.terminologies import Terminology
from app.helpers.oci_auth import oci_authentication
from app.helpers.db_helper import db_cursor
from elasticsearch import TransportError
from numpy import source


# This is from when we used `scrappyMaps`. It's used for mapping inclusions and can be removed as soon as that has been ported to the new maps.
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


# This is the new maps system
class ConceptMap:
    def __init__(self, uuid):
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

        self.load_data()

    def load_data(self):
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
        self.most_recent_active_version = ConceptMapVersion(
            version.uuid, concept_map=self
        )

    @staticmethod
    def new_version_from_previous(
        previous_version_uuid,
        new_version_description,
        new_version_num,
        new_source_value_set_version_uuid,
        new_target_value_set_version_uuid,
    ):
        conn = get_db()
        new_version_uuid = uuid.uuid4()
        print("New Version UUID", new_version_uuid)

        # Lookup concept_map_uuid
        concept_map_uuid = (
            conn.execute(
                text(
                    """
                select * from concept_maps.concept_map_version
                where uuid=:previous_version_uuid
                """
                ),
                {"previous_version_uuid": previous_version_uuid},
            )
            .first()
            .concept_map_uuid
        )

        # Add entry to concept_maps.concept_map_version
        conn.execute(
            text(
                """
                insert into concept_maps.concept_map_version
                (uuid, concept_map_uuid, description, status, created_date, version, source_value_set_version_uuid, target_value_set_version_uuid)
                values
                (:new_version_uuid, :concept_map_uuid, :description, 'pending', now(), :version_num, :source_value_set_version_uuid, :target_value_set_version_uuid)
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                "concept_map_uuid": concept_map_uuid,
                "description": new_version_description,
                "version_num": new_version_num,
                "source_value_set_version_uuid": new_source_value_set_version_uuid,
                "target_value_set_version_uuid": new_target_value_set_version_uuid,
            },
        )
        # Populate concept_maps.source_concept
        conn.execute(
            text(
                """
                insert into concept_maps.source_concept
                (uuid, code, display, system, map_status, concept_map_version_uuid)
                select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid from value_sets.expansion_member
                join value_sets.expansion
                on expansion.uuid=expansion_member.expansion_uuid
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where vs_version_uuid=:new_source_value_set_version_uuid
                """
            ),
            {
                "new_source_value_set_version_uuid": new_source_value_set_version_uuid,
                "concept_map_version_uuid": new_version_uuid,
            },
        )

        # Load new target value set
        target_value_set_expansion = conn.execute(
            text(
                """
                select expansion_member.*, tv.uuid as terminology_uuid from value_sets.expansion_member
                join value_sets.expansion
                on expansion.uuid=expansion_member.expansion_uuid
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where vs_version_uuid=:vs_version_uuid
                """
            ),
            {"vs_version_uuid": new_target_value_set_version_uuid},
        )
        target_value_set_lookup = {
            (x.code, x.display, x.system): x for x in target_value_set_expansion
        }

        # Iterate through source_concepts in new version
        previous_concept_map_version = ConceptMapVersion(previous_version_uuid)
        exact_previous_mappings = previous_concept_map_version.mappings
        code_display_system_previous_mappings = {
            (key.code, key.display, key.system): value
            for key, value in exact_previous_mappings.items()
        }

        new_source_concepts = conn.execute(
            text(
                """
                select tv.fhir_uri, tv.version terminology_version, source_concept.uuid as source_concept_uuid, * from concept_maps.source_concept
                join public.terminology_versions tv
                on tv.uuid = cast(source_concept.system as uuid)
                where concept_map_version_uuid = :new_version_uuid
                """
            ),
            {"new_version_uuid": new_version_uuid},
        )
        for item in new_source_concepts:
            source_code = Code.load_concept_map_source_concept(item.source_concept_uuid)

            if (
                item.code,
                item.display,
                item.fhir_uri,
            ) in code_display_system_previous_mappings:
                mappings = code_display_system_previous_mappings[
                    (item.code, item.display, item.fhir_uri)
                ]

                for mapping in mappings:
                    target_code = mapping.target

                    # See if the target from the old mapping is in the new target value set or not
                    if (
                        target_code.code,
                        target_code.display,
                        target_code.system,
                    ) in target_value_set_lookup:
                        # A match was found, copy the mapping over
                        target_info = target_value_set_lookup.get(
                            (target_code.code, target_code.display, target_code.system)
                        )

                        target_code = Code(
                            code=target_info.code,
                            display=target_info.display,
                            system=None,
                            version=None,
                            terminology_version=target_info.terminology_uuid,
                        )

                        new_mapping = Mapping(
                            source=source_code,
                            relationship=mapping.relationship,
                            target=target_code,
                            mapping_comments=mapping.mapping_comments,
                            author=mapping.author,
                            review_status=mapping.review_status,
                        )
                        new_mapping.save()

    @classmethod
    def concept_map_metadata(cls, cm_uuid):
        """
        This function executes a sql query to get the concept map based on the uuid passed in.
        @param cm_uuid: concept map uuid
        @return: tuple of metadata related to the given concept map uuid
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
        This function creates a brand new concept map and concept map version 1 and inserts the source concept value set version codes, displays and systems into the source concept table.
        @param name: string concept map name
        @param title: string concept map title
        @param publisher: string hard coded Project Ronin
        @param author: string auto fill as retool current user
        @param use_case_uuid: uuid use case
        @param cm_description: string concept map description
        @param experimental: boolean
        @param source_value_set_uuid: uuid value set
        @param target_value_set_uuid: uuid value set
        @param cm_version_description: string concept map version description
        @param source_value_set_version_uuid: uuid value set version
        @param target_value_set_version_uuid: uuid value set version
        @return: tuple of metadata related to the given concept map uuid
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
        ex_members = cls.get_source_concepts_for_mapping(source_value_set_version_uuid)
        cls.insert_source_concepts_for_mapping(ex_members, cm_version_uuid)
        return cls.concept_map_metadata(cm_uuid)

    @classmethod
    def insert_source_concepts_for_mapping(
        cls, cmv_uuid, source_value_set_version_uuid
    ):
        """
        This function gets and inserts the codes, displays and systems from the source value set version AND a concept map version uuid, into the source_concept table for mapping.
        @param cmv_uuid: uuid concept map version
        @param source_value_set_version_uuid: uuid source value set version
        @return:none, the items are simply inserted into the concept_maps.source_concepts table
        """
        conn = get_db()
        # Populate concept_maps.source_concept
        conn.execute(
            text(
                """
                insert into concept_maps.source_concept
                (uuid, code, display, system, map_status, concept_map_version_uuid)
                select uuid_generate_v4(), code, display, tv.uuid, 'pending', :concept_map_version_uuid from value_sets.expansion_member
                join value_sets.expansion
                on expansion.uuid=expansion_member.expansion_uuid
                join public.terminology_versions tv
                on tv.fhir_uri=expansion_member.system
                and tv.version=expansion_member.version
                where vs_version_uuid=:source_value_set_version_uuid
                """
            ),
            {
                "source_value_set_version_uuid": source_value_set_version_uuid,
                "concept_map_version_uuid": cmv_uuid,
            },
        )

    def serialize(self):
        """
        This function serializes a concept map object
        @return: Serialized concept map metadata
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
        }


class ConceptMapVersion:
    def __init__(self, uuid, concept_map=None):
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

        self.load_data(concept_map=concept_map)

    def load_data(self, concept_map=None):
        """
        runs sql query to return all information related to specified concept map version, data returned is used to
        set class attributes
        @rtype: object
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
        self.load_allowed_target_terminologies()
        self.load_mappings()
        self.generate_self_mappings()

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

    def generate_self_mappings(self):
        """
        if self_map flag in db is true, generate the self_mappings here
        @rtype: mappings dictionary
        """
        if self.concept_map.include_self_map is True:
            for target_terminology in self.allowed_target_terminologies:
                target_terminology.load_content()
                for code in target_terminology.codes:
                    self.mappings[code] = [
                        Mapping(
                            source=code,
                            relationship=MappingRelationship.load_by_code("equivalent"),
                            target=code,  # self is equivalent to self
                            mapping_comments="Auto-generated self map",
                        )
                    ]

    def load_mappings(self):
        conn = get_db()
        query = """
            select concept_relationship.uuid as mapping_uuid, concept_relationship.author, concept_relationship.review_status, concept_relationship.mapping_comments,
            source_concept.code as source_code, source_concept.display as source_display, source_concept.system as source_system, 
            tv_source.version as source_version, tv_source.fhir_uri as source_fhir_uri,
            relationship_codes.code as relationship_code, source_concept.map_status,
            concept_relationship.target_concept_code, concept_relationship.target_concept_display,
            concept_relationship.target_concept_system_version_uuid as target_system,
            tv_target.version as target_version, tv_target.fhir_uri as target_fhir_uri
            from concept_maps.source_concept
            left join concept_maps.concept_relationship
            on source_concept.uuid = concept_relationship.source_concept_uuid
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
            source_code = Code(
                item.source_fhir_uri,
                item.source_version,
                item.source_code,
                item.source_display,
            )
            target_code = Code(
                item.target_fhir_uri,
                item.target_version,
                item.target_concept_code,
                item.target_concept_display,
            )
            relationship = MappingRelationship.load_by_code(
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
            )
            if source_code in self.mappings:
                self.mappings[source_code].append(mapping)
            else:
                self.mappings[source_code] = [mapping]

    def serialize_mappings(self):
        # Identify all the source terminology / target terminology pairings in the mappings
        source_target_pairs_set = set()

        for source_code, mappings in self.mappings.items():
            source_uri = source_code.system
            source_version = source_code.version
            for mapping in mappings:
                target_uri = mapping.target.system
                target_version = mapping.target.version

                source_target_pairs_set.add(
                    (source_uri, source_version, target_uri, target_version)
                )

        # Serialize the mappings
        groups = []

        for (
            source_uri,
            source_version,
            target_uri,
            target_version,
        ) in source_target_pairs_set:
            elements = []
            for source_code, mappings in self.mappings.items():
                if (
                    source_code.system == source_uri
                    and source_code.version == source_version
                ):
                    filtered_mappings = [
                        x
                        for x in mappings
                        if x.target.system == target_uri
                        and x.target.version == target_version
                    ]
                    elements.append(
                        {
                            "code": source_code.code.rstrip(),
                            "display": source_code.display.rstrip(),
                            "target": [
                                {
                                    "code": mapping.target.code,
                                    "display": mapping.target.display,
                                    "equivalence": mapping.relationship.code,
                                    "comment": None,
                                }
                                for mapping in filtered_mappings
                            ],
                        }
                    )

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
        results = conn.execute(
            text(
                """select description from project_management.use_case
                where uuid=:uuid"""
            ),
            {
                "uuid": self.concept_map.use_case_uuid,
            },
        ).first()
        return results.description

    def serialize(self):
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
        return {
            "resourceType": "ConceptMap",
            "title": self.concept_map.title,
            "id": self.uuid,
            "name": self.concept_map.name,
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
                    "url": "http://projectronin.io/fhir/ronin.common-fhir-model.uscore-r4/StructureDefinition/Extension/ronin-ConceptMapSchema",
                    "valueString": "1.0.0",
                }
            ]
            # For now, we are intentionally leaving out created_dates as they are not part of the FHIR spec and
            # are not required for our use cases at this time
        }

    def pre_export_validate(self):
        if self.pre_export_validate is False:
            raise BadRequest(
                "Concept map cannot be published because it failed validation"
            )

    @staticmethod
    def folder_path_for_oci(folder, concept_map, path):
        """
        This function creates the oci folder path based on folder given (prerelease or published) - prerelease includes
        an utc timestamp appended at the end as there can be multiple versions in prerelease
        @param folder: destination folder (prerelease or published)
        @param concept_map: concept map object
        @param path: string path - complete folder path location
        @return: string of folder path
        """
        if folder == "prerelease":
            path = (
                path
                + f"/{concept_map['version']}_{datetime.datetime.utcnow().strftime('%Y%m%d-%H%M')}.json"
            )
            return path
        if folder == "published":
            path = path + f"/{concept_map['version']}.json"
            return path

    @staticmethod
    def check_for_prerelease_in_published(
        path, object_storage_client, bucket_name, namespace, concept_map
    ):
        """
        This function changes the folder path to reflect published if the folder passed was prerelease.  We do this
        specifically to check if a PRERELEASE concept map is already in the PUBLISHED FOLDER
        @param path: complete string folder path for the concept map
        @param object_storage_client: oci client to check for file existence
        @param bucket_name: bucket for oci - most cases 'infx-shared'
        @param namespace: oci namespaced for infx bucket
        @param concept_map: concept map object - used here to get the version appended to the folder path
        @return: True or False depending on if the file exists in the published folder
        """
        published_path = path.replace("prerelease", "published")
        path_to_check = published_path + f"/{concept_map['version']}.json"
        exists_in_published = ConceptMapVersion.folder_in_bucket(
            path_to_check, object_storage_client, bucket_name, namespace
        )
        return exists_in_published

    @staticmethod
    def set_up_object_store(concept_map, folder):
        """
        This function is the conditional matrix for saving a concept map to oci.  The function LOOKS
        to see if the concept map already exists and LOOKS to see where it should be saved.
        @param concept_map: concept map object - used to retrieve version and correct folder name from url
        @param folder: string folder destination (prerelease or published)
        @return: concept map if saved to oci, otherwise messages returned based on findings
        """
        object_storage_client = oci_authentication()
        concept_map_uuid = concept_map["url"].rsplit("/", 1)[1]
        if concept_map["status"] == "active" or concept_map["status"] == "in progress":
            path = f"ConceptMaps/v1/{folder}/{concept_map_uuid}"
        else:
            raise BadRequest(
                "Concept map cannot be saved in object store, status must be either active or in progress."
            )
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data

        # if folder is prerelease check if file exists in PUBLISHED folder
        if folder == "prerelease":
            pre_in_pub = ConceptMapVersion.check_for_prerelease_in_published(
                path, object_storage_client, bucket_name, namespace, concept_map
            )
            if pre_in_pub:
                return {"message": "concept map is already in the published bucket"}
        folder_exists = ConceptMapVersion.folder_in_bucket(
            path, object_storage_client, bucket_name, namespace
        )
        if not folder_exists:
            del concept_map["status"]
            path = ConceptMapVersion.folder_path_for_oci(folder, concept_map, path)
            ConceptMapVersion.save_to_object_store(
                path, object_storage_client, bucket_name, namespace, concept_map
            )
            return concept_map
        elif folder_exists:
            del concept_map["status"]
            path = ConceptMapVersion.folder_path_for_oci(folder, concept_map, path)
            if folder == "prerelease":
                ConceptMapVersion.save_to_object_store(
                    path, object_storage_client, bucket_name, namespace, concept_map
                )
                return concept_map
            if folder == "published":
                version_exist = ConceptMapVersion.folder_in_bucket(
                    path, object_storage_client, bucket_name, namespace
                )
                if version_exist:
                    return {"message": "concept map already in bucket"}
                else:
                    ConceptMapVersion.save_to_object_store(
                        path, object_storage_client, bucket_name, namespace, concept_map
                    )
                return concept_map

    @staticmethod
    def folder_in_bucket(path, object_storage_client, bucket_name, namespace):
        """
        This function will check if a specified folder/file already exists in oci
        @param path: string path of folder
        @param object_storage_client: oci client
        @param bucket_name: bucket for oci - most cases 'infx-shared'
        @param namespace: oci namespaced for infx bucket
        @return: True or False depending on if the file exists in the published folder
        """
        object_list = object_storage_client.list_objects(namespace, bucket_name)
        exists = [x for x in object_list.data.objects if path in x.name]
        return True if exists else False

    @staticmethod
    def save_to_object_store(
        path, object_storage_client, bucket_name, namespace, concept_map
    ):
        """
        This function saves the given concept map to the oci infx-shared bucket based on the folder path given
        @param path: string path for folder
        @param object_storage_client: oci client
        @param bucket_name: bucket for oci - most cases 'infx-shared'
        @param namespace: oci namespaced for infx bucket
        @param concept_map: concept map object - used here to get the version appended to the folder path
        @return: completion message and concept map
        """
        object_storage_client.put_object(
            namespace,
            bucket_name,
            path,
            json.dumps(concept_map, indent=2).encode("utf-8"),
        )
        return {"message": "concept map pushed to bucket", "object": concept_map}

    @staticmethod
    @db_cursor
    def version_set_status_active(conn, version_uuid):
        """
        This function updates the status of the concept map version to "active" and inserts the publication date as now
        @param conn: db_cursor wrapper function to create connection to sql db
        @param version_uuid: UUID; concept map version used to set status in pgAdmin
        @return: result from query
        """
        data = conn.execute(
            text(
                """
                UPDATE concept_maps.concept_map_version
                SET status='active', published_date=:published_date
                WHERE uuid=:version_uuid
                """
            ),
            {"version_uuid": version_uuid, "published_date": datetime.datetime.now()},
        )
        return data

    @staticmethod
    @db_cursor
    def get_concept_map_from_db(conn, version_uuid):
        """
        This function runs the below sql query to get the overall concept map uuid and version for use in searching
        oci storage, sql query returns most recent version
        @param conn: db_cursor wrapper function to create connection to sql db
        @param version_uuid: UUID; concept map version used to retrieve overall concept uuid
        @return: dictionary containing overall concept map uuid and version
        """
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version
                where uuid=:version_uuid order by version desc 
                """
            ),
            {"version_uuid": version_uuid},
        ).first()
        if data is None:
            return False

        result = dict(data)
        return {"folder_name": result["concept_map_uuid"], "version": result["version"]}

    @staticmethod
    def get_concept_map_from_object_store(concept_map, folder):
        """
        This function gets the requested concept from oci storage
        @param concept_map: concept map object used to get the most recent version
        @param folder: string path to look for in oci
        @return: json of concept map found in oci storage
        """
        object_storage_client = oci_authentication()
        bucket_name = config("OCI_CLI_BUCKET")
        namespace = object_storage_client.get_namespace().data
        path = f"ConceptMaps/v1/{folder}/{str(concept_map['folder_name'])}/{concept_map['version']}.json"
        try:
            concept_map_found = object_storage_client.get_object(
                namespace, bucket_name, path
            )
        except:
            return {"message": f"{path} not found."}
        return concept_map_found.data.json()


@dataclass
class MappingRelationship:
    uuid: UUID
    code: str
    display: str

    @classmethod
    def load(cls, uuid):
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
    @functools.lru_cache(maxsize=32)
    def load_by_code(cls, code):
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

    def serialize(self):
        return {"uuid": self.uuid, "code": self.code, "display": self.display}


@dataclass
class Mapping:
    source: Code
    relationship: MappingRelationship
    target: Code
    mapping_comments: Optional[str] = None
    author: Optional[str] = None
    uuid: Optional[UUID] = None
    cursor: Optional[None] = None
    review_status: str = "ready for review"

    def __post_init__(self):
        self.cursor = get_db()
        self.uuid = uuid.uuid4()

    @classmethod
    def load(cls, uuid):
        pass

    def save(self):
        self.cursor.execute(
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
                "target_concept_system_version_uuid": self.target.terminology_version,
                "mapping_comments": self.mapping_comments,
                "author": self.author,
                "created_date": datetime.datetime.now(),
            },
        )

    def serialize(self):
        return {
            "source": self.source.serialize(),
            "relationship": self.relationship.serialize(),
            "target": self.target.serialize(),
            "mapping_comments": self.mapping_comments,
            "author": self.author,
            "uuid": self.uuid,
            "review_status": self.review_status,
        }


@dataclass
class MappingSuggestion:
    uuid: UUID
    source_concept_uuid: UUID
    code: Code
    suggestion_source: str
    confidence: float
    timestamp: datetime.datetime = None
    accepted: bool = None

    def save(self):
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
        return {
            "uuid": self.uuid,
            "source_concept_uuid": self.source_concept_uuid,
            "code": self.code.serialize(),
            "suggestion_source": self.suggestion_source,
            "confidence": self.confidence,
            "timestamp": self.timestamp,
            "accepted": self.accepted,
        }
