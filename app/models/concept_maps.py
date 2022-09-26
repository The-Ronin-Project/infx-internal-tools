import datetime
import uuid
import functools
import os
import io
import app.models.terminologies
import app.models.codes

from sqlalchemy import text
from dataclasses import dataclass
from uuid import UUID
from typing import Optional
from app.database import get_db
from app.models.codes import Code
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
        self.purpose = None
        self.publisher = None
        self.experimental = None
        self.author = None
        self.created_date = None
        self.include_self_map = None

        self.load_data()

    def load_data(self):
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
        self.purpose = data.purpose
        self.publisher = data.publisher
        self.experimental = data.experimental
        self.author = data.author
        self.created_date = data.created_date
        self.include_self_map = data.include_self_map


class ConceptMapVersion:
    def __init__(self, uuid):
        self.uuid = uuid
        self.concept_map = None
        self.description = None
        self.comments = None
        self.status = None
        self.created_date = None
        self.effective_start = None
        self.effective_end = None
        self.published_date = None
        self.version = None
        self.allowed_target_terminologies = []
        self.mappings = {}
        self.url = None

        self.load_data()

    def load_data(self):
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

        self.concept_map = ConceptMap(data.concept_map_uuid)
        self.description = data.description
        self.comments = data.comments
        self.status = data.status
        self.created_date = data.created_date
        self.effective_start = data.effective_start
        self.effective_end = data.effective_end
        self.version = data.version
        self.published_date = data.published_date

        self.load_allowed_target_terminologies()
        self.load_mappings()
        self.generate_self_mappings()

    def load_allowed_target_terminologies(self):
        conn = get_db()
        data = conn.execute(
            text(
                """
                select * from concept_maps.concept_map_version_terminologies
                where concept_map_version_uuid=:concept_map_version_uuid
                and context='target_terminology'
                """
            ), {
                'concept_map_version_uuid': self.uuid
            }
        )
        for item in data:
            terminology_version_uuid = item.terminology_version_uuid
            self.allowed_target_terminologies.append(
                app.models.terminologies.Terminology.load(terminology_version_uuid)
            )

    def generate_self_mappings(self):
        if self.concept_map.include_self_map is True:
            for target_terminology in self.allowed_target_terminologies:
                target_terminology.load_content()
                for code in target_terminology.codes:
                    self.mappings[code] = [Mapping(
                        source=code,
                        relationship=MappingRelationship.load_by_code('equivalent'),
                        target=code,  # self is equivalent to self
                        mapping_comments="Auto-generated self map",
                    )]


    def load_mappings(self):
        conn = get_db()
        query = """
            select source_concept.code as source_code, source_concept.display as source_display, source_concept.system as source_system, 
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
            relationship = MappingRelationship.load_by_code(item.relationship_code) # this needs optimization

            mapping = Mapping(
                source_code,
                relationship,
                target_code)
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
                            "code": source_code.code,
                            "display": source_code.display,
                            "target": [
                                {
                                    "code": mapping.target.code,
                                    "display": mapping.target.display,
                                    "equivalence": mapping.relationship.code,

                                }
                                for mapping in filtered_mappings]

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

    def serialize(self):
        combined_description = (
                str(self.concept_map.description)
                + " Version-specific notes:"
                + str(self.description)
        )

        return {

            'resourceType': 'ConceptMap',
            'title': self.concept_map.title,
            'id': self.uuid,
            'name': self.concept_map.name,
            'contact': [{'name': self.concept_map.author}],
            'url': f'http://projectronin.com/fhir/us/ronin/ConceptMap/{self.concept_map.uuid}',
            'description': self.concept_map.description,
            'purpose': self.concept_map.purpose,
            'publisher': self.concept_map.publisher,
            'experimental': self.concept_map.experimental,
            'status': self.status,
            'date': self.published_date.strftime('%Y-%m-%d'),
            'version': self.version,
            'group': self.serialize_mappings()
            # For now, we are intentionally leaving out created_dates as they are not part of the FHIR spec and not required for our use cases at this time
        }



    def pre_export_validate(self):
        pass
        if self.pre_export_validate is False:
            raise BadRequest('Concept map cannot be published because it failed validation')
        pass

    def save_in_object_store(self):
        object_storage_client = oci_authentication
        # authenticate
        # check status to direct to appropriate folder: list_objects(namespace_name, bucket_name)
        path = 'Concept Maps/v1'
        for status in self.mappings.items():
            if status is 'active':
                path += f'/published/{self.concept_map.uuid}'
            elif status is 'in progress':
                path += f'/prerelease/{self.concept_map.uuid}'
            else:
                raise BadRequest(
                    'Concept map cannot be saved in object store, status must be either active or in progress.'
                )


        # check if concept_map_uuid folder exists
        # if no uuid folder, create uuid folder
        # if yes, check for version file
        # if no version file, follow steps below
        # schema property with a value of 1.0.0
        # strip status before save
        # save object in said uuid folder

        files_to_process = 'concept_map that we want to upload'
        bucket_name = 'infx-shared'
        namespace = object_storage_client.get_namespace().data   # object_storage_client = ObjectStorageClient(config) <-- oci_auth file
        for upload_file in files_to_process:
            print(f'Uploading file {upload_file}')
            object_storage_client.put_object(namespace, bucket_name, upload_file,
                                             io.open(os.path.join(directory, upload_file), 'r'))
        return 'done'


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
            {'code': code}
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
