from datetime import datetime
import uuid
from typing import List, Dict, Union, Optional
from dataclasses import dataclass

from sqlalchemy import text
from functools import lru_cache
import app.models.codes
from app.database import get_db
from app.errors import BadRequestWithCode
from app.value_sets.models import ValueSet, ValueSetVersion

# Need reference ranges for labs and vitals


@dataclass
class Registry:
    uuid: uuid.UUID
    title: str
    registry_type: str  # todo: likely this should be an enum

    @classmethod
    def load(cls, registry_uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                select * from flexible_registry.registry
                where uuid=:registry_uuid
                """
            ),
            {
                "registry_uuid": registry_uuid
            }
        ).fetchone()

        if result is None:
            return None

        return cls(
            uuid=registry_uuid,
            title=result.title,
            registry_type=result.registry_type
        )


@dataclass
class Group:
    uuid: uuid.UUID
    registry: Registry
    title: str
    sequence_num: int

    @classmethod
    def create(
            cls,
            registry_uuid,
            product_group_title,
               ):
        conn = get_db()
        group_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                INSERT INTO flexible_registry."group"
                ("uuid", "registry_uuid", "product_group_title", "sequence")
                VALUES
                (:uuid, :registry_uuid, :product_group_title, (
                        SELECT COALESCE(MAX(sequence), 0) + 1
                        FROM flexible_registry.group
                        WHERE registry_uuid = :registry_uuid
                    )
                )
                """
            ),
            {
                "uuid": group_uuid,
                "registry_uuid": registry_uuid,
                "product_group_title": product_group_title,
            },
        )

        return cls.load(group_uuid)

    @classmethod
    def load(cls, group_uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                SELECT * FROM flexible_registry."group"
                WHERE "uuid" = :group_uuid
                """
            ),
            {"group_uuid": group_uuid},
        ).fetchone()

        registry = Registry.load(result.registry_uuid)

        return cls(
            uuid=result.uuid,
            title=result.product_group_title,
            sequence_num=result.sequence,
            registry=registry
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "registry_uuid": self.registry.uuid,
            "product_group_title": self.title,
            "sequence": self.sequence_num,
        }


@dataclass
class GroupMember:
    uuid: uuid.UUID
    group: Group
    title: str
    sequence_num: int
    value_set: ValueSet

    @classmethod
    def create(
        cls,
        group_uuid,
        product_title,
        value_set_uuid,
        **kwargs
    ):
        conn = get_db()
        gm_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                INSERT INTO flexible_registry."group_member"
                ("uuid", "group_uuid", "product_title", "value_set_uuid", "sequence")
                VALUES
                (:uuid, :group_uuid, :product_title, :value_set_uuid, (
                        SELECT COALESCE(MAX(sequence), 0) + 1
                        FROM flexible_registry.group_member
                        WHERE group_uuid = :group_uuid
                    )
                )
                """
            ),
            {
                "uuid": gm_uuid,
                "group_uuid": group_uuid,
                "product_title": product_title,
                "value_set_uuid": value_set_uuid
            },
        )

        return cls.post_create_hook(gm_uuid, **kwargs)

    @classmethod
    def post_create_hook(cls, gm_uuid, **kwargs):
        """
        To be overridden in subclasses to save additional data to accessory tables
        """
        return cls.load(gm_uuid)

    @classmethod
    def load(cls, uuid):
        data = cls.fetch_data(uuid)
        if not data:
            return None
        return cls.create_instance_from_data(**data)

    @classmethod
    def fetch_data(cls, uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                SELECT * FROM flexible_registry."group_member"
                WHERE "uuid" = :uuid
                """
            ),
            {"uuid": uuid},
        ).fetchone()

        if result:
            return {
                "uuid": result.uuid,
                "group": Group.load(result.group_uuid),
                "title": result.product_title,
                "sequence_num": result.sequence,
                "value_set": ValueSet.load(result.value_set_uuid),
            }
        return None

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence_num=data["sequence_num"],
            value_set=data["value_set"],
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "group_uuid": self.group.uuid,
            "product_title": self.title,
            "sequence": self.sequence_num,
            "value_set_uuid": self.value_set.uuid,
        }


@dataclass
class LabGroupMember(GroupMember):
    minimum_panel_members: int

    @classmethod
    def create(
        cls,
        group_uuid,
        product_title,
        value_set_uuid,
        **kwargs
    ):
        if 'minimum_panel_members' not in kwargs:
            raise BadRequestWithCode('missing-required-param', "minimum_panel_members is required to add lab to panel")

        super().create(group_uuid, product_title, value_set_uuid)

    @classmethod
    def post_create_hook(cls, gm_uuid, **kwargs):
        minimum_panel_members = kwargs['minimum_panel_members']

        conn = get_db()
        conn.execute(
            """
            INSERT INTO flexible_registry.labs
            (group_member_uuid, minimum_panel_members)
            values
            (:group_member_uuid, :minimum_panel_members)
            """,
            {
                "group_member_uuid": gm_uuid,
                "minimum_panel_members": minimum_panel_members
            }
        )

        return cls.load(gm_uuid)

    @classmethod
    def fetch_data(cls, gm_uuid):
        data = super().fetch_data(uuid)

        # If there's no data for the given uuid, return None
        if not data:
            return None

        # Load additional data or modify existing data
        conn = get_db()
        result = conn.execute(
            """
            select * from flexible_registry.labs
            where group_member_uuid=:gm_uuid
            """,
            {
                "gm_uuid": gm_uuid
            }
        ).fetchone()
        data['minimum_panel_members'] = result.minimum_panel_members

        return data

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence_num=data["sequence_num"],
            value_set=data["value_set"],
            minimum_panel_members=data["minimum_panel_members"]
        )

    def serialize(self):
        serialized = super().serialize()
        serialized['minimum_panel_members'] = self.minimum_panel_members
        return serialized
