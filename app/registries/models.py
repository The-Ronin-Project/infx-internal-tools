from datetime import datetime
import uuid
from typing import List, Dict, Union, Optional
from dataclasses import dataclass

from sqlalchemy import text
from functools import lru_cache
import app.models.codes
from app.database import get_db
from app.errors import BadRequestWithCode
from app.value_sets.models import ValueSet

# Need reference ranges for labs and vitals

@dataclass
class Registry:
    uuid: uuid.UUID
    title: str
    data_type: str #todo: likely this should be an enum


@dataclass
class Group:
    uuid: uuid.UUID
    title: str
    sequence_num: int


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
        group_member_type,
        product_element_type_label,
        sequence,
        value_set_uuid,
        value_set_version_uuid,
    ):
        conn = get_db()  # Assuming you have a get_db method like in your provided code
        gm_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                INSERT INTO flexible_registry."group_member"
                ("uuid", "group_uuid", "group_member_type", "product_element_type_label", "sequence", "value_set_uuid", "value_set_version_uuid")
                VALUES
                (:uuid, :group_uuid, :group_member_type, :product_element_type_label, :sequence, :value_set_uuid, :value_set_version_uuid)
                """
            ),
            {
                "uuid": gm_uuid,
                "group_uuid": group_uuid,
                "group_member_type": group_member_type,
                "product_element_type_label": product_element_type_label,
                "sequence": sequence,
                "value_set_uuid": value_set_uuid,
                "value_set_version_uuid": value_set_version_uuid,
            },
        )

        return cls.load(gm_uuid)

    @classmethod
    def load(cls, uuid):
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
            return cls(
                uuid=result["uuid"],
                group_uuid=result["group_uuid"],
                group_member_type=result["group_member_type"],
                product_element_type_label=result["product_element_type_label"],
                sequence=result["sequence"],
                value_set_uuid=result["value_set_uuid"],
                value_set_version_uuid=result["value_set_version_uuid"],
            )
        else:
            return None

    def serialize(self):
        return {
            "uuid": self.uuid,
            "group_uuid": self.group_uuid,
            "group_member_type": self.group_member_type,
            "product_element_type_label": self.product_element_type_label,
            "sequence": self.sequence,
            "value_set_uuid": self.value_set_uuid,
            "value_set_version_uuid": self.value_set_version_uuid,
        }