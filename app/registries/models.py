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
    registry_type: str  # not an enum so users can create new types of registries w/o code change
    sorting_enabled: bool

    @classmethod
    def create(cls, title: str, registry_type: str, sorting_enabled: bool):
        conn = get_db()
        registry_uuid = uuid.uuid4()
        conn.execute(
            text(
                """
                insert into flexible_registry.registry
                (uuid, title, registry_type, sorting_enabled)
                values
                (:uuid, :title, :registry_type, :sorting_enabled)
                """
            ),
            {
                "uuid": registry_uuid,
                "title": title,
                "registry_type": registry_type,
                "sorting_enabled": sorting_enabled,
            },
        )
        return cls.load(registry_uuid)

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
            {"registry_uuid": registry_uuid},
        ).fetchone()

        if result is None:
            return None

        return cls(
            uuid=registry_uuid,
            title=result.title,
            registry_type=result.registry_type,
            sorting_enabled=result.sorting_enabled,
        )

    @classmethod
    def load_all_registries(cls) -> List["Registry"]:
        conn = get_db()
        results = conn.execute(
            text(
                """
                select * from flexible_registry.registry
                """
            )
        )

        return [
            cls(
                uuid=result.uuid,
                title=result.title,
                registry_type=result.registry_type,
                sorting_enabled=result.sorting_enabled,
            )
            for result in results
        ]

    def update(self, title=None, sorting_enabled=None, registry_type=None):
        conn = get_db()

        if title is not None:
            conn.execute(
                text(
                    """  
                    UPDATE flexible_registry.registry  
                    SET title=:title  
                    WHERE uuid=:registry_uuid  
                    """
                ),
                {"title": title, "registry_uuid": self.uuid},
            )
            self.title = title

        if sorting_enabled is not None:
            conn.execute(
                text(
                    """  
                    UPDATE flexible_registry.registry  
                    SET sorting_enabled=:sorting_enabled  
                    WHERE uuid=:registry_uuid  
                    """
                ),
                {"sorting_enabled": sorting_enabled, "registry_uuid": self.uuid},
            )
            self.sorting_enabled = sorting_enabled

        if registry_type is not None:
            conn.execute(
                text(
                    """  
                        UPDATE flexible_registry.registry  
                        SET registry_type=:registry_type  
                        WHERE uuid=:registry_uuid  
                        """
                ),
                {"registry_type": registry_type, "registry_uuid": self.uuid},
            )
            self.registry_type = registry_type

    def serialize(self):
        return {
            "uuid": self.uuid,
            "title": self.title,
            "registry_type": self.registry_type,
            "sorting_enabled": self.sorting_enabled,
        }


@dataclass
class Group:
    uuid: uuid.UUID
    registry: Registry
    title: str
    sequence: int

    @classmethod
    def create(
        cls,
        registry_uuid,
        title,
    ):
        conn = get_db()
        group_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                INSERT INTO flexible_registry."group"
                ("uuid", "registry_uuid", "title", "sequence")
                VALUES
                (:uuid, :registry_uuid, :title, (
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
                "title": title,
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
            title=result.title,
            sequence=result.sequence,
            registry=registry,
        )

    def update(self, title):
        conn = get_db()
        if title is not None:
            conn.execute(
                text(
                    """    
                    UPDATE flexible_registry."group"    
                    SET title=:title    
                    WHERE "uuid"=:group_uuid    
                    """
                ),
                {"title": title, "group_uuid": self.uuid},
            )
            self.title = title

    def delete(self):
        conn = get_db()
        conn.execute(
            text(
                """  
                DELETE FROM flexible_registry."group"  
                WHERE "uuid" = :group_uuid  
                """
            ),
            {"group_uuid": self.uuid},
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "registry_uuid": self.registry.uuid,
            "title": self.title,
            "sequence": self.sequence,
        }


@dataclass
class GroupMember:
    uuid: uuid.UUID
    group: Group
    title: str
    sequence: int
    value_set: ValueSet

    @classmethod
    def create(cls, group_uuid, title, value_set_uuid, **kwargs):
        """
        In order to allow effective subclassing, this method will NOT return the created resource.
        Instead, it will call a `post_create_hook` that can be overridden in subclasses to perform additional processing
        and then return the new resource.
        """
        conn = get_db()
        gm_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
                INSERT INTO flexible_registry."group_member"
                ("uuid", "group_uuid", "title", "value_set_uuid", "sequence")
                VALUES
                (:uuid, :group_uuid, :title, :value_set_uuid, (
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
                "title": title,
                "value_set_uuid": value_set_uuid,
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
                "title": result.title,
                "sequence": result.sequence,
                "value_set": ValueSet.load(result.value_set_uuid),
            }
        return None

    def update(self, title=None, value_set_uuid=None):
        conn = get_db()

        if title is not None:
            conn.execute(
                text(
                    """  
                    UPDATE flexible_registry.group_member  
                    SET title=:title  
                    WHERE uuid=:member_uuid  
                    """
                ),
                {"title": title, "member_uuid": self.uuid},
            )
            self.title = title

        if value_set_uuid is not None:
            # Load the new ValueSet instance using the provided value_set_uuid
            new_value_set = ValueSet.load(value_set_uuid)
            if new_value_set is None:
                raise ValueError(f"ValueSet with UUID {value_set_uuid} not found")
            conn.execute(
                text(
                    """  
                    UPDATE flexible_registry.group_member  
                    SET value_set_uuid=:value_set_uuid  
                    WHERE uuid=:member_uuid  
                    """
                ),
                {"value_set_uuid": value_set_uuid, "member_uuid": self.uuid},
            )
            self.value_set = new_value_set

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence=data["sequence"],
            value_set=data["value_set"],
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "group_uuid": self.group.uuid,
            "title": self.title,
            "sequence": self.sequence,
            "value_set_uuid": self.value_set.uuid,
        }


@dataclass
class VitalsGroupMember(GroupMember):
    ucum_ref_units: str
    ref_range_high: str
    ref_range_low: str

    @classmethod
    def create(cls, group_uuid, title, value_set_uuid, **kwargs):
        if "ucum_ref_units" not in kwargs:
            raise BadRequestWithCode(
                "missing-required-param",
                "ucum_ref_units is required to add vital to panel",
            )
        if "ref_range_high" not in kwargs:
            raise BadRequestWithCode(
                "missing-required-param",
                "ref_range_high is required to add vital to panel",
            )
        if "ref_range_low" not in kwargs:
            raise BadRequestWithCode(
                "missing-required-param",
                "ref_range_low is required to add vital to panel",
            )

        super().create(group_uuid, title, value_set_uuid)

    @classmethod
    def post_create_hook(cls, gm_uuid, **kwargs):
        ucum_ref_units = kwargs["ucum_ref_units"]
        ref_range_high = kwargs["ref_range_high"]
        ref_range_low = kwargs["ref_range_low"]

        conn = get_db()
        conn.execute(
            """
            INSERT INTO flexible_registry.vitals
            (group_member_uuid, ucum_ref_units, ref_range_high, ref_range_low)
            values
            (:group_member_uuid, :ucum_ref_units, :ref_range_high, :ref_range_low)
            """,
            {
                "group_member_uuid": gm_uuid,
                "ucum_ref_units": ucum_ref_units,
                "ref_range_high": ref_range_high,
                "ref_range_low": ref_range_low,
            },
        )

        return cls.load(gm_uuid)

    @classmethod
    def fetch_data(cls, uuid):
        data = super().fetch_data(uuid)

        # If there's no data for the given uuid, return None
        if not data:
            return None

        # Load additional data or modify existing data
        conn = get_db()
        result = conn.execute(
            """
            select * from flexible_registry.vitals
            where group_member_uuid=:gm_uuid
            """,
            {"gm_uuid": gm_uuid},
        ).fetchone()
        data["ucum_ref_units"] = result.ucum_ref_units
        data["ref_range_high"] = result.ref_range_high
        data["ref_range_low"] = result.ref_range_low

        return data

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence=data["sequence"],
            value_set=data["value_set"],
            ucum_ref_units=data["ucum_ref_units"],
            ref_range_high=data["ref_range_high"],
            ref_range_low=data["ref_range_low"],
        )

    def serialize(self):
        serialized = super().serialize()
        serialized["ucum_ref_units"] = self.ucum_ref_units
        serialized["ref_range_high"] = self.ref_range_high
        serialized["ref_range_low"] = self.ref_range_low
        return serialized


@dataclass
class LabGroupMember(GroupMember):
    minimum_panel_members: int

    @classmethod
    def create(cls, group_uuid, title, value_set_uuid, **kwargs):
        if "minimum_panel_members" not in kwargs:
            raise BadRequestWithCode(
                "missing-required-param",
                "minimum_panel_members is required to add lab to panel",
            )

        super().create(group_uuid, title, value_set_uuid)

    @classmethod
    def post_create_hook(cls, gm_uuid, **kwargs):
        minimum_panel_members = kwargs["minimum_panel_members"]

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
                "minimum_panel_members": minimum_panel_members,
            },
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
            {"gm_uuid": gm_uuid},
        ).fetchone()
        data["minimum_panel_members"] = result.minimum_panel_members

        return data

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence=data["sequence"],
            value_set=data["value_set"],
            minimum_panel_members=data["minimum_panel_members"],
        )

    def serialize(self):
        serialized = super().serialize()
        serialized["minimum_panel_members"] = self.minimum_panel_members
        return serialized
