from datetime import datetime
import uuid
from typing import List, Dict, Union, Optional
from dataclasses import dataclass

from sqlalchemy import text
from functools import lru_cache
import app.models.codes
from app.database import get_db
from app.errors import BadRequestWithCode, NotFoundException
from app.value_sets.models import ValueSet, ValueSetVersion

# Need reference ranges for labs and vitals


@dataclass
class Registry:
    uuid: uuid.UUID
    title: str
    registry_type: str  # not an enum so users can create new types of registries w/o code change
    sorting_enabled: bool

    def __post_init__(self):
        self.groups = []

    @classmethod
    def create(cls, title: str, registry_type: str, sorting_enabled: bool):
        conn = get_db()
        # Check for duplicate registry names
        existing_registry = conn.execute(
            text(
                """  
                SELECT * FROM flexible_registry.registry  
                WHERE title = :title  
                """
            ),
            {"title": title},
        ).fetchone()

        if existing_registry:
            raise BadRequestWithCode(
                "Registry.title.duplicate",
                "Cannot create a new registry with the same title as an existing one",
            )

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
            raise NotFoundException(f"No Registry found with UUID: {registry_uuid}")

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

    def load_groups(self):
        conn = get_db()
        results = conn.execute(
            text(
                """  
                SELECT * FROM flexible_registry."group"  
                WHERE registry_uuid = :registry_uuid
                order by sequence  
                """
            ),
            {"registry_uuid": self.uuid},
        ).fetchall()

        if results is None:
            raise NotFoundException(
                f"No Groups found for Registry with UUID: {self.uuid}"
            )

        groups = []
        for result in results:
            if self.registry_type == "labs":
                group = LabsGroup.load(result.uuid)
            else:
                group = Group(
                    uuid=result.uuid,
                    registry=self,
                    title=result.title,
                    sequence=result.sequence,
                )
            groups.append(group)

        self.groups = groups

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
    def create(cls, registry_uuid, title, **kwargs):
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

        return cls.post_create_hook(group_uuid, **kwargs)

    @classmethod
    def post_create_hook(cls, group_uuid, **kwargs):
        """
        To be overridden in subclasses to save additional data to accessory tables
        """
        return cls.load(group_uuid)

    @classmethod
    def load(cls, uuid):
        # todo: identify and instantiate the correct type of Group
        data = cls.fetch_data(uuid)
        return cls.create_instance_from_data(**data)

    @classmethod
    def fetch_data(cls, uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                SELECT * FROM flexible_registry."group"
                WHERE "uuid" = :group_uuid
                """
            ),
            {"group_uuid": uuid},
        ).fetchone()

        if result is None:
            raise NotFoundException(f"No Group found with UUID: {uuid}")

        registry = Registry.load(result.registry_uuid)

        return {
            "uuid": result.uuid,
            "title": result.title,
            "sequence": result.sequence,
            "registry": registry,
        }

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            title=data["title"],
            sequence=data["sequence"],
            registry=data["registry"],
        )

    def load_members(self):
        group_uuid = self.uuid
        conn = get_db()
        results = conn.execute(
            text(
                """  
                SELECT * FROM flexible_registry.group_member  
                WHERE group_uuid = :group_uuid
                order by sequence  
                """
            ),
            {"group_uuid": group_uuid},
        ).fetchall()

        if results is None:
            raise NotFoundException(
                f"No Members found for Group with UUID: {group_uuid}"
            )

        members = []
        for result in results:
            value_set = ValueSet.load(result.value_set_uuid)
            # todo: load the appropriate type of group member
            member = GroupMember(
                uuid=result.uuid,
                group=self,
                title=result.title,
                sequence=result.sequence,
                value_set=value_set,
            )
            members.append(member)

        self.members = members

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

    def swap_sequence(self, direction):
        """
        Swap the sequence of a group item with the item after or before it.

        Parameters:
        - direction: A string, either "next" or "previous"
        """

        conn = get_db()

        # Using self.sequence and self.group.uuid directly.
        given_sequence = self.sequence
        registry_uuid = self.registry.uuid

        # Depending on the direction, the SQL query changes
        if direction == "next":
            order_by = "ASC"
            comparison_operator = ">"
        elif direction == "previous":
            order_by = "DESC"
            comparison_operator = "<"
        else:
            raise BadRequestWithCode(
                "Group.sequence.direction",
                "Direction should be either 'next' or 'previous'.",
            )

        # Get the UUID and sequence for the item after/before the current item
        result = conn.execute(
            text(
                f"""
            SELECT uuid, sequence FROM flexible_registry.group
            WHERE sequence {comparison_operator} :given_sequence AND registry_uuid = :registry_uuid
            ORDER BY sequence {order_by}
            LIMIT 1
        """
            ),
            {"given_sequence": given_sequence, "registry_uuid": registry_uuid},
        )

        adjacent_uuid, adjacent_sequence = result.fetchone()

        # To avoid violating unique constraints, set the adjacent item's sequence to a temporary value
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group
            SET sequence = -1
            WHERE uuid = :adjacent_uuid
        """
            ),
            {"adjacent_uuid": adjacent_uuid},
        )

        # Now set the current item's sequence to the adjacent_sequence
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group
            SET sequence = :adjacent_sequence
            WHERE sequence = :given_sequence AND registry_uuid = :registry_uuid
        """
            ),
            {
                "adjacent_sequence": adjacent_sequence,
                "given_sequence": given_sequence,
                "registry_uuid": registry_uuid,
            },
        )

        # Finally, set the sequence of the adjacent item (currently -1) to the current item's original sequence
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group
            SET sequence = :given_sequence
            WHERE uuid = :adjacent_uuid
        """
            ),
            {"given_sequence": given_sequence, "adjacent_uuid": adjacent_uuid},
        )

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
class LabsGroup(Group):
    minimum_panel_members: Optional[int]

    @classmethod
    def post_create_hook(cls, group_uuid, **kwargs):
        """
        Provide create overrides for this subclass
        """
        conn = get_db()
        conn.execute(
            """
            INSERT INTO flexible_registry.labs_group
            (group_uuid, minimum_panel_members)
            values
            (:group_uuid, :minimum_panel_members)
            """,
            {
                "group_uuid": group_uuid,
                "minimum_panel_members": kwargs["minimum_panel_members"],
            },
        )

        return cls.load(group_uuid)

    @classmethod
    def fetch_data(cls, uuid):
        data = super().fetch_data(uuid)

        # Load additional data or modify existing data
        conn = get_db()
        result = conn.execute(
            """
            select * from flexible_registry.labs_group
            where group_uuid=:group_uuid
            """,
            {"group_uuid": uuid},
        ).fetchone()
        data["minimum_panel_members"] = result.minimum_panel_members

        return data

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            title=data["title"],
            sequence=data["sequence"],
            registry=data["registry"],
            minimum_panel_members=data["minimum_panel_members"],
        )

    def serialize(self):
        serialized = super().serialize()
        serialized["minimum_panel_members"] = self.minimum_panel_members
        return serialized


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

        if not result:
            raise NotFoundException(f"No Group Member found with UUID: {uuid}")

        if result:
            return {
                "uuid": result.uuid,
                "group": Group.load(result.group_uuid),
                "title": result.title,
                "sequence": result.sequence,
                "value_set": ValueSet.load(result.value_set_uuid),
            }

    @classmethod
    def create_instance_from_data(cls, **data):
        return cls(
            uuid=data["uuid"],
            group=data["group"],
            title=data["title"],
            sequence=data["sequence"],
            value_set=data["value_set"],
        )

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
                raise NotFoundException(
                    f"No ValueSet found with UUID: {value_set_uuid}"
                )
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

    def swap_sequence(self, direction):
        """
        Swap the sequence of a group_member item with the item after or before it.

        Parameters:
        - direction: A string, either "next" or "previous"
        """

        conn = get_db()

        # Using self.sequence and self.group.uuid directly.
        given_sequence = self.sequence
        group_uuid = self.group.uuid

        # Depending on the direction, the SQL query changes
        if direction == "next":
            order_by = "ASC"
            comparison_operator = ">"
        elif direction == "previous":
            order_by = "DESC"
            comparison_operator = "<"
        else:
            raise BadRequestWithCode(
                "GroupMember.sequence.direction",
                "Direction should be either 'next' or 'previous'.",
            )

        # Get the UUID and sequence for the item after/before the current item
        result = conn.execute(
            text(
                f"""
            SELECT uuid, sequence FROM flexible_registry.group_member
            WHERE sequence {comparison_operator} :given_sequence AND group_uuid = :group_uuid
            ORDER BY sequence {order_by}
            LIMIT 1
        """
            ),
            {"given_sequence": given_sequence, "group_uuid": group_uuid},
        )

        adjacent_uuid, adjacent_sequence = result.fetchone()

        # To avoid violating unique constraints, set the adjacent item's sequence to a temporary value
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group_member
            SET sequence = -1
            WHERE uuid = :adjacent_uuid
        """
            ),
            {"adjacent_uuid": adjacent_uuid},
        )

        # Now set the current item's sequence to the adjacent_sequence
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group_member
            SET sequence = :adjacent_sequence
            WHERE sequence = :given_sequence AND group_uuid = :group_uuid
        """
            ),
            {
                "adjacent_sequence": adjacent_sequence,
                "given_sequence": given_sequence,
                "group_uuid": group_uuid,
            },
        )

        # Finally, set the sequence of the adjacent item (currently -1) to the current item's original sequence
        conn.execute(
            text(
                """
            UPDATE flexible_registry.group_member
            SET sequence = :given_sequence
            WHERE uuid = :adjacent_uuid
        """
            ),
            {"given_sequence": given_sequence, "adjacent_uuid": adjacent_uuid},
        )

    def delete(self):
        conn = get_db()
        conn.execute(
            text(
                """  
                DELETE FROM flexible_registry.group_member  
                WHERE "uuid" = :member_uuid  
                """
            ),
            {"member_uuid": self.uuid},
        )

    def serialize(self):
        return {
            "uuid": self.uuid,
            "group_uuid": self.group.uuid,
            "title": self.title,
            "sequence": self.sequence,
            "value_set": {"uuid": self.value_set.uuid, "title": self.value_set.title},
        }


@dataclass
class VitalsGroupMember(GroupMember):
    ucum_ref_units: Optional[str]
    ref_range_high: Optional[str]
    ref_range_low: Optional[str]

    @classmethod
    def post_create_hook(cls, gm_uuid, **kwargs):
        """
        Provide create overrides for this subclass
        """
        ucum_ref_units = kwargs["ucum_ref_units"]
        ref_range_high = kwargs["ref_range_high"]
        ref_range_low = kwargs["ref_range_low"]

        conn = get_db()
        conn.execute(
            """
            INSERT INTO flexible_registry.vitals_group_member
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
            select * from flexible_registry.vitals_group_member
            where group_member_uuid=:gm_uuid
            """,
            {"gm_uuid": uuid},
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

