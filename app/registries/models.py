import csv
import datetime
import io
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlalchemy import text

from app.database import get_db
from app.errors import BadRequestWithCode, NotFoundException
from app.helpers.oci_helper import get_csv_from_oci, put_data_to_oci
from app.value_sets.models import ValueSet

REGISTRY_SCHEMA_VERSION = 1


@dataclass
class Registry:
    uuid: uuid.UUID
    title: str
    registry_type: str  # not an enum so users can create new types of registries w/o code change
    sorting_enabled: bool

    def __post_init__(cls):
        cls.groups = []

    @classmethod
    def create(
        cls,
        title: str,
        registry_type: str,
        sorting_enabled: bool
    ):
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

        RegistryVersion.create(registry_uuid, version=1)

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

    @classmethod
    def load_most_recent_version(cls, registry_uuid, environment):
        conn = get_db()
        if environment is None:
            results = conn.execute(
                text(
                    """
                    select * from flexible_registry.registry_version
                    where registry_uuid = :uuid 
                    order by version desc
                    limit 1
                    """
                ),
                {"uuid": registry_uuid}
            )
        else:
            results = conn.execute(
                text(
                    """
                    select * from flexible_registry.registry_version
                    where registry_uuid = :registry_uuid and status like :environment
                    order by version desc
                    limit 1
                    """
                ),
                {
                    "registry_uuid": registry_uuid,
                    "environment": f"%{environment}%"
                }
            )
        recent_version = results.first()
        if recent_version is None:
            raise NotFoundException(f"No {environment} version(s) published in OCI for Registry with UUID: {registry_uuid}")

        return RegistryVersion.load(recent_version.uuid)

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
    
    @classmethod
    def export_csv(cls, registry_uuid):
        """
            CSV column labels for Product, and corresponding table column names, are
            - "productGroupLabel" group.title
            - "productItemLabel" group_member.title
            - "minimum_panel_members" lab_group.minimum_panel_members (labs only)
            - "ucum_ref_units" vitals_group_member.ucum_ref_units (vitals only)
            - "ref_range_low" vitals_group_member.ref_range_low (vitals only)
            - "ref_range_low" vitals_group_member.ref_range_low (vitals only)
            - "sequence" ORDER BY group.sequence, member.sequence (display as numbers starting at 1)
            - "valueSetUuid" group_member.value_set_uuid
            - "valueSetDisplayTitle" value_set.title
            - "valueSetCodeName" value_set.name
            - "valueSetVersion" value_set_version.version
        """
        # Create a file-like object in memory
        output = io.StringIO()

        # Get the row data from the flexible_registry tables
        registry = Registry.load(registry_uuid)

        conn = get_db()
        if registry.registry_type == "labs":
            results = conn.execute(
                text(
                    """  
                    SELECT 
                    S.uuid AS value_set_uuid, S.title AS value_set_title, S.name AS value_set_name,
                    M.title AS member_title, M.sequence AS member_sequence,
                    G.title AS group_title, G.sequence AS group_sequence,
                    L.minimum_panel_members AS minimum_panel_members
                    FROM flexible_registry."group" G
                    JOIN flexible_registry.labs_group L ON L.group_uuid = G.uuid
                    JOIN flexible_registry.group_member M ON M.group_uuid = G.uuid
                    JOIN value_sets.value_set S ON S.uuid = M.value_set_uuid
                    WHERE G.registry_uuid = :registry_uuid
                    ORDER BY G.sequence, M.sequence  
                    """
                ),
                {"registry_uuid": registry_uuid},
            ).fetchall()
        elif registry.registry_type == "vitals":
            results = conn.execute(
                text(
                    """  
                    SELECT 
                    S.uuid AS value_set_uuid, S.title AS value_set_title, S.name AS value_set_name,
                    M.title AS member_title, M.sequence AS member_sequence,
                    G.title AS group_title, G.sequence AS group_sequence,
                    V.ucum_ref_units AS ucum_ref_units, 
                    V.ref_range_high AS ref_range_high, V.ref_range_low AS ref_range_low
                    FROM flexible_registry."group" G
                    JOIN flexible_registry.group_member M ON M.group_uuid = G.uuid
                    JOIN flexible_registry.vitals_group_member V ON V.group_member_uuid = M.uuid
                    JOIN value_sets.value_set S ON S.uuid = M.value_set_uuid
                    WHERE G.registry_uuid = :registry_uuid
                    ORDER BY G.sequence, M.sequence  
                    """
                ),
                {"registry_uuid": registry_uuid},
            ).fetchall()
        else:
            results = conn.execute(
                text(
                    """  
                    SELECT 
                    S.uuid AS value_set_uuid, S.title AS value_set_title, S.name AS value_set_name,
                    M.title AS member_title, M.sequence AS member_sequence,
                    G.title AS group_title, G.sequence AS group_sequence
                    FROM flexible_registry."group" G
                    JOIN flexible_registry.group_member M ON M.group_uuid = G.uuid
                    JOIN value_sets.value_set S ON S.uuid = M.value_set_uuid
                    WHERE G.registry_uuid = :registry_uuid
                    ORDER BY G.sequence, M.sequence  
                    """
                ),
                {"registry_uuid": registry_uuid},
            ).fetchall()

        # Make a dictionary using the keys for Product
        data = []
        csv_sequence = 0
        for result in results:
            csv_sequence += 1
            value_set_uuid = result.value_set_uuid
            value_set_version = ValueSet.load_most_recent_active_version(value_set_uuid).version
            if registry.registry_type == "labs":
                row = {
                    "productGroupLabel": result.group_title,
                    "productItemLabel": result.member_title,
                    "minimumPanelMembers": result.minimum_panel_members,
                    "sequence": csv_sequence,
                    "valueSetUuid": value_set_uuid,
                    "valueSetDisplayTitle": result.value_set_title,
                    "valueSetCodeName": result.value_set_name,
                    "valueSetVersion": value_set_version
                }
            elif registry.registry_type == "vitals":
                row = {
                    "productGroupLabel": result.group_title,
                    "productItemLabel": result.member_title,
                    "ucumRefUnits": result.ucum_ref_units,
                    "refRangeLow": result.ref_range_low,
                    "refRangeHigh": result.ref_range_high,
                    "sequence": csv_sequence,
                    "valueSetUuid": value_set_uuid,
                    "valueSetDisplayTitle": result.value_set_title,
                    "valueSetCodeName": result.value_set_name,
                    "valueSetVersion": value_set_version
                }
            else:
                row = {
                    "productGroupLabel": result.group_title,
                    "productItemLabel": result.member_title,
                    "sequence": csv_sequence,
                    "valueSetUuid": value_set_uuid,
                    "valueSetDisplayTitle": result.value_set_title,
                    "valueSetCodeName": result.value_set_name,
                    "valueSetVersion": value_set_version
                }
            data.append(row)

        # Create a DictWriter object
        fieldnames = [
            "productGroupLabel",
            "productItemLabel",
        ]
        if registry.registry_type == "labs":
            fieldnames += ["minimumPanelMembers"]
        if registry.registry_type == "vitals":
            fieldnames += [
                "ucumRefUnits",
                "refRangeLow",
                "refRangeHigh"
            ]
        fieldnames += [
            "sequence",
            "valueSetUuid",
            "valueSetDisplayTitle",
            "valueSetCodeName",
            "valueSetVersion",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        # Write the header and then the rows
        writer.writeheader()
        for row in data:
            writer.writerow(row)

        # Move the cursor to the beginning of the file-like object to read its content
        output.seek(0)
        return output.getvalue()

    @classmethod
    def publish_to_object_store(cls, registry_uuid, environment):
        """
        Publish the Registry CSV export to the Object Storage in the specified environment:
        'dev', 'stage', 'prod, or 'dev,stage,prod'
        """
        registry = Registry.load(registry_uuid)
        environment_options = environment.split(",")

        # generate a new CSV for dev, or promote the previous CSV: from dev to stage, or stage to prod
        if environment == "prod":
            previous = "stage"
            output = Registry.get_from_object_store(
                registry_uuid,
                environment=previous,
            )
        elif environment == "stage":
            previous = "dev"
            output = Registry.get_from_object_store(
                registry_uuid,
                environment=previous,
            )
        else:
            previous = "pending"
            output = registry.export_csv(registry_uuid)

        # write current version CSV output to OCI
        resource_schema_version = f"{REGISTRY_SCHEMA_VERSION}/{registry.registry_type}"
        registry_version = Registry.load_most_recent_version(registry_uuid, environment=previous)
        publish_version_number = registry_version.version
        for env in environment_options:
            put_data_to_oci(
                content=output,
                resource_type="registry",
                resource_schema_version=resource_schema_version,
                release_status=env,
                resource_id=registry_uuid,
                resource_version=publish_version_number,
                content_type="csv"
            )

        # if write did not raise an exception, update current version and (if dev) create new version in database
        registry_version.update(environment)
        if environment_options[0] == "dev":
            registry_version.create_new_version_from_specified_previous(registry_version.uuid)

        # return CSV to the caller
        return output

    @classmethod
    def get_from_object_store(cls, registry_uuid, environment):
        """
        Get the Registry CSV export from Object Storage in the specified environment:
        'dev', 'stage', or 'prod'
        """
        registry = Registry.load(registry_uuid)
        registry_version_number = Registry.load_most_recent_version(registry_uuid, environment).version
        resource_schema_version = f"{REGISTRY_SCHEMA_VERSION}/{registry.registry_type}"
        return get_csv_from_oci(
            resource_type="registry",
            resource_schema=resource_schema_version,
            environment=environment,
            resource_uuid=registry_uuid,
            resource_version=registry_version_number,
            return_content=True
        )


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
    def load(cls, group_uuid):
        data = cls.fetch_data(group_uuid)
        return cls.create_instance_from_data(**data)

    @classmethod
    def fetch_data(cls, group_uuid):
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

        if result is None:
            raise NotFoundException(f"No Group found with UUID: {group_uuid}")

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

    def update(self, title, **kwargs):
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
        self.post_update_hook(**kwargs)

    def post_update_hook(self, **kwargs):
        return

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
                "Direction must be 'next' or 'previous'.",
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
            text(
                """
                INSERT INTO flexible_registry.labs_group
                (group_uuid, minimum_panel_members)
                values
                (:group_uuid, :minimum_panel_members)
                """
            ),
            {
                "group_uuid": group_uuid,
                "minimum_panel_members": kwargs["minimum_panel_members"],
            },
        )

        return cls.load(group_uuid)

    @classmethod
    def fetch_data(cls, group_uuid):
        data = super().fetch_data(group_uuid)

        # Load additional data or modify existing data
        conn = get_db()
        result = conn.execute(
            text(
                """
                select * from flexible_registry.labs_group
                where group_uuid=:group_uuid
                """
            ),
            {"group_uuid": group_uuid},
        ).fetchone()

        if result is not None:
            data["minimum_panel_members"] = result.minimum_panel_members
        else:
            data["minimum_panel_members"] = None

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

    def post_update_hook(self, **kwargs):
        conn = get_db()
        minimum_panel_members = kwargs.get("minimum_panel_members")
        if minimum_panel_members is not None:
            conn.execute(
                text(
                    """    
                    UPDATE flexible_registry.labs_group    
                    SET minimum_panel_members=:minimum_panel_members    
                    WHERE group_uuid=:group_uuid    
                    """
                ),
                {
                    "minimum_panel_members": minimum_panel_members,
                    "group_uuid": self.uuid,
                },
            )
            self.minimum_panel_members = minimum_panel_members

    def serialize(self):
        serialized = super().serialize()
        serialized["minimum_panel_members"] = self.minimum_panel_members
        return serialized


@dataclass
class VitalsGroup(Group):
    def load_members(self):
        """
        Join all the data from group_member and vitals_group_member and return a VitalsGroupMember
        """
        conn = get_db()
        results = conn.execute(
            text(
                """  
                SELECT * FROM flexible_registry.group_member 
                JOIN flexible_registry.vitals_group_member
                ON group_member.uuid = vitals_group_member.group_member_uuid
                WHERE group_member.group_uuid = :group_uuid
                order by sequence  
                """
            ),
            {"group_uuid": self.uuid},
        ).fetchall()

        if results is None:
            raise NotFoundException(
                f"No Members found for Vitals Group with UUID: {self.uuid}"
            )

        members = []
        for result in results:
            value_set = ValueSet.load(result.value_set_uuid)
            member = VitalsGroupMember(
                uuid=result.uuid,
                group=self,
                title=result.title,
                sequence=result.sequence,
                value_set=value_set,
                ucum_ref_units=result.ucum_ref_units,
                ref_range_high=result.ref_range_high,
                ref_range_low=result.ref_range_low,
            )
            members.append(member)

        self.members = members


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
        group_member_uuid = uuid.uuid4()

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
                "uuid": group_member_uuid,
                "group_uuid": group_uuid,
                "title": title,
                "value_set_uuid": value_set_uuid,
            },
        )

        return cls.post_create_hook(group_member_uuid, **kwargs)

    @classmethod
    def post_create_hook(cls, group_member_uuid, **kwargs):
        """
        To be overridden in subclasses to save additional data to accessory tables
        """
        return cls.load(group_member_uuid)

    @classmethod
    def load(cls, group_member_uuid):
        data = cls.fetch_data(group_member_uuid)
        return cls.create_instance_from_data(**data)

    @classmethod
    def fetch_data(cls, group_member_uuid):
        conn = get_db()
        result = conn.execute(
            text(
                """
                SELECT * FROM flexible_registry."group_member"
                WHERE "uuid" = :group_member_uuid
                """
            ),
            {"group_member_uuid": group_member_uuid},
        ).fetchone()

        if not result:
            raise NotFoundException(
                f"No Group Member found with UUID: {group_member_uuid}"
            )

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

    def update(self, title=None, **kwargs):
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

        self.post_update_hook(**kwargs)

    def post_update_hook(self, **kwargs):
        return

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
                "Direction must be 'next' or 'previous'.",
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
    def post_create_hook(cls, group_member_uuid, **kwargs):
        """
        Provide create overrides for this subclass
        """

        conn = get_db()
        conn.execute(
            text(
                """
                INSERT INTO flexible_registry.vitals_group_member
                (group_member_uuid, ucum_ref_units, ref_range_high, ref_range_low)
                values
                (:group_member_uuid, :ucum_ref_units, :ref_range_high, :ref_range_low)
                """
            ),
            {
                "group_member_uuid": group_member_uuid,
                "ucum_ref_units": kwargs["ucum_ref_units"],
                "ref_range_high": kwargs["ref_range_high"],
                "ref_range_low": kwargs["ref_range_low"],
            },
        )

        return cls.load(group_member_uuid)

    @classmethod
    def fetch_data(cls, group_member_uuid):
        data = super().fetch_data(group_member_uuid)

        # If there's no data for the given uuid, return None
        if not data:
            return None

        # Load additional data or modify existing data
        conn = get_db()
        result = conn.execute(
            text(
                """
                select * from flexible_registry.vitals_group_member
                where group_member_uuid=:group_member_uuid
                """
            ),
            {"group_member_uuid": group_member_uuid},
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

    def post_update_hook(self, **kwargs):
        ucum_ref_units = kwargs.get("ucum_ref_units")
        ref_range_high = kwargs.get("ref_range_high")
        ref_range_low = kwargs.get("ref_range_low")
        conn = get_db()
        conn.execute(
            text(
                """    
                UPDATE flexible_registry.vitals_group_member
                SET ucum_ref_units=:ucum_ref_units, ref_range_high=:ref_range_high, ref_range_low=:ref_range_low
                WHERE group_member_uuid=:group_member_uuid    
                """
            ),
            {
                "ucum_ref_units": ucum_ref_units,
                "ref_range_high": ref_range_high,
                "ref_range_low": ref_range_low,
                "group_member_uuid": self.uuid,
            },
        )
        self.ucum_ref_units = ucum_ref_units

    def serialize(self):
        serialized = super().serialize()
        serialized["ucum_ref_units"] = self.ucum_ref_units
        serialized["ref_range_high"] = self.ref_range_high
        serialized["ref_range_low"] = self.ref_range_low
        return serialized


class RegistryVersion:
    def __init__(
        self,
        version_uuid,
        effective_start,
        effective_end,
        version,
        registry,
        status,
        description,
        comments,
    ):
        self.uuid = version_uuid
        self.effective_start = effective_start
        self.effective_end = effective_end
        self.version = version
        self.registry = registry
        self.status = status
        self.description = description
        self.comments = comments

    def __repr__(self):
        return f"<RegistryVersion uuid={self.uuid}, title={self.registry.title}, version={self.version}>"

    @classmethod
    def create(
        cls,
        registry_uuid,
        version: int
    ):
        """
        Only the essential fields are used at this time. created_date is now() and uuid is generated during create.
        Initial status is "pending".
        @param registry_uuid:
        @param version:
        """
        new_version_uuid = uuid.uuid4()
        conn = get_db()
        conn.execute(
            text(
                """  
                insert into flexible_registry.registry_version  
                (uuid, registry_uuid, status, created_date, version)  
                values  
                (:new_version_uuid, :registry_uuid, :status, :created_date, :version)  
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                # "effective_start": effective_start,
                # "effective_end": effective_end,
                "registry_uuid": registry_uuid,
                "status": "pending",
                # "version_description": description,
                # "version_comments": comments,
                "created_date": datetime.now(),
                "version": version,
            },
        )
        return cls.load(new_version_uuid)

    @classmethod
    def load(cls, version_uuid):
        if version_uuid is None:
            raise NotFoundException(f"Registry Version not found")

        conn = get_db()
        version_data = conn.execute(
            text(
                """
                select * from flexible_registry.registry_version where uuid=:uuid
                """
            ),
            {"uuid": version_uuid},
        ).first()
        if version_data is None:
            raise NotFoundException(f"Unable to find Registry Version with UUID: {version_uuid}")

        registry = Registry.load(version_data.registry_uuid)
        registry_version = cls(
            version_data.uuid,
            version_data.effective_start,
            version_data.effective_end,
            version_data.version,
            registry,
            version_data.status,
            version_data.description,
            version_data.comments,
        )
        return registry_version

    @classmethod
    def create_new_version_from_specified_previous(
        cls,
        version_uuid
    ):
        """
        Only the essential fields are used at this time. created_date is now() and uuid is generated during create.
        Initial status is "pending".
        @param version_uuid:
        """
        # Load the input registry_version
        input_version = cls.load(version_uuid)

        # Create a new registry_version
        new_version_uuid = uuid.uuid4()
        new_version = input_version.version + 1
        conn = get_db()
        conn.execute(
            text(
                """  
                INSERT INTO flexible_registry.registry_version  
                (uuid, registry_uuid, status, created_date, version)  
                VALUES  
                (:new_version_uuid, :registry_uuid, :status, :created_date, :version)  
                """
            ),
            {
                "new_version_uuid": new_version_uuid,
                # "effective_start": input_version.effective_start,
                # "effective_end": input_version.effective_end,
                "registry_uuid": input_version.registry.uuid,
                "status": "pending",
                # "description": new_version_description or input_version.description,
                # "comments": new_version_comments or input_version.comments,
                "created_date": datetime.now(),
                "version": new_version,
            },
        )

        # Return the new RegistryVersion object
        return cls.load(new_version_uuid)

    def update(self, environment):
        """
        The only update is to set status to 'dev' 'dev,stage' or 'dev,stage,prod' when publishing the version to OCI
        @param environment: may be 'dev' 'stage' 'prod' for cumulative updates or 'dev,stage,prod' for 3-in-1 updates
        """
        if self.status == "pending":
            status_update = environment
        elif environment not in self.status:
            status_update = ",".join([self.status, environment])
        else:
            return

        conn = get_db()
        result = conn.execute(
            text(
                """  
                update flexible_registry.registry_version SET status=:status where version=:version and uuid=:uuid
                """
            ),
            {
                "status": status_update,
                "version": self.version,
                "uuid": self.uuid,
            }
        )

        return status_update
