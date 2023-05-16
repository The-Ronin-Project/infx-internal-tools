import concurrent.futures
from sqlalchemy import create_engine, text, MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import date, datetime, timedelta
from dateutil import parser
from collections import defaultdict
import werkzeug
from werkzeug.exceptions import BadRequest, NotFound
from decouple import config
from sqlalchemy.sql.expression import bindparam
from app.models.codes import Code

import app.models.concept_maps
from app.models.terminologies import Terminology
from app.database import get_db, get_elasticsearch
from flask import current_app


class ElementType:
    def __init__(
        self,
        element_type_uuid,
        element_type,
        product_element_type_label,
        value_set_uuid,
    ):
        self.element_type_uuid = element_type_uuid
        self.element_type = element_type
        self.product_element_type_label = product_element_type_label
        self.value_set_uuid = value_set_uuid

    @classmethod
    def register_new_element_type(
        cls,
        element_type,
        product_element_type_label,
        value_set_uuid,
    ):
        conn = get_db()
        element_type_uuid = uuid.uuid4()

        conn.execute(
            text(
                """
              insert into value_set_registry.element_type
              (element_type_uuid, element_type, product_element_type_label,value_set_uuid)
              values (:element_type_uuid, :element_type, :product_element_type_label,:value_set_uuid)
              """
            ),
            {
                "element_type_uuid": element_type_uuid,
                "element_type": element_type,
                "product_element_type_label": product_element_type_label,
                "value_set_uuid": value_set_uuid,
            },
        )
        conn.execute(text("commit"))

    @classmethod
    def load(cls, element_type_uuid):
        conn = get_db()
        element_type_data = conn.execute(
            text(
                """
            select * from value_set_registry.element_type where element_type_uuid=:element_type_uuid
            """
            ),
            {"element_type_uuid": element_type_uuid},
        ).first()

        element_type = cls(
            element_type_data.element_type_uuid,
            element_type_data.element_type,
            element_type_data.product_element_type_label,
            element_type_data.value_set_uuid,
        )
        return element_type


class ElementTypeVersion:
    def __init__(
        self,
        element_type_version_uuid,
        element_type_uuid,
        ucum_ref_units,
        ref_range_high,
        ref_range_low,
    ):
        self.element_type_version_uuid = element_type_version_uuid
        self.element_type_uuid = element_type_uuid
        self.ucum_ref_units = ucum_ref_units
        self.ref_range_high = ref_range_high
        self.ref_range_low = ref_range_low

    @classmethod
    def create_element_type_version(
        cls,
        element_type_uuid,
        ucum_ref_units,
        ref_range_high,
        ref_range_low,
    ):
        conn = get_db()
        element_type_version_uuid = uuid.uuid4()

        conn.execute(
            text(
                """  
              insert into value_set_registry.element_type_version  
              (element_type_version_uuid, element_type_uuid, ucum_ref_units, ref_range_high, ref_range_low)  
              values (:element_type_version_uuid, :element_type_uuid, :ucum_ref_units, :ref_range_high, :ref_range_low)  
              """
            ),
            {
                "element_type_version_uuid": element_type_version_uuid,
                "element_type_uuid": element_type_uuid,
                "ucum_ref_units": ucum_ref_units,
                "ref_range_high": ref_range_high,
                "ref_range_low": ref_range_low,
            },
        )
        conn.execute(text("commit"))

    @classmethod
    def load(cls, element_type_version_uuid):
        conn = get_db()
        element_type_version_data = conn.execute(
            text(
                """  
            select * from value_set_registry.element_type_version where element_type_version_uuid=:element_type_version_uuid  
            """
            ),
            {"element_type_version_uuid": element_type_version_uuid},
        ).first()

        element_type_version = cls(
            element_type_version_data.element_type_version_uuid,
            element_type_version_data.element_type_uuid,
            element_type_version_data.ucum_ref_units,
            element_type_version_data.ref_range_high,
            element_type_version_data.ref_range_low,
        )
        return element_type_version


class Group:
    def __init__(
        self,
        group_uuid,
        product_group_label,
        group_sequence,
        group_description,
    ):
        self.group_uuid = group_uuid
        self.product_group_label = product_group_label
        self.group_sequence = group_sequence
        self.group_description = group_description

    @classmethod
    def create_group(
        cls,
        product_group_label,
        group_sequence,
        group_description,
    ):
        conn = get_db()
        group_uuid = uuid.uuid4()

        conn.execute(
            text(
                """    
              insert into value_set_registry."group"    
              (group_uuid, product_group_label, group_sequence, group_description)    
              values (:group_uuid, :product_group_label,:group_sequence, :group_description)    
              """
            ),
            {
                "group_uuid": group_uuid,
                "product_group_label": product_group_label,
                "group_sequence": group_sequence,
                "group_description": group_description,
            },
        )
        conn.execute(text("commit"))

        # Return the created group instance
        return cls(
            group_uuid=group_uuid,
            product_group_label=product_group_label,
            group_sequence=group_sequence,
            group_description=group_description,
        )

    @classmethod
    def load(cls, group_uuid):
        conn = get_db()
        group_data = conn.execute(
            text(
                """  
            select * from value_set_registry."group" where group_uuid=:group_uuid  
            """
            ),
            {"group_uuid": group_uuid},
        ).first()

        group = cls(
            group_data.group_uuid,
            group_data.product_group_label,
            group_data.group_sequence,
            group_data.group_description,
        )
        return group

        # expand_panel method is now in Group class, and updated according to the new schema

    @staticmethod
    def expand_group(group_uuid):
        conn = get_db()
        group_member_data = conn.execute(
            text(
                """    
            select * from value_set_registry.group_member where group_version_uuid=:group_version_uuid    
            """
            ),
            {"group_version_uuid": group_version_uuid},
        ).fetchall()

        group_members = []
        for member in group_member_data:
            group_member = GroupMember(
                member.group_member_uuid,
                member.group_version_uuid,
                member.element_type_version_uuid,
                member.element_type_sequence_number,
            )
            group_members.append(group_member)

        return group_members


class GroupVersion:
    def __init__(
        self,
        group_version_uuid,
        group_uuid,
        group_version_description,
    ):
        self.group_version_uuid = group_version_uuid
        self.group_uuid = group_uuid
        self.group_version_description = group_version_description

    @classmethod
    def create_group_version(
        cls,
        group_uuid,
        group_version_description,
    ):
        conn = get_db()
        group_version_uuid = uuid.uuid4()

        conn.execute(
            text(
                """  
              insert into value_set_registry.group_version  
              (group_version_uuid, group_uuid, group_version_description)  
              values (:group_version_uuid, :group_uuid, :group_version_description)  
              """
            ),
            {
                "group_version_uuid": group_version_uuid,
                "group_uuid": group_uuid,
                "group_version_description": group_version_description,
            },
        )
        conn.execute(text("commit"))

    @classmethod
    def load(cls, group_version_uuid):
        conn = get_db()
        group_version_data = conn.execute(
            text(
                """  
            select * from value_set_registry.group_version where group_version_uuid=:group_version_uuid  
            """
            ),
            {"group_version_uuid": group_version_uuid},
        ).first()

        group_version = cls(
            group_version_data.group_version_uuid,
            group_version_data.group_uuid,
            group_version_data.group_version_description,
        )
        return group_version


class GroupMember:
    def __init__(
        self,
        group_member_uuid,
        group_version_uuid,
        element_type_version_uuid,
        element_type_sequence_number,
    ):
        self.group_member_uuid = group_member_uuid
        self.group_version_uuid = group_version_uuid
        self.element_type_version_uuid = element_type_version_uuid
        self.element_type_sequence_number = element_type_sequence_number

    @classmethod
    def register_element_type(
        cls,
        group_version_uuid,
        element_type_version_uuid,
        element_type_sequence_number,
    ):
        conn = get_db()
        group_member_uuid = uuid.uuid4()

        conn.execute(
            text(
                """    
              insert into value_set_registry.group_member    
              (group_member_uuid, group_version_uuid, element_type_version_uuid, element_type_sequence_number)    
              values (:group_member_uuid, :group_version_uuid, :element_type_version_uuid, :element_type_sequence_number)    
              """
            ),
            {
                "group_member_uuid": group_member_uuid,
                "group_version_uuid": group_version_uuid,
                "element_type_version_uuid": element_type_version_uuid,
                "element_type_sequence_number": element_type_sequence_number,
            },
        )
        conn.execute(text("commit"))

        group_member = cls(
            group_member_uuid,
            group_version_uuid,
            element_type_version_uuid,
            element_type_sequence_number,
        )
        return group_member


if __name__ == "__main__":
    app.run(debug=True)
