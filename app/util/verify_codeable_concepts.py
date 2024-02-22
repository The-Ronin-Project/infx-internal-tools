import logging

from sqlalchemy import text

import app.models.codes
from app.database import get_db

if __name__ == "__main__":
    conn = get_db()

    print("Verifying custom_terminologies.code_data")

    # Verify codeable concepts in custom_terminologies.code_data
    custom_terminologies_data = conn.execute(
        text(
            """
            select * from custom_terminologies.code_data
            where code_schema != 'code'
            """
        )
    ).fetchall()

    for row in custom_terminologies_data:
        code_jsonb_data = row.code_jsonb
        try:
            app.models.codes.FHIRCodeableConcept.deserialize(code_jsonb_data)
        except Exception as e:
            print('uuid', row.uuid)
            print('code_schema', row.code_schema)
            print('code_simple', row.code_simple)
            print('code_jsonb', row.code_jsonb)

    # Verify codeable concepts in value_sets.expansion_member_data
    expansion_member_codeable_concepts = conn.execute(
        text(
            """
            select * from value_sets.expansion_member_data
            where code_schema != 'code'
            """
        )
    ).fetchall()

    print("Verifying value_sets.expansion_member_data")

    for row in expansion_member_codeable_concepts:
        code_jsonb_data = row.code_jsonb
        try:
            app.models.codes.FHIRCodeableConcept.deserialize(code_jsonb_data)
        except Exception as e:
            print('uuid', row.uuid)
            print('code_schema', row.code_schema)
            print('code_simple', row.code_simple)
            print('code_jsonb', row.code_jsonb)

    conn.close()
    print("Complete")
