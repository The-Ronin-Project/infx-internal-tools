import sqlite3
from sqlalchemy import create_engine, text, bindparam
from decouple import config

# Connect to database
postgres_engine = create_engine(f"postgresql://{config('DATABASE_USER')}@{config('DATABASE_HOST')}:{config('DATABASE_PASSWORD')}@{config('DATABASE_HOST')}/{config('DATABASE_NAME')}", connect_args={'sslmode':'require'})
postgres_conn = postgres_engine.connect()

#
# Identify which concept maps should be exported
#

# UUIDs for specific Concept Map Versions
concept_map_version_uuids = [
    # '55d9bc89-b91d-4d8c-9f75-6d7c433195b9',
    'cbe12636-102f-4ab0-9616-a8684c9f2a21'
]

#
# Set up schema
#

# Extract schema from Postgres database
column_data_query = postgres_conn.execute(
    text(
        """
            SELECT 
            table_name, 
            column_name, 
            udt_name
            FROM 
            information_schema.columns
            WHERE 
            table_schema = 'concept_maps'
            AND table_name != 'history';
        """
    )
)
all_column_data = [x for x in column_data_query]

# Insert schema into SQLite
db = sqlite3.connect("dbs/public.db")
sqlite_cursor = db.cursor()
sqlite_cursor.execute("attach database 'dbs/concept_maps.db' as concept_maps")

table_names = set([x.table_name for x in all_column_data])

table_columns = {}

for table in table_names:
    column_data = [x for x in all_column_data if x.table_name == table]
    table_columns[table] = [x.column_name for x in column_data]

    query = f"""
        CREATE TABLE concept_maps.{table} (
            {', '.join([f'{x.column_name} {x.udt_name}' for x in column_data])}
        )
    """
    # print(query)
    sqlite_cursor.execute(query)

#
# Download their dependencies and save them to SQLite database
#

def save_postgres_results_to_sqlite(results, table_name):
    for item in results:
        sqlite_cursor.execute(
            f"""
            insert into concept_maps.{table_name}
            ({','.join(table_columns[table_name])})
            values
            ({','.join(['?' for x in table_columns[table_name]])})
            """, tuple([str(item[x]) for x in table_columns[table_name]])
        )

# Concept Map table
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.concept_map
        where uuid in (
            select concept_map_uuid from concept_maps.concept_map_version
            where uuid in :version_uuids
        )
        """
    ).bindparams(
        bindparam('version_uuids', expanding=True)
    ), {
        'version_uuids': concept_map_version_uuids
    }
)
save_postgres_results_to_sqlite(concept_map_data, 'concept_map')

# Concept Map Version table
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.concept_map_version
        where uuid in :version_uuids
        """
    ).bindparams(
        bindparam('version_uuids', expanding=True)
    ), {
        'version_uuids': concept_map_version_uuids
    }
)
save_postgres_results_to_sqlite(concept_map_data, 'concept_map_version')

# Source Concept Table
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.source_concept
        where concept_map_version_uuid in :version_uuids
        """
    ).bindparams(
        bindparam('version_uuids', expanding=True)
    ), {
        'version_uuids': concept_map_version_uuids
    }
)
save_postgres_results_to_sqlite(concept_map_data, 'source_concept')

# Concept Map Version Terminologies

# Concept Relationship
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.concept_relationship
        where concept_map_version_uuid in :version_uuids
        """
    ).bindparams(
        bindparam('version_uuids', expanding=True)
    ), {
        'version_uuids': concept_map_version_uuids
    }
)
save_postgres_results_to_sqlite(concept_map_data, 'concept_relationship')

# Relationship Codes
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.relationship_codes
        """
    )
)
save_postgres_results_to_sqlite(concept_map_data, 'relationship_codes')

# Relationship System
concept_map_data = postgres_conn.execute(
    text(
        """
        select * from concept_maps.relationship_system
        """
    )
)
save_postgres_results_to_sqlite(concept_map_data, 'relationship_system')

sqlite_cursor.execute('commit')


# results = sqlite_cursor.execute(
#     """
#     select source_concept.code as source_code, source_concept.display as source_display, source_concept.system as source_system, 
#             -- tv_source.version as source_version, tv_source.fhir_uri as source_fhir_uri,
#             relationship_codes.code as relationship_code, 
#             concept_relationship.target_concept_code, concept_relationship.target_concept_display,
#             concept_relationship.target_concept_system_version_uuid as target_system,
#             tv_target.version as target_version, tv_target.fhir_uri as target_fhir_uri
#             from concept_maps.source_concept
#             left join concept_maps.concept_relationship
#             on source_concept.uuid = concept_relationship.source_concept_uuid
#             join concept_maps.relationship_codes
#             on relationship_codes.uuid = concept_relationship.relationship_code_uuid
#             join terminology_versions as tv_source
#             on cast(tv_source.uuid as uuid) = cast(source_concept.system as uuid)
#             join terminology_versions as tv_target
#             on tv_target.uuid = concept_relationship.target_concept_system_version_uuid
#             where source_concept.concept_map_version_uuid = ?
#     """, ['cbe12636-102f-4ab0-9616-a8684c9f2a21']
# )
# for item in results: print(item)

