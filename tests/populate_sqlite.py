import sqlite3
import csv

db = sqlite3.connect("dbs/public.db")
cursor = db.cursor()

cursor.execute("attach database 'dbs/value_sets.db' as value_sets")
cursor.execute("attach database 'dbs/snomedct.db' as snomedct")
cursor.execute("attach database 'dbs/icd_10_cm.db' as icd_10_cm")
cursor.execute("attach database 'dbs/rxndirty.db' as rxnormDirty")
cursor.execute("attach database 'dbs/surveys.db' as surveys")
cursor.execute("attach database 'dbs/organizations.db' as organizations")
cursor.execute("attach database 'dbs/loinc.db' as loinc")

# Small files that can be exported directly w/ pgAdmin can be loaded via SQL
sql_files = [
    "sql_exports/term_versions.sql", 
    "sql_exports/resource_synonyms.sql",
    "sql_exports/snomed_schema.sql", 
    "sql_exports/icd10cm.sql", 
    "sql_exports/rxndirty.sql",
    "sql_exports/surveys.sql",
    "sql_exports/loinc.sql",
    "sql_exports/organizations.sql",
    # Value Sets
    "sql_exports/value_set.sql",
    "sql_exports/vs_version.sql",
    "sql_exports/value_set_rule.sql",
    "sql_exports/vs_expansion_member.sql",
    "sql_exports/vs_extensional_member.sql",
    "sql_exports/vs_expansion.sql",
    "sql_exports/vs_mapping_inclusion.sql"
    ]

for sql_path in sql_files:
    sql_file = open(sql_path)
    sql_as_string = sql_file.read()
    cursor.executescript(sql_as_string)

# Large tables will have a subset selected for testing and be loaded from CSV

csv_files_map = [
    # Table Name, CSV file name, number of columns
    ('loinc.code', 'loinc_selection.csv', 46)
]

for table_name, csv_name, num_columns in csv_files_map:
    with open('csvs/'+csv_name) as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            query = f"INSERT INTO {table_name} VALUES ({','.join(['?' for x in range(num_columns)])})"
            cursor.execute(query, row)
    cursor.execute("commit")

print("Success")