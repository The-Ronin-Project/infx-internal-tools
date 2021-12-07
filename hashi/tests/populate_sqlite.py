import sqlite3

db = sqlite3.connect("dbs/public.db")
cursor = db.cursor()

cursor.execute("attach database 'dbs/value_sets.db' as value_sets")
cursor.execute("attach database 'dbs/snomedct.db' as snomedct")
cursor.execute("attach database 'dbs/icd_10_cm.db' as icd_10_cm")
cursor.execute("attach database 'dbs/rxndirty.db' as rxnormDirty")
cursor.execute("attach database 'dbs/surveys.db' as surveys")

sql_files = [
    "sql_exports/term_versions.sql", 
    "sql_exports/snomed_schema.sql", 
    "sql_exports/icd10cm.sql", 
    "sql_exports/value_sets.sql",
    "sql_exports/rxndirty.sql",
    "sql_exports/surveys.sql",
    ]

for sql_path in sql_files:
    sql_file = open(sql_path)
    sql_as_string = sql_file.read()
    cursor.executescript(sql_as_string)

print("Success")