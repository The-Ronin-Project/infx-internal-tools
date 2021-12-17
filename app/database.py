from flask import current_app, g
from sqlalchemy import create_engine
from decouple import config

def get_db():
    if 'db' not in g:
        if current_app.config['MOCK_DB'] is True:
            engine = create_engine('sqlite:///tests/dbs/public.db')
            g.db = engine.connect()
            g.db.execute("attach database 'tests/dbs/value_sets.db' as value_sets")
            g.db.execute("attach database 'tests/dbs/snomedct.db' as snomedct")
            g.db.execute("attach database 'tests/dbs/icd_10_cm.db' as icd_10_cm")
            g.db.execute("attach database 'tests/dbs/rxndirty.db' as rxnormDirty")
            g.db.execute("attach database 'tests/dbs/surveys.db' as surveys")
        else:
            engine = create_engine(f"postgresql://{config('DATABASE_USER')}@{config('DATABASE_HOST')}:{config('DATABASE_PASSWORD')}@{config('DATABASE_HOST')}/{config('DATABASE_NAME')}", connect_args={'sslmode':'require'})
            g.db = engine.connect()
    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()