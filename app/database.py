from flask import current_app, g
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from decouple import config

# Create an SQL Alchemy engine instance for connecting to the Postgres database.
SQL_ALCHEMY_ENGINE = create_engine(
    f"postgresql://{config('DATABASE_USER', default='')}@{config('DATABASE_HOST', default='')}:{config('DATABASE_PASSWORD', default='')}@{config('DATABASE_HOST', default='')}/{config('DATABASE_NAME', default='')}",
    connect_args={"sslmode": "require"},
    pool_size=2,
    max_overflow=0,
)


def get_db():
    """
    Retrieve the database connection for the application.
    If not already connected, connect to the appropriate database depending on the configuration.
    """
    if "db" not in g:
        if current_app.config["MOCK_DB"] is True:
            engine = create_engine("sqlite:///tests/dbs/public.db")
            g.db = engine.connect()
            g.db.execute("attach database 'tests/dbs/concept_maps.db' as concept_maps")
            g.db.execute("attach database 'tests/dbs/value_sets.db' as value_sets")
            g.db.execute("attach database 'tests/dbs/snomedct.db' as snomedct")
            g.db.execute("attach database 'tests/dbs/icd_10_cm.db' as icd_10_cm")
            g.db.execute("attach database 'tests/dbs/icd_10_pcs.db' as icd_10_pcs")
            g.db.execute("attach database 'tests/dbs/rxndirty.db' as rxnormDirty")
            g.db.execute("attach database 'tests/dbs/surveys.db' as surveys")
            g.db.execute("attach database 'tests/dbs/loinc.db' as loinc")
            g.db.execute(
                "attach database 'tests/dbs/organizations.db' as organizations"
            )
        else:
            g.db = SQL_ALCHEMY_ENGINE.connect()
    return g.db


def get_elasticsearch():
    """
    Retrieve the Elasticsearch instance for the application.
    If not already connected, connect to the Elasticsearch server using the provided configuration.
    """
    if "es" not in g:
        g.es = Elasticsearch(
            f"https://{config('ELASTICSEARCH_USER')}:{config('ELASTICSEARCH_PASSWORD')}@{config('ELASTICSEARCH_HOST')}/"
        )
    return g.es


def close_db(e=None):
    """
    Close the database connection if it exists.
    """
    db = g.pop("db", None)

    if db is not None:
        db.close()
