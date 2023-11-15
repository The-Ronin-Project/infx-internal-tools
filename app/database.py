from flask import current_app, g, has_request_context
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
from decouple import config

# Create an SQL Alchemy engine instance for connecting to the Postgres database.
SQL_ALCHEMY_ENGINE = create_engine(
    f"postgresql://{config('DATABASE_USER', default='')}@{config('DATABASE_HOST', default='')}:{config('DATABASE_PASSWORD', default='')}@{config('DATABASE_HOST', default='')}/{config('DATABASE_NAME', default='')}",
    connect_args={"sslmode": "require"},
    pool_size=5,
    max_overflow=0,
)


class DatabaseManager:
    """
    This is used to ensure there is only one database connection created at a time.

    While working in Flask, we handle this a different way in get_db.

    This is only for use when not working in Flask.
    """

    instance = None

    class __Database:
        def __init__(self):
            self.engine = create_engine(
                f"postgresql://{config('DATABASE_USER', default='')}@{config('DATABASE_HOST', default='')}:{config('DATABASE_PASSWORD', default='')}@{config('DATABASE_HOST', default='')}/{config('DATABASE_NAME', default='')}",
                connect_args={"sslmode": "require"},
                pool_size=2,
                max_overflow=0,
            )
            self.connection = self.engine.connect()
            self.connection.begin()

        def get_connection(self):
            if self.connection.closed is True:
                self.connection = self.engine.connect()
                self.connection.begin()
            return self.connection

    def __new__(cls, connection_string=None):
        if not DatabaseManager.instance:
            DatabaseManager.instance = DatabaseManager.__Database()
        return DatabaseManager.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, value):
        return setattr(self.instance, name, value)


def get_db():
    """
    Retrieve the database connection for the application.
    If not already connected, connect to the appropriate database depending on the configuration.
    """
    if has_request_context():
        if "db" not in g:
            if current_app.config["MOCK_DB"] is True:
                engine = create_engine("sqlite:///tests/dbs/public.db")
                g.db = engine.connect()
                g.db.execute(
                    "attach database 'tests/dbs/concept_maps.db' as concept_maps"
                )
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
                g.db.begin()
        return g.db
    else:
        db = DatabaseManager()
        return db.get_connection()


def get_opensearch():
    """
    Retrieve the Elasticsearch instance for the application.
    If not already connected, connect to the Elasticsearch server using the provided configuration.
    """
    if has_request_context():
        if "opensearch" not in g:
            g.opensearch = OpenSearch(
                f"{config('OPENSEARCH_PROTOCOL', 'https')}://{config('OPENSEARCH_USER')}:{config('OPENSEARCH_PASSWORD')}@{config('OPENSEARCH_HOST')}/",
                verify_certs=False,
            )
        return g.opensearch
    return OpenSearch(
        f"http://{config('OPENSEARCH_PROTOCOL', 'https')}:{config('OPENSEARCH_PASSWORD')}@{config('OPENSEARCH_HOST')}/",
        verify_certs=False,
    )


def rollback_and_close_connection_if_open():
    """
    Rollback and close the database connection if it exists.
    """
    db = g.pop("db", None)

    if db is not None:
        db.rollback()
        db.close()


def close_db(e=None):
    """
    Close the database connection if it exists.
    """
    db = g.pop("db", None)

    if db is not None:
        if e is None:
            db.commit()
        if e is not None:
            db.rollback()
        db.close()
