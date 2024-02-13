import logging
from flask import current_app, g, has_request_context
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
from decouple import config

# # Create an SQL Alchemy engine instance for connecting to the Postgres database.
# SQL_ALCHEMY_ENGINE = create_engine(
#     f"postgresql://{config('DATABASE_USER', default='')}:{config('DATABASE_PASSWORD', default='')}@{config('DATABASE_HOST', default='')}/{config('DATABASE_NAME', default='')}",
#     connect_args={"sslmode": "require"},
#     pool_size=5,
#     max_overflow=0,
# )


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
                f"postgresql://{config('DATABASE_USER', default='')}:{config('DATABASE_PASSWORD', default='')}@{config('DATABASE_HOST', default='')}/{config('DATABASE_NAME', default='')}",
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
                db_manager = DatabaseManager()
                g.db = db_manager.get_connection()
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
                hosts=[
                    {
                        "host": config("OPENSEARCH_HOST"),
                        "port": config("OPENSEARCH_PORT"),
                    }
                ],
                http_compress=True,  # enables gzip compression for request bodies
                http_auth=(config("OPENSEARCH_USER"), config("OPENSEARCH_PASSWORD")),
                use_ssl=config("OPENSEARCH_USE_SSL", cast=bool),
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
            )
        return g.opensearch
    return OpenSearch(
        hosts=[{"host": config("OPENSEARCH_HOST"), "port": config("OPENSEARCH_PORT")}],
        http_compress=True,  # enables gzip compression for request bodies
        http_auth=(config("OPENSEARCH_USER"), config("OPENSEARCH_PASSWORD")),
        use_ssl=config("OPENSEARCH_USE_SSL", cast=bool),
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
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
            testing = current_app.config.get("TESTING")
            if testing:
                if not current_app.config.get("DISABLE_ROLLBACK_AFTER_REQUEST"):
                    db.rollback()
                else:
                    logging.info("Not rolling back because DISABLE_ROLLBACK_AFTER_REQUEST=True")
            else:
                db.commit()
        if e is not None:
            db.rollback()
        if not current_app.config.get("DISABLE_CLOSE_AFTER_REQUEST"):
            db.close()
        else:
            logging.info("Not closing connection because DISABLE_CLOSE_AFTER_REQUEST=True")
