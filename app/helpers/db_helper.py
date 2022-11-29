from app.database import get_db
from sqlalchemy import Table, MetaData, desc, update
from types import SimpleNamespace


def db_cursor(func):
    """helper to use curser and only call for get_db here... for @staticmethods"""

    def new_func(*args):
        conn = get_db()
        try:
            conn.execute("BEGIN")
            transaction = func(conn, *args)
            conn.execute("COMMIT")
        except Exception as error:
            conn.execute("ROLLBACK")
            raise error
        return transaction

    return new_func


@db_cursor
def dynamic_select_stmt(conn, query_table, data, orderby=None):
    """dynamic select query, can handle multiple 'WHERE' - turns into 'AND'"""
    table_name = query_table.get("name")
    schema = query_table.get("schema")
    metadata = MetaData()
    table = Table(
        table_name, metadata, schema=schema, autoload=True, autoload_with=conn
    )
    query = table.select()
    for k, v in data.items():
        query = query.where(getattr(table.columns, k) == v)
    if orderby:
        orderby_result = [
            dict(row) for row in conn.execute(query.order_by(desc(orderby)).limit(1))
        ]
        return SimpleNamespace(**orderby_result[0]) if orderby_result else False
    result = [dict(row) for row in conn.execute(query).all()]
    return SimpleNamespace(**result[0]) if result else False


@db_cursor
def dynamic_insert_stmt(conn, query_table, data):
    """dynamic insert - data passed in as dictionary"""
    table_name = query_table.get("name")
    schema = query_table.get("schema")
    metadata = MetaData()
    table = Table(
        table_name, metadata, schema=schema, autoload=True, autoload_with=conn
    )
    conn.execute(table.insert(), data)
    return


@db_cursor
def dynamic_update_stmt(conn, query_table, id_where, data):
    """update where { ? } == passed in value"""
    table_name = query_table.get("name")
    schema = query_table.get("schema")
    metadata = MetaData()
    table = Table(
        table_name, metadata, schema=schema, autoload=True, autoload_with=conn
    )
    update_action = update(table)
    update_row = update_action.values(**data)
    if conn.execute(table.select().filter_by(**id_where)).scalar() is not None:
        for k, v in id_where.items():
            update_row = update_row.where(getattr(table.columns, k) == v)
            conn.execute(update_row)
            return {"message": f"{id_where} has been updated with {data}"}
    return {"message": f"{id_where} does not exists"}


@db_cursor
def dynamic_delete_stmt(conn, query_table, data):
    """delete where { ? } == passed in value"""
    table_name = query_table.get("name")
    schema = query_table.get("schema")
    metadata = MetaData()
    table = Table(
        table_name, metadata, schema=schema, autoload=True, autoload_with=conn
    )
    for k, v in data.items():
        row_to_delete = table.delete().where(getattr(table.columns, k) == v)
    conn.execute(row_to_delete)
    return
