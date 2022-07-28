from app.database import get_db


def db_cursor(func):
    """ helper to use curser and only call for get_db here... for @staticmethods """
    def new_func(*args):
        cursor = get_db()
        try:
            cursor.execute('BEGIN')
            transaction = func(cursor, *args)
            cursor.execute('COMMIT')
        except Exception as error:
            cursor.execute('ROLLBACK')
            raise f"Failed operation: {error}"
        return transaction

    return new_func
