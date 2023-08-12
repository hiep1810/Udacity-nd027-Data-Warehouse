import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables from drop_table_queries list.

    Args:
        cur: Cursor object
        conn: Connection object

    Returns:
        None

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("drop successfully")


def create_tables(cur, conn):
    """
    Creates tables based on create_table_queries list.

    Args:
        cur: Cursor object
        conn: Connection object

    Returns:
        None

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("create tables successfully")


def main():
    """
    - Connect to the database using the configuration from 'dwh.cfg'
    - Drop tables if exist
    - Create new tables
    - Close the connection

    Args:
        None

    Returns:
        None

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()