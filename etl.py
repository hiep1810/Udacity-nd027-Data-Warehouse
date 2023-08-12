import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from source files into staging tables by executing the SQL queries in the copy_table_queries list.

    Args:
        cur: Cursor object
        conn: Connection object

    Returns:
        None

    """
    for query in copy_table_queries:
        print ("loading: "+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into the final tables by extracting data from the staging tables using the SQL queries specified in the insert_table_queries list.

    Args:
        cur: Cursor object
        conn: Connection object

    Returns:
        None

    """
    for query in insert_table_queries:
        print ("inserting: "+query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connect to the database using the configuration from 'dwh.cfg'
    - Load data into staging tables
    - Insert data into final tables
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()