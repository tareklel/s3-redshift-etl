import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Connect to Sparkify database 
    - Copy data from S3 to tables in sparkify database
    Arguments:
        cur: the cursor object.
        conn: the connect object.
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Connect to Sparkify database 
    - Insert data into fact and dim tables from staging tables
    Arguments:
        cur: the cursor object.
        conn: the connect object.
    Returns:
        None
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads host, dbname, user, password and port from cluster config.
    - Establishes connection with the Sparkify database and gets
    cursor to it.
    - Loads data from S3 into staging table
    - Inserts data from staging tables into fact and dim tables
    - Finally, closes the connection.
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