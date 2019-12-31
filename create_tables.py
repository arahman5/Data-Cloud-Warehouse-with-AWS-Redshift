import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function executes SQL queries that drops the tables from the sparkify database
    :param cur: Daatabse cursor
    :param conn: Connection to the Database
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function executes SQL queries that creates the tables in the sparkify database
    :param cur: Daatabse cursor
    :param conn: Connection to the Database
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
