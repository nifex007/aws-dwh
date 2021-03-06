import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Deletes table if they already exist.
    :param cur:
    :param conn:
    :return:
    """
    for query in drop_table_queries:
        cur.execute(query)
        print('Dropping Tables')
        conn.commit()


def create_tables(cur, conn):
    """
    Creates staging tables
    :param cur:
    :param conn:
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        print(query, 'Created!')
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
