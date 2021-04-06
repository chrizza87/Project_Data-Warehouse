import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes the copy_table_queries to copy the data of the files from s3 into the redshift db 

    Parameters
    ----------
    cur: The psycopg2 cursor for the connection
    conn: The psycopg2 connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes the insert_table_queries to insert the data from the staging tables to the fact and dimension tables inside redshift 

    Parameters
    ----------
    cur: The psycopg2 cursor for the connection
    conn: The psycopg2 connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that reads the config file, connects to the redshift db, executes the loading and inserting methods and afterwerads closes the connection
    """
    config = configparser.ConfigParser()
    config.read('config.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()