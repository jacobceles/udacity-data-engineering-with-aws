import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into staging tables in Redshift.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL commands.
        conn (psycopg2.connection): Connection object to the Redshift database.
    """
    for query in copy_table_queries:
        # Execute each COPY query to load data into staging tables
        cur.execute(query)
        # Commit the transaction
        conn.commit()
        print("Done with load_staging_table")


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into analytics tables in Redshift.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL commands.
        conn (psycopg2.connection): Connection object to the Redshift database.
    """
    for query in insert_table_queries:
        # Execute each INSERT query to insert data into analytics tables
        cur.execute(query)
        # Commit the transaction
        conn.commit()
        print("Done with insert_table")


def main():
    """
    Main function to connect to Redshift, load staging tables, and insert data into analytics tables.
    """
    # Read configuration parameters from 'dwh.cfg' file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster using parameters from configuration
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create a cursor object to execute SQL commands
    cur = conn.cursor()
    
    # Load data into staging tables
    load_staging_tables(cur, conn)
    # Insert data into analytics tables
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
