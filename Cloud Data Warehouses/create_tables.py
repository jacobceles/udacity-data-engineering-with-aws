import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops tables from the Redshift database.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL commands.
        conn (psycopg2.connection): Connection object to the Redshift database.
    """
    for query in drop_table_queries:
        # Execute each DROP query to drop tables
        cur.execute(query)
        # Commit the transaction
        conn.commit()
        print("Dropped table")


def create_tables(cur, conn):
    """
    Creates tables in the Redshift database.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL commands.
        conn (psycopg2.connection): Connection object to the Redshift database.
    """
    for query in create_table_queries:
        # Execute each CREATE query to create tables
        cur.execute(query)
        # Commit the transaction
        conn.commit()
        print("Created table")


def main():
    """
    Main function to connect to Redshift, drop existing tables, and create new tables.
    """
    # Read configuration parameters from 'dwh.cfg' file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift cluster using parameters from configuration
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # Create a cursor object to execute SQL commands
    cur = conn.cursor()

    # Drop existing tables
    drop_tables(cur, conn)
    # Create new tables
    create_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
