import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
   ''' Copies raw data from S3 into staging tables
        - staging_events_table
        - staging_songs_table
   '''
   for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table Loaded")

# Load star schema tables with data from staging tables
def insert_tables(cur, conn):
    ''' Inserts data from staging tables into star schema tables
        - Fact Table: songplay_table
        - Dimensional Tables: user_table, song_table, artist_tab, time_table
   '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table Complete")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("LOAD COMPLETE")

    insert_tables(cur, conn)
    print("DATABASE COMPLETE")

    conn.close()


if __name__ == "__main__":
    main()