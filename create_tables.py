import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    ''' Drops all existing tables in database '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table Dropped")
        
def create_tables(cur, conn):
    ''' Create tables in database
        - Staging Tables: staging_events_table, staging_songs_table
        - Fact Table: songplay_table
        - Dimensional Tables: user_table, song_table, artist_tab, time_table
   '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        print("Table Created")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)

    cur = conn.cursor()

    drop_tables(cur, conn)
    print("ALL DROP")

    create_tables(cur, conn)
    print("ALL CREATE")

    conn.close()


if __name__ == "__main__":
    main() 