import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Drop all tables in database
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table Dropped")

# Create tables in database
def create_tables(cur, conn):
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