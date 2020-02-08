import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def create_database():
    config = configparser.ConfigParser()
    config.read('redshift/dwh.cfg')
    
    # connect to default database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
    *config['CLUSTER'].values()))
    cur = conn.cursor()
    
    return cur, conn
    
def load_staging_tables(cur, conn):
    print("Loading staging tables.")
    for query in copy_table_queries:
        print("Executing query: ", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    print("Inserting data into tables.")
    for query in insert_table_queries:
        print("Executing query: ", query)
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
