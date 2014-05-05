#!/usr/bin/python

from cassandra.cluster import Cluster

def ap(ip):
    return {'username': 'feea15ae5779434cc639cassandra', 'password': 'cassandra'}

def main():
    cluster = Cluster(['127.0.0.1'], compression=False, auth_provider=ap)

    session = cluster.connect()

    session.execute("CREATE KEYSPACE foobar WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute("USE foobar;")

    session.execute("CREATE TABLE sprockets (part_num varchar PRIMARY KEY, description varchar, cost int, in_stock int);")
    session.execute("INSERT INTO sprockets (part_num, description, cost, in_stock) VALUES ('a', 'fancy sprocket', 15, 100);")
    session.execute("INSERT INTO sprockets (part_num, description, cost, in_stock) VALUES ('b', 'cheap whizbang', 1, 67);")
    session.execute("INSERT INTO sprockets (part_num, description, cost, in_stock) VALUES ('c', 'baz 2.0', 25, 1);")
    session.execute("INSERT INTO sprockets (part_num, description, cost, in_stock) VALUES ('d', 'self-sealing stem bolt', 7, 14400);")

    rows = session.execute("SELECT * FROM sprockets;")
    for r in rows:
        print r

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
