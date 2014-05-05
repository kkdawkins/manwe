#!/usr/bin/python

from cassandra.cluster import Cluster

def ap(ip):
    return {'username': '5c3ba3712ed3f0e280f4cassandra', 'password': 'cassandra'}

def main():
    cluster = Cluster(['127.0.0.1'], compression=False, auth_provider=ap)

    session = cluster.connect()

    session.execute("CREATE KEYSPACE foobar WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
    session.execute("USE foobar;")

    session.execute("CREATE TABLE birds (name varchar PRIMARY KEY, description varchar, size varchar);")
    session.execute("INSERT INTO birds (name, description, size) VALUES ('robin', 'song bird', 'small');");
    session.execute("INSERT INTO birds (name, description, size) VALUES ('eagle', 'bird of prey', 'large');");
    session.execute("INSERT INTO birds (name, description, size) VALUES ('albatross', 'seabird', 'can hang around your neck');");

    rows = session.execute("SELECT * FROM birds;");
    for r in rows:
        print r

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
