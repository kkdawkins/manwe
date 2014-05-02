#!/usr/bin/python

from cassandra.cluster import Cluster

def ap(ip):
    return {'username': 'cassandra', 'password': 'cassandra'}

def main():
    cluster = Cluster(['127.0.0.1'], compression=False, auth_provider=ap)

    session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS demo10 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
    session.execute("USE demo10;")

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
