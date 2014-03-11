#!/usr/bin/python

from cassandra.cluster import Cluster

def ap(ip):
    return {'username': '1234567890123456789012345678901234567890cassandra', 'password': 'cassandra'}

def main():
    cluster = Cluster(['127.0.0.1'], compression=False, auth_provider=ap)

    session = cluster.connect()

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()