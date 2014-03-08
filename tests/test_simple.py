#!/usr/bin/python

from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], compression=False, port=9160)

session = cluster.connect()

session.shutdown()
cluster.shutdown()