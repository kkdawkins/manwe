#!/usr/bin/python

from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], compression=False)

session = cluster.connect()

session.shutdown()
cluster.shutdown()