#!/bin/sh

nohup python cassandra-backend.py -C /opt/planetstack/cassandra_observer/cassandra_observer_config > /dev/null 2>&1 &
