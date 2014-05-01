#!/usr/bin/python2

import datetime
import random

from cassandra.cluster import Cluster


# Change these constants as needed to directly connect to the cluster (bypassing the gateway) with root permissions
CASSANDRA_IP = "127.0.0.1"
CASSANDRA_USERNAME = "cassandra"
CASSANDRA_PASSWORD = "cassandra"

TOKEN_LENGTH = 20



def getAuth(ip):
    return {'username': CASSANDRA_USERNAME, 'password': CASSANDRA_PASSWORD}

def main():
    print "Connecting to Cassandra cluster listening on IP %s" % (CASSANDRA_IP)

    cluster = Cluster([CASSANDRA_IP], port=9043, compression=False, auth_provider=getAuth)
    session = cluster.connect()

    print "Done!\n\nMaking sure proper keyspace and table(s) exist..."

    session.execute("CREATE KEYSPACE IF NOT EXISTS multiTenantCassandra WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute("USE multiTenantCassandra;")
    session.execute("""
CREATE TABLE IF NOT EXISTS tokenTable (
    internalToken ascii,
    userToken ascii PRIMARY KEY,
    userId text,
    expiration timestamp,
    comment text
) WITH comment='Internal table for multi-tenancy';
""")

    print "Done!\n"

    uid = raw_input("Please enter a human-readable user identifier (email address, OpenID string, etc): ")
    comment = raw_input("Please enter an optional comment: ")

    print "Creating user tokens..."

    # Note that Cassanra requires that keyspace names begin with a letter, so we must make sure the internalToken does as well
    internalToken = random.choice("abcdef")
    internalToken += "%019x" % (random.randrange(16**(TOKEN_LENGTH-1)))
    userToken = "%020x" % (random.randrange(16**TOKEN_LENGTH))

    # FIXME currently tokens do not expire unless revoked
    expire = datetime.datetime.utcfromtimestamp(0)

    # Add the new tenant to the tokenTable
    cmd = session.prepare("INSERT INTO tokenTable (internalToken, userToken, userId, expiration, comment) VALUES (?, ?, ?, ?, ?);")
    session.execute(cmd, (internalToken, userToken, uid, expire, comment))

    # Now, setup a tenant-specific "cassandra" default user
    session.execute("CREATE USER '%scassandra' WITH PASSWORD 'cassandra' SUPERUSER;" % internalToken)

    print "Done!\n\nThe user token is '%s'." % (userToken)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
