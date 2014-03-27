#!/usr/bin/python2

# This is a script that will run different queries and verify that a multi-tenant Cassandra instance looks "proper".
# There will be some queries that we noticed during testing that ought to work properly, as well as queries that test
# all possible CQL commands.

# There are two ways to run this test: directly against Cassandra (currently the default), or against the gateway.
# Running against Cassandra directly should produce no errors. When running against the gateway, any error indicates a bug in our code.
# To switch the mode, change the port (9043 for Cassandra, 9042 for gateway) and add or remove a user token to the username as needed.

# Important -- The tests assume being run in a new tenant instance. That is, re-running a series of tests again as the
#              same tenant may produce incorrect results.
#              For proper coverage, this script should be run without error under at least two different tenants to verify
#              that 'foo' for tenant 1 is different than 'foo' for tenant 2.
#              Also, test cases are run alphabetically, so if you need to run in a certain order, make sure the function
#              names are in that order.

import unittest

from cassandra.cluster import Cluster
from cassandra import AlreadyExists, InvalidRequest
from cassandra.decoder import SyntaxException

def ap(ip):
    return {'username': 'cassandra', 'password': 'cassandra'}

class TestCassandraQueries(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Called before running tests. Establish a connection to Cassandra

        print "Connecting to Cassandra"

        cls._cluster = Cluster(['127.0.0.1'], port=9043, compression=False, auth_provider=ap)
        cls._session = cls._cluster.connect()

    @classmethod
    def tearDownClass(cls):
        # Called after tests are done.
        cls._session.shutdown()
        cls._cluster.shutdown()

    ########## These queries come from looking at queries made automatically when connecting to Cassandra ##########
    ########## They demonstrate the possibility of leaking information about tenants in the system ##########

    def test_connect_1_select_system_keyspaces(self):
        ########## Test getting initial keyspaces ##########

        rows = self._session.execute("SELECT * FROM system.schema_keyspaces;")

        # There should be only three keyspaces in a default instance: system, system_auth, and system_traces
        self.assertEqual(len(rows), 3)

        for row in rows:
            self.assertTrue(row.keyspace_name in ["system", "system_auth", "system_traces"], "Got unexpected keyspace '%s'" % row.keyspace_name)

    def test_connect_2_select_system_columnfamilies(self):
        ########## Test getting initial columnfamilies ##########

        rows = self._session.execute("SELECT * FROM system.schema_columnfamilies;")

        # FIXME todo

    def test_connect_3_select_system_columns(self):
        ########## Test getting initial columns ##########

        rows = self._session.execute("SELECT * FROM system.schema_columns;")

        # FIXME todo

    ########## These test all CQL queries ##########

    def test_query_CREATE_KEYSPACE(self):
        ########## Test 'CREATE KEYSPACE' command ##########

        # Try an invalid use of CREATE KEYSPACE
        with self.assertRaises(SyntaxException):
            self._session.execute("CREATE KEYSPACE ;")

        # First, make sure keyspace doesn't exist
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'test1' does not exist\""):
            self._session.execute("USE test1;")

        # Make a bunch of keyspaces
        self._session.execute("CREATE KEYSPACE test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
        self._session.execute("CREATE SCHEMA test2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};") # SCHEMA is an alias for KEYSPACE
        self._session.execute("CREATE KEYSPACE IF NOT EXISTS test3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
        self._session.execute("CREATE KEYSPACE IF NOT EXISTS test3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
        self._session.execute("CREATE KEYSPACE test4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} AND DURABLE_WRITES = true;")

        # Try to create an existing keyspace
        with self.assertRaises(AlreadyExists):
            self._session.execute("CREATE KEYSPACE test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")

        # Make sure we can use a new keyspace
        self._session.execute("USE test4;")

        # Clean up keyspaces
        self._session.execute("DROP KEYSPACE test1;")
        self._session.execute("DROP KEYSPACE test2;")
        self._session.execute("DROP KEYSPACE test3;")
        self._session.execute("DROP KEYSPACE test4;")

    def test_query_USE(self):
        ########## Test 'USE' command ##########

        # Try an invalid use of USE
        with self.assertRaises(SyntaxException):
            self._session.execute("USE ;")

        # Try to access an invalid keyspace
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'blah' does not exist\""):
            self._session.execute("USE blah;")

        # Try to access a protected keyspace
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'multitenantcassandra' does not exist\""):
            self._session.execute("USE multiTenantCassandra;")

        # Try to access the three pre-existing 'system' keyspaces. Any error will cause an exception, which will then be caught by the testing framework
        self._session.execute("USE system;")
        self._session.execute("USE system_auth;")
        self._session.execute("USE system_traces;")

# Run the tests!
suite = unittest.TestLoader().loadTestsFromTestCase(TestCassandraQueries)
unittest.TextTestRunner(verbosity=2).run(suite)