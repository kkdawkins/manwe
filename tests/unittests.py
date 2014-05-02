#!/usr/bin/python2

# This is a script that will run different queries and verify that a multi-tenant Cassandra instance looks "proper".
# There will be some queries that we noticed during testing that ought to work properly, as well as queries that test
# all possible CQL commands.

# There are two ways to run this test: directly against Cassandra (currently the default), or against the gateway.
# Running against Cassandra directly should produce no errors. (If there are, try removing all of Cassandra's databases with
# `rm -rf /var/lib/cassandra/*`, then restart Cassandra. Of course, this will dump all data stored in Cassandra!) When running
# against the gateway, any error indicates a bug in our code. To switch the mode, change the port (9043 for Cassandra, 9042
# for gateway) and add or remove a user token to the username as needed.

# Important -- The tests assume being run in a new tenant instance. That is, re-running a series of tests again as the
#              same tenant may produce incorrect results.
#              For proper coverage, this script should be run without error under at least two different tenants to verify
#              that 'foo' for tenant 1 is different than 'foo' for tenant 2.
#              Also, test cases are run alphabetically, so if you need to run in a certain order, make sure the function
#              names are in that order.
#              If a test fails, there might be leftover keyspaces or tables that weren't properly cleaned up. This may cause
#              subsequent tests to fail!

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
    ########## (There are additional startup queries, but they deal only with getting peer information, so we can ignore them) ##########

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

        # 21 rows should be returned in a fresh Cassandra instance
        self.assertEqual(len(rows), 21)

        for row in rows:
            self.assertTrue(row.keyspace_name in ["system", "system_auth", "system_traces"], "Got unexpected keyspace '%s'" % row.keyspace_name)

    def test_connect_3_select_system_columns(self):
        ########## Test getting initial columns ##########

        rows = self._session.execute("SELECT * FROM system.schema_columns;")

        # 130 rows should be returned in a fresh Cassandra instance
        self.assertEqual(len(rows), 130)

        for row in rows:
            self.assertTrue(row.keyspace_name in ["system", "system_auth", "system_traces"], "Got unexpected keyspace '%s'" % row.keyspace_name)

    def test_tenant_users(self):
        ########## Test that the only users we see are ours ##########

        rows = self._session.execute("SELECT * FROM system_auth.users;")

        # There should only be the "virtual" cassandra user by default
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].name, "cassandra")
        self.assertTrue(rows[0].super)

        rows = self._session.execute("SELECT * FROM system_auth.credentials;")

        # There should only be the "virtual" cassandra user by default
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].username, "cassandra")

        # Now, add a user and make sure it appears properly
        self._session.execute("CREATE USER 'test' WITH PASSWORD 'test';")
        rows = self._session.execute("SELECT * FROM system_auth.users;")

        self.assertEqual(len(rows), 2)

        for row in rows:
            self.assertTrue(row.name in ["cassandra", "test"], "Got unexpected username '%s'" % row.name)

        # Finally, remove test user
        self._session.execute("DROP USER 'test';")
        rows = self._session.execute("SELECT * FROM system_auth.users;")

        self.assertEqual(len(rows), 1)

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

    def test_query_CREATE_KEYSPACE_metadata(self):
        ########## Test 'CREATE KEYSPACE' command metadata ##########

        self._session.execute("CREATE KEYSPACE test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")

        # Make sure we get back expected metadata (observed from gateway logs)
        rows = self._session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'test1';")
        self.assertEqual(len(rows), 1)

        # These next two queries don't return anything when run directly against Cassandra (no tables or data yet), but they should still be rewritten
        rows = self._session.execute("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name ='test1';")
        self.assertEqual(len(rows), 0)

        rows = self._session.execute("SELECT * FROM system.schema_columns WHERE keyspace_name = 'test1';")
        self.assertEqual(len(rows), 0)

        self._session.execute("DROP KEYSPACE test1;")

        # Make sure we can't snoop other keyspaces
        rows = self._session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'multitenantcassandra';")
        self.assertEqual(len(rows), 0)

        # But make sure the default system keyspaces are available
        rows = self._session.execute("SELECT * FROM system.schema_keyspaces WHERE keyspace_name = 'system';")
        self.assertEqual(len(rows), 1)

        rows = self._session.execute("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name ='system_auth';")
        self.assertEqual(len(rows), 3)

        rows = self._session.execute("SELECT * FROM system.schema_columns WHERE keyspace_name = 'system_traces';")
        self.assertEqual(len(rows), 12)

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

    ########## Misc / unusual CQL queries ##########

    def test_whitepace_and_capitalization(self):
        ########## Test various weird (but valid) whitespace and capitalization variations ##########

        # All queries should run without error
        self._session.execute("use system;")
        self._session.execute("use \"system\";")
        self._session.execute("UsE system;")
        self._session.execute("USE SyStEm;")
        self._session.execute("USE\n\n\t\n\n\n\t\t\t\t\tsystem\n;")
        self._session.execute("select cluster_name from local;")
        self._session.execute("select                                                       cluster_name FROM local;")
        self._session.execute("Select \"cluster_name\" from local;")
        self._session.execute("Select cluster_name from \"local\";")
        self._session.execute("select\ncluster_name\nfrom\nlocal\n;")

        # These should return errors
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'blah' does not exist\""):
            self._session.execute("USe \"blah\";")
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'blah' does not exist\""):
            self._session.execute("USE\n\nblah;")
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"Keyspace 'blah' does not exist\""):
            self._session.execute("USE                            \n\t\tblah;")
        with self.assertRaisesRegexp(InvalidRequest, "code=2200 \[Invalid query\] message=\"unconfigured columnfamily blah\""):
            self._session.execute("select cluster_name from \"blah\";")


    def test_ALTER_KEYSPACE_TABLE(self):
	## You cannot change the name of the keyspace ##
	#self._session.execute("DROP KEYSPACE test;")
	self._session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
	self._session.execute("ALTER KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};")

	self._session.execute("CREATE TABLE test.users (user_name varchar PRIMARY KEY, bio ascii);")
	
	## Changing the type of a column ##
	self._session.execute("ALTER TABLE test.users ALTER bio TYPE text;")
	with self.assertRaises(InvalidRequest): ## Column password was not found in table users ##
	    self._session.execute("ALTER TABLE test.users ALTER password TYPE blob;")


	## Adding a column ##
	self._session.execute("ALTER TABLE test.users ADD address varchar;")
	self._session.execute("ALTER TABLE test.users ADD places list<text>;")

	with self.assertRaises(InvalidRequest):
	    self._session.execute("ALTER TABLE test.users ADD address varchar;")

	## Dropping a column ##
	self._session.execute("ALTER TABLE test.users DROP places;")

	## Modifying table porperties ##
	self._session.execute("ALTER TABLE test.users WITH comment = 'A most excellent and useful table' AND read_repair_chance = 0.2;")

	## Modifying the compression or compaction setting ##
	self._session.execute("ALTER TABLE test.users WITH compression = { 'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64 };")
	self._session.execute("ALTER TABLE test.users WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 6 };")

 	self._session.execute("DROP TABLE test.users;")
	self._session.execute("DROP KEYSPACE test;")
	
	
    def test_UPDATE(self):
        self._session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
        self._session.execute("CREATE TABLE test.emp (empID int,deptID int,first_name varchar,last_name varchar,PRIMARY KEY (empID, deptID));")
	self._session.execute("INSERT \nINTO \ntest.emp (empID,deptID,first_name,last_name)VALUES (123,45,'karan','chadha');")
	self._session.execute("insert \tinto test.emp (empID,deptID,first_name,last_name)values (132,45,'kevin','dawkins');")
	with self.assertRaises(InvalidRequest):## Missing mandatory PRIMARY KEY part deptid ##
	    self._session.execute("UPDATE test.emp SET last_name = 'singh' WHERE empID = 123;")
	
	self._session.execute("UPDATE test.emp SET last_name = 'singh' WHERE empID = 123 AND deptID = 45;")

	## Update a column in several rows at once: ##
	self._session.execute("UPDATE test.emp SET last_name = 'singh' WHERE empID IN (123,132) AND deptID = 45;")
	## Update several columns in a single row: ##
	self._session.execute("UPDATE test.emp SET last_name = 'singh', first_name = 'raj' WHERE empID IN (123,132) AND deptID = 45;")
	self._session.execute("UPDATE test.emp SET last_name = 'singh', first_name = 'raj' WHERE empID = 123 AND deptID = 45;")

	## Update using a collection set ##
	self._session.execute("CREATE TABLE test.users (user_name varchar PRIMARY KEY, bio ascii);")
	self._session.execute("ALTER TABLE test.users ADD places set<text>;")
	self._session.execute("INSERT INTO test.users (user_name, bio, places)VALUES ('kchadha','test',{'Delhi','Meerut'});")
	self._session.execute("UPDATE test.users SET places = places + {'Tucson'} WHERE user_name = 'kchadha';")

	## Update using a collection list ##
	self._session.execute("ALTER TABLE test.users ADD visit_places list<text>;")
	self._session.execute("UPDATE test.users SET visit_places = visit_places + ['Paris','NewYork','Spain'] WHERE user_name = 'kchadha';")
	
	## Update using a collection map ##
	self._session.execute("ALTER TABLE test.users ADD visit_places_withwhom map<text,text>;")
	self._session.execute("UPDATE test.users SET visit_places_withwhom = {'Paris':'Kevin','NewYork':'Wallace','Spain':'Mathias'} WHERE user_name = 'kchadha';")
	
	

	self._session.execute("DROP TABLE test.emp;")
	self._session.execute("DROP TABLE test.users;")
	self._session.execute("DROP KEYSPACE test;")

    def test_CREATE_USER_PERMISSIONS(self):
	#Have to be superuser to execute the following queries

	self._session.execute("CREATE KEYSPACE test1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
	self._session.execute("CREATE TABLE test1.users (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	self._session.execute("INSERT INTO test1.users (user_name, password, gender,session_token,state,birth_year)VALUES ('kchadha','test123','male','12345abc2323','AZ',1990);")
	self._session.execute("CREATE KEYSPACE test2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
	self._session.execute("CREATE USER spillman WITH PASSWORD 'Niner27';")
	self._session.execute("CREATE USER akers WITH PASSWORD 'Niner2' SUPERUSER;")
	self._session.execute("CREATE USER 'boone12' WITH PASSWORD 'Niner75' NOSUPERUSER;")
	self._session.execute("GRANT SELECT ON ALL KEYSPACES TO spillman;")
	self._session.execute("GRANT MODIFY ON KEYSPACE test2 TO akers;")
	self._session.execute("GRANT ALTER ON KEYSPACE test2 TO 'boone12';")
	self._session.execute("GRANT ALL PERMISSIONS ON test1.users TO 'boone12';")
	self._session.execute("GRANT ALL ON KEYSPACE test1 TO spillman;")
	rows = self._session.execute("LIST USERS;")
	self.assertEqual(len(rows), 4)
	for row in rows:
            self.assertTrue(row.name in ["cassandra", "spillman", "akers","boone12"], "Got unexpected user '%s'" % row.name)

	rows = self._session.execute("LIST ALL PERMISSIONS OF akers;")
	self.assertEqual(len(rows), 1)
	for row in rows:
            self.assertTrue(row.permission in ["MODIFY"], "Got unexpected permission '%s'" % row.permission)

	rows = self._session.execute("LIST ALL PERMISSIONS OF 'boone12';")
	self.assertEqual(len(rows), 7)
	for row in rows:
            self.assertTrue(row.permission in ["MODIFY","CREATE","ALTER","DROP","AUTHORIZE","SELECT"], "Got unexpected permission '%s'" % row.permission)

	rows = self._session.execute("LIST ALL PERMISSIONS ON test1.users;")
	self.assertEqual(len(rows), 13)
	for row in rows:
            self.assertTrue(row.permission in ["MODIFY","CREATE","ALTER","DROP","AUTHORIZE","SELECT"], "Got unexpected permission '%s'" % row.permission)

	self._session.execute("REVOKE SELECT ON test1.users FROM 'boone12';")
	rows = self._session.execute("LIST ALL PERMISSIONS OF 'boone12';")
	self.assertEqual(len(rows), 6)
	for row in rows:
            self.assertTrue(row.permission in ["MODIFY","CREATE","ALTER","DROP","AUTHORIZE"], "Got unexpected permission '%s'" % row.permission)

	self._session.execute("DROP USER spillman;")
	self._session.execute("DROP USER akers;")
	self._session.execute("DROP USER 'boone12';")
	self._session.execute("TRUNCATE test1.users;")
	self._session.execute("DROP KEYSPACE test1;")
	self._session.execute("DROP KEYSPACE test2;")


    def test_CREATE_INSERT_TABLE(self):
	#self._session.execute("DROP KEYSPACE test;")
	self._session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")

	self._session.execute("CREATE TABLE test.users (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	self._session.execute("INSERT INTO test.users (user_name, password, gender,session_token,state,birth_year)VALUES ('kchadha','test123','male','12345abc2323','AZ',1990);")
	self._session.execute("DROP TABLE test.users;")

	self._session.execute("Use test;")
	self._session.execute("CREATE table users1 (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")	

	
	self._session.execute("DROP TABLE users1;")

	self._session.execute("CREATE TABLE \"usErs1\" (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	self._session.execute("DROP TABLE \"usErs1\";")

	self._session.execute("CREATE TABLE \"USErS123\" (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	self._session.execute("DROP TABLE \"USErS123\";")
	
	
	##Errors with Creation and Dropping of Tables##
	with self.assertRaises(InvalidRequest):
	    self._session.execute("DROP TABLE \"USErS123\";")

	with self.assertRaises(InvalidRequest):
	    self._session.execute("DROP TABLE \"123\";")

	with self.assertRaises(InvalidRequest):		#Table has already been dropped
	    self._session.execute("DROP TABLE users1;")

	self._session.execute("CREATE table person (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	with self.assertRaises(SyntaxException):
	    self._session.execute("DROP person;")
	
	with self.assertRaises(SyntaxException):
	    self._session.execute("DROP TABL person;")
	
	with self.assertRaises(SyntaxException):
	    self._session.execute("DRO table person;")

	self._session.execute("drop table person;")


	with self.assertRaises(SyntaxException):
	    self._session.execute("CREATE TABLE users1 (user_name vachar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")

	with self.assertRaises(SyntaxException):
	    self._session.execute("CREATE TABE users1 (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")

	with self.assertRaises(SyntaxException):
	    self._session.execute("CREATE users1 (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")

	with self.assertRaises(SyntaxException):
	    self._session.execute("CREATE TABLE users1 (user_name varchar PRIARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")

	self._session.execute("CREATE TABLE \"UUSErs1\" (user_name varchar PRIMARY KEY,password varchar,gender varchar,session_token varchar,state varchar,birth_year bigint);")
	with self.assertRaises(InvalidRequest):
	    self._session.execute("DROP TABLE uusers1;")

	



	self._session.execute("CREATE TABLE test.emp (empID int,deptID int,first_name varchar,last_name varchar,PRIMARY KEY (empID, deptID));")
	self._session.execute("INSERT \nINTO \ntest.emp (empID,deptID,first_name,last_name)VALUES (123,45,'karan','chadha');")
	self._session.execute("insert \tinto test.emp (empID,deptID,first_name,last_name)values (132,45,'kevin','dawkins');")
	self._session.execute("InSErT INtO \n\ttest.emp (empID,deptID,last_name)VALUES (136,45,'chipidza');")
	
	# Errors for Insertion #
	with self.assertRaises(SyntaxException):
	    self._session.execute("INERT INTO test.emp (empID,deptID,first_name,last_name)VALUES (133,45,'karan','chadha');")

	with self.assertRaises(InvalidRequest):
	    self._session.execute("INSERT INTO test.emp (empD,deptID,first_name,last_name)VALUES (133,45,'karan','chadha');")

	with self.assertRaises(InvalidRequest):
	    self._session.execute("INSERT INTO test.emp (empD,deptID,first_name,last_name)VALUES (133,45,'karan','chadha');")

	with self.assertRaises(InvalidRequest):
	    self._session.execute("INSERT INTO test.emp (deptID,first_name,last_name)VALUES (133,45,'karan','chadha');")

	with self.assertRaises(SyntaxException):
	    self._session.execute("insert test.empp (empD,deptID,first_name,last_name)VALUES (1773,46,'mathias','gibbens');")

	with self.assertRaises(SyntaxException):
	    self._session.execute("insert test.emp (empD,deptID,first_name,last_name)VALUES (133,45,'karan','chadha');")
	


	self._session.execute("DROP TABLE test.emp;")
	self._session.execute("DROP KEYSPACE test;")

# Run the tests!
suite = unittest.TestLoader().loadTestsFromTestCase(TestCassandraQueries)
unittest.TextTestRunner(verbosity=2).run(suite)
