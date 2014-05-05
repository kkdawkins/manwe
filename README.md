Multi-tenant Cassandra
======================

Introduction
------------

This is a project from the Spring 2014 CSC652 "Advanced Topics: Operating Systems" course taught by Dr. Larry Peterson at The University of Arizona.

The goal was to add multi-tenant support to [Cassandra](https://cassandra.apache.org/), and do so in a transparent manner to existing code. The ultimate goal was integration and automatic deployment and tuning within [OpenCloud](http://www.opencloud.us/), but that was not achieved by the end of the semester.

Authors were Wallace Chipidza, Karan Chadha, Kevin Dawkins and Mathias Gibbens.

Compiling
---------

To compile the gateway, cd to gateway/src and run `make`. You can also enable a debug build or profiling build by running `make debug` or `make profile`, respectively.

To manually add a new tenant token to Cassandra, run the `generate-user-token.py` script in the gateway folder.

Development was done on current versions of Debian and Ubuntu, but the code should also compile on other Linux systems as well.

The [DataStax cpp driver](https://github.com/datastax/cpp-driver) is assumed to be installed, and at the time of writing we used the code present in the master branch of the project's git repository.

Testing
-------

Some tests are provided in the tests directory. The `unittests.py` script covers the various CQL commands that could possibly be sent to the gateway and verifies correct responses. If this same script is run directly against a fresh Cassandra instance all tests should pass as well. This demonstrates that the gateway is appropriately "transparent" to end users.

Known issues
------------

1.  Not all features of the CQL spec are fully implemented. These parts are commented in the code with either "FIXME" or "TODO" comments. We observed that the current drivers either do not support some of the optional features or do not default to using them.
2.  If the gateway is compiled normally and run under valgrind, there is a known false positive for invalid reads within the `strlen()` function. You can read a bug report at https://bugzilla.redhat.com/show_bug.cgi?id=678518
3.  The DataStax driver has [known memory leaks](https://groups.google.com/a/lists.datastax.com/d/msg/cpp-driver-user/2OYfRXkr1lY/rd_esNbqLBQJ).
4.  Prepared statements return a unique uuid, which then is submitted when actually running the query. If a malicous tenant was able to guess another tenant's prepared statement uuid, they could run it themselves. We have a couple comments in the code about this, but haven't yet added the checks to make sure only the client that prepared the statement can execute it.
5.  Cassandra limits keyspace names to ~48 characters. Our prefixing the keyspace names with a token might break code that already uses really long keyspace names.
