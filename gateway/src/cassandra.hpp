#ifndef _CASSANDRA_H
#define _CASSANDRA_H

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include <cql/cql.hpp>
#include <cql/cql_error.hpp>
#include <cql/cql_event.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_execute.hpp>
#include <cql/cql_result.hpp>

#define CASSANDRA_IP "127.0.0.1"
// We are using the CQL port, not the Thrift one (that's 9160)
#define CASSANDRA_PORT 9042

bool checkToken(char *inToken, char *internalToken, bool use_ssl);

#endif
