#ifndef _CASSANDRA_H
#define _CASSANDRA_H

#include <boost/asio.hpp>
#include <cql/cql.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_result.hpp>

bool checkToken(char *inToken, char *internalToken);

#define CASSANDRA_PORT 9160
#define CASSANDRA_IP 127.0.0.1 // Might need this b/c using different IP known internally

#endif
