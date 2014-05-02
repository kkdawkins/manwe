// Includes
#include <boost/asio.hpp>
#include <cql/cql.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_result.hpp>

// for logging
void log_callback( const cql::cql_short_t, const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}


int
main(int argc, char**)
{
    using namespace cql;
    using boost::shared_ptr;

    // initialize cql
    cql_initialize();
 //   cql_thread_infrastructure_t cql_ti;

    // Suppose you have the Cassandra cluster at 127.0.0.1,
    // listening at default port (9042).
    shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
    builder->add_contact_point(boost::asio::ip::address::from_string("127.0.0.1"));
    builder->with_credentials("12345678901234567890cassandra", "cassandra");
    //builder->with_ssl();
    builder->with_log_callback(&log_callback); //for logging of activities 

    // Now build a model of cluster and connect it to DB.
    shared_ptr<cql::cql_cluster_t> cluster(builder->build());
    shared_ptr<cql::cql_session_t> session(cluster->connect());

    // Write a query, switch keyspaces.
    shared_ptr<cql::cql_query_t> my_first_query(new cql::cql_query_t("SELECT * FROM system.schema_keyspaces;"));

    // Send the query.
    boost::shared_future<cql::cql_future_result_t> future = session->query(my_first_query);

    // Wait for the query to execute; retrieve the result.
    future.wait();
    shared_ptr<cql_result_t> result = future.get().result;

    // Boilerplate: close the connection session and perform the cleanup.
    session->close();
    cluster->shutdown();
    return 0;
}
