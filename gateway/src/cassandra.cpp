/*
 * cassandra.cpp - Connection functions for gateway.cpp
 * CSC 652 - 2014
 */

#include "cassandra.hpp"
#include "gateway.hpp"

using boost::shared_ptr;
// This function is called asynchronously every time an event is logged
void
log_callback(const cql::cql_short_t, const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}


shared_ptr<cql::cql_builder_t> initCassandraBuilder(bool use_ssl){
    using namespace cql;
    using boost::shared_ptr;
    #if DEBUG
        std::cout << "[cassandra.cpp initCassandraBuilder] Init CQL.\n";
    #endif
    // Init CQL
    cql_initialize();
    #if DEBUG
        std::cout << "[cassandra.cpp initCassandraBuilder] CQL Init Success.\n";
    #endif    
    try{
        // listening at default port plus one (9042 + 1).
        shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
        #if DEBUG
            std::cout << "[cassandra.cpp initCassandraBuilder] CQL Builder Created.\n";
            builder->with_log_callback(&log_callback); // Only log when debugging
        #endif
        builder->add_contact_point(boost::asio::ip::address::from_string(CASSANDRA_IP), CASSANDRA_PORT + 1);
        #if DEBUG
            std::cout << "[cassandra.cpp initCassandraBuilder] Builder Cluster Contact point Created.\n";
        #endif

        builder->with_credentials(CASSANDRA_ROOT_USERNAME, CASSANDRA_ROOT_PASSWORD);
        #if DEBUG
            std::cout << "[cassandra.cpp initCassandraBuilder] Set 'root' username and password for Cassandra.\n";
        #endif

        if (use_ssl) {
            builder->with_ssl();
            #if DEBUG
                std::cout << "[cassandra.cpp initCassandraBuilder] SSL Enabled.\n";
            #endif
        }
        #if DEBUG
        else{
            std::cout << "[cassandra.cpp initCassandraBuilder] SSL Disabled.\n";
        }
        #endif

	        
        return builder;
    }
    catch (std::exception& e)
    {
        #if DEBUG
            std::cout << "[cassandra.cpp initCassandraBuilder] **Exception Fail**.\n";
        #endif
        std::cout << "Exception: " << e.what() << std::endl;
        exit(1);
    }
}




/*
 * Returns true on success, false on failure (auth or otherwise)
 * Assume that internalToken is already malloc-ed in calling function
 * On failure, internalToken is NULL and false is returned
*/ 
bool checkToken(char *inToken, char *internalToken, bool use_ssl){
    using namespace cql;
    using boost::shared_ptr;
    
    (void)internalToken; // Ugh ... c++ does not like "set but not used"
    
    if(inToken == NULL){
        std::cout << "Parameter inToken not successfully passed.\n";
        exit(1);
    }
    
    try{
    #if DEBUG
        std::cout << "[cassandra.cpp checkToken] Create Cluster.\n";
    #endif
    
        shared_ptr<cql::cql_cluster_t> cluster(initCassandraBuilder(use_ssl)->build());
		
    #if DEBUG
        std::cout << "[cassandra.cpp checkToken] Create Session.\n";
    #endif
    
        shared_ptr<cql::cql_session_t> session(cluster->connect());	
        
    #if DEBUG
        std::cout << "[cassandra.cpp checkToken] Cluster and Session Created.\n";
    #endif	
    	
        if (session) {
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Query - USE multiTenantCassandra.\n";
            #endif
            shared_ptr<cql::cql_query_t> use_system(
            new cql::cql_query_t("USE multiTenantCassandra;", cql::CQL_CONSISTENCY_ONE));
            
            // send the query to Cassandra
            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Executing Query.\n";
            #endif            
            // wait for the query to execute
            future.wait();
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Query Execution Returned.\n";
            #endif            
            if(future.get().error.is_err()){
                // Alert of error?
                printf("'USE multiTenantCassandra' failed: '%s'\n", future.get().error.message.c_str());
                internalToken = NULL;
                session->close();
                cluster->shutdown();
                return false;
            }
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Query - Attempt to find user token, prepare, send, compile.\n";
            #endif            
            // Execute a query where we attempt to find an internal token
            shared_ptr<cql::cql_query_t> select_internal(
                new cql::cql_query_t("SELECT internalToken, expiration FROM tokenTable WHERE userToken=?;", cql::CQL_CONSISTENCY_ONE));
                // FIXME need to also verify that the token is still valid based on the expiration timestamp
                
             // compile the parametrized query on the server
            future = session->prepare(select_internal);
            future.wait();
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Attempt to find user token prepared statement compile return.\n";
            #endif            
            if(future.get().error.is_err()){
                // Alert of error?
                printf("Statement prepare failed: '%s'\n", future.get().error.message.c_str());
                internalToken = NULL;
                session->close();
                cluster->shutdown();
                return false;               
            }
            
            // read the hash (ID) returned by Cassandra as identificator of prepared query
            std::vector<cql::cql_byte_t> queryid = future.get().result->query_id();
            
            shared_ptr<cql::cql_execute_t> bound(
                new cql::cql_execute_t(queryid, cql::CQL_CONSISTENCY_ONE));
                
            
            // bind the query with concrete parameter, which was passed to function
            bound->push_back(inToken);
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Push inToken to prepared statement.\n";
            #endif
            future = session->execute(bound);
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Prepared statement - token check - Passed to execution.\n";
            #endif
            future.wait();
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Prepared statement - token check - returned from exectution.\n";
            #endif            
            if(future.get().error.is_err()){
                // Alert of error?
                printf("User token query failed: '%s'\n", future.get().error.message.c_str());
                internalToken = NULL;
                session->close();
                cluster->shutdown();
                return false;               
            }
            #if DEBUG
                std::cout << "[cassandra.cpp checkToken] Computing query result.\n";
            #endif

            if (future.get().result) {
                if ((*future.get().result).row_count() == 1) {
                    (*future.get().result).next(); // Need to advance to the first row returned

                    std::vector< cql::cql_byte_t > data;
                    int expiration;

                    // Get the internal token
                    (*future.get().result).get_data(0 /* Index */, data);
                    strncpy(internalToken, reinterpret_cast<char*>(&data[0]), TOKEN_LENGTH);
                    
                    // Get the expiration
                    (*future.get().result).get_data(1 /* Index */, data);
                    expiration = atoi(reinterpret_cast<char*>(&data[0]));
                    
                    // Failure case, such that we carry on given a valid expiration 
                    if(expiration != 0 && expiration <= static_cast<long int>(time(NULL))){
                        internalToken = NULL;
                        session->close();
                        cluster->shutdown();
                        return false;
                    }
                }
                else {
                    // There was no user token found. Normal fail case.
                    internalToken = NULL;
                    session->close();
                    cluster->shutdown();
                    return false;
                }
            }
            else {
                // There was no user token found. Normal fail case.
                // Note that we don't seem to hit this, since the resultSet in future.get().result is not null even if no results are returned
                internalToken = NULL;
                session->close();
                cluster->shutdown();
                return false;
            }
            #if DEBUG
            std::cout << "[cassandra.cpp checkToken] Close the session.\n";
            #endif
            session->close();
        }
        #if DEBUG
        std::cout << "[cassandra.cpp checkToken] Shutdown cluster.\n";
        #endif
        cluster->shutdown();
	// TODO: Can I shutdown the cluster? 
	return true;
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
        return false;
    }
}
