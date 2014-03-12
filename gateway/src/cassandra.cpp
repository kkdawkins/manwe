/*
 * cassandra.cpp - Connection functions for gateway.cpp
 * CSC 652 - 2014
 */

#include "cassandra.hpp"

// This function is called asynchronously every time an event is logged
void
log_callback(const cql::cql_short_t, const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}

/*
 * Returns true on success, false on failure (auth or otherwise)
 * Assume that internalToken is already malloc-ed in calling function
 * On failure, internalToken is NULL and false is returned
*/ 
bool checkToken(char *inToken, char *internalToken, bool use_ssl){
    using namespace cql;
    using boost::shared_ptr;
    
    // Init CQL
    cql_initialize();
    //cql_thread_infrastructure_t cql_ti;
    
    
    (void)internalToken; // Ugh ... c++ does not like "set but not used"
    
    try{
    
		// listening at default port plus one (9042 + 1).
        shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
        builder->with_log_callback(&log_callback);
        builder->add_contact_point(boost::asio::ip::address::from_string(CASSANDRA_IP), CASSANDRA_PORT + 1);
		
		
        if (use_ssl) {
        builder->with_ssl();
        }
		
		
		// Now build a model of cluster and connect it to DB.
        shared_ptr<cql::cql_cluster_t> cluster(builder->build());
        shared_ptr<cql::cql_session_t> session(cluster->connect());
		
        if (session) {
            boost::shared_ptr<cql::cql_query_t> use_system(
            new cql::cql_query_t("USE system;", cql::CQL_CONSISTENCY_ONE));
            
            // send the query to Cassandra
            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);
            
            // wait for the query to execute
            future.wait();
            
            if(future.get().error.is_err()){
                // Alert of error?
                std::cout << "Use System failed.";
                internalToken = NULL;
                return false;
            }
            
            // Execute a query where we attempt to find an internal token
            boost::shared_ptr<cql::cql_query_t> select_internal(
                new cql::cql_query_t("SELECT internalToken FROM tokentable WHERE usertoken=?;", cql::CQL_CONSISTENCY_ONE));
                
             // compile the parametrized query on the server
            future = session->prepare(select_internal);
            future.wait();
            
            if(future.get().error.is_err()){
                // Alert of error?
                std::cout << "Statement prepare failed.";
                internalToken = NULL;
                return false;               
            }
            
            // read the hash (ID) returned by Cassandra as identificator of prepared query
            std::vector<cql::cql_byte_t> queryid = future.get().result->query_id();
            
            boost::shared_ptr<cql::cql_execute_t> bound(
                new cql::cql_execute_t(queryid, cql::CQL_CONSISTENCY_ONE));
                
            
            // bind the query with concrete parameter: "system_auth"
            bound->push_back(inToken);
            
            future = session->execute(bound);
            
            future.wait();
            
            if(future.get().error.is_err()){
                // Alert of error?
                std::cout << "User token query failed.";
                internalToken = NULL;
                return false;               
            }
            
            if (future.get().result) {
                cql::cql_byte_t* data = NULL;
                cql::cql_int_t size = 0;
                (*future.get().result).get_data(0 /* Index */, &data, size);
                internalToken = reinterpret_cast<char*>(data);
            }else{
                // There was no user token found. Normal fail case.
                internalToken = NULL;
                return false;
            }
            
            session->close();
        }
		cluster->shutdown();
		return true;
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
        return false;
    }
}
