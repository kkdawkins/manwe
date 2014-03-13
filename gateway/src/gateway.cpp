/*
 * gateway.cpp - Gateway for Cassandra
 * CSC 652 - 2014
 */
extern "C" {
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pcre.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

}
#include "gateway.hpp"
#include "helpers.hpp"
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <vector>
#include <string>
//#include "cassandra.hpp"
using namespace std;

const char *printable_opcodes[17] = {"ERROR", "STARTUP", "READY", "AUTHENTICATE", "CREDENTIALS", "OPTIONS", "SUPPORTED", "QUERY", "RESULT", "PREPARE", "EXECUTE", "REGISTER", "EVENT", "BATCH", "AUTH_CHALLENGE", "AUTH_RESPONSE", "AUTH_SUCCESS"};

/*
 * Main processing loop of gateway. Spawns individual threads to handle each incoming TCP connection from a client.
 * Return 0 on success (never reached, since it will listen for connections until killed), 1 on error.
*/
int main(int argc, char *argv[]) {
    signal(SIGINT, gracefulExit); // Catch CTRL+C and exit cleanly to properly cleanup memory usage

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <IP addr to listen on>\n", argv[0]);
        exit(1);
    }
    if (inet_addr(argv[1]) == INADDR_NONE) {
        fprintf(stderr, "Please specify a valid IP address to listen on.\n");
        exit(1);
    }

    #if DEBUG
    printf("Cassandra gateway starting up on %s:%d.\n", argv[1], CASSANDRA_PORT);
    #endif

    pthread_attr_t attr;
    pthread_t thread; // Only need one
    
    /* The 2 file descriptors
     * listenfd - listen file
     * connfd   - accepted connection file
     */
    int listenfd = 0, connfd = 0;
    int* thread_data;
    
    struct sockaddr_in serv_addr;
    
    // Do some quick pthread attr setup
    pthread_attr_init(&attr);
    
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        fprintf(stderr, "Socket creation error: %s\n", strerror(errno));
        exit(1);
    }
    
    // Clear the serv_addr struct
    memset(&serv_addr, 0, sizeof(serv_addr));
    
    serv_addr.sin_family = AF_INET;
    
    // Bind to IP address specified on command line
    serv_addr.sin_addr.s_addr = inet_addr(argv[1]);
    serv_addr.sin_port = htons(CASSANDRA_PORT);
    
    // Bind the socket and port to the name
    if (bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        fprintf(stderr, "Socket bind error: %s\n", strerror(errno));
        exit(1);
    }
    
    // Listen for connections, with a backlog of 10
    if (listen(listenfd, 10) == -1) {
        fprintf(stderr, "Socket listen error: %s\n", strerror(errno));
        exit(1);
    }

    #if DEBUG
    printf("Setup complete, beginning loop to listen for connections.\n");
    #endif
    
    // Main listen loop
    while (1) {
        // This is a blocking call!
        connfd = accept(listenfd, (struct sockaddr*)NULL, NULL);
        
        // Must free in function, now threads have their own conn addr.
        thread_data = (int*)malloc(sizeof(int));
        *thread_data = connfd;
        
        if (pthread_create(&thread, &attr, HandleConn, (void *)thread_data) != 0) {
            fprintf(stderr, "pthread_create failed.\n");
            exit(1);
        }
        /*
         * Creating the threads as detached for 2 reasons:
         *  1. They will never rejoin
         *  2. This will save some sys resources
         */
        pthread_detach(thread);
    }

    // Execution will never reach here, but we need to return a value
    return 0;
}

/*
 * This method handles each client connection, processes packets, rewrites queries as needed, and then forwards new packets onto the actaul Cassandra instance.
 * Assumes the real Cassandra instance is listening on CASSANDRA_IP:(CASSANDRA_PORT + 1).
 */
void* HandleConn(void* thread_data) {
    int connfd = *(int *)thread_data;

    // Need to free asap
    free(thread_data);

    // Save this thread's ID to prefix all messages with
    pthread_t tid = pthread_self();

    #if DEBUG
    printf("%u: Thread spawned.\n", (uint32_t)tid);
    #endif

    // Before entering the processing loop, we need to establish a connection to the real Cassandra

    #if DEBUG
    printf("%u: Establishing connection to Cassandra listening on %s:%d.\n", (uint32_t)tid, CASSANDRA_IP, CASSANDRA_PORT + 1);
    #endif

    struct sockaddr_in cassandra_addr;
    int cassandrafd = socket(AF_INET, SOCK_STREAM, 0);
    if (cassandrafd < 0) {
        fprintf(stderr, "%u: Socket creation error: %s\n", (uint32_t)tid, strerror(errno));
        exit(1);
    }

    // Setup sockaddr struct
    memset(&cassandra_addr, 0, sizeof(cassandra_addr));
    cassandra_addr.sin_family = AF_INET;
    cassandra_addr.sin_addr.s_addr = inet_addr(CASSANDRA_IP);
    cassandra_addr.sin_port = htons(CASSANDRA_PORT + 1);

    // Bind the socket and port to the name
    if (connect(cassandrafd, (struct sockaddr*)&cassandra_addr, sizeof(cassandra_addr)) < 0) {
        fprintf(stderr, "%u: Socket bind error: %s\n", (uint32_t)tid, strerror(errno));
        exit(1);
    }

    // Now, enter the packet processing loop

    #if DEBUG
    printf("%u: Entering the packet processing loop.\n\n", (uint32_t)tid);
    #endif

    cql_packet_t *packet = NULL;
    uint8_t header_len = sizeof(cql_packet_t);
    uint32_t body_len = 0;
    int compression_type = CQL_COMPRESSION_NONE;
    int protocol_version_in_use = 0;
    char *token = (char *)malloc(TOKEN_LENGTH + 1);
    memset(token, 0, TOKEN_LENGTH + 1);
    int bytes_avail = 0;

    while (1) {
        // At the top of the loop, we are expecting the start of another CQL packet. If it doesn't look right, send back an error and close the connection.
        // We poll each socket to see if any data is present for us to read. If not, we check the other one, then repeat. Hopefully this won't tax the CPU too
        // much and will give good performance by not being stuck processing lots of packets from only one direction.

        // FIXME There's not a one-to-one correspondance of client to server packets / this first approach may be too CPU intensive.
        //       We may need to use non-blocking I/O.

        // Test to see if any bytes have arrived from the client
        // FIXME when client closes connection, we should break out of this loop, otherwise we may crash when reading a closed socket!
        ioctl(connfd, FIONREAD, &bytes_avail);
        if (bytes_avail > 0) {

            #if DEBUG
            printf("%u: Processing packet from client.\n", (uint32_t)tid);
            #endif

            // Get packet from client
            packet = (cql_packet_t *)malloc(header_len);

            // Perform some basic sanity checks to verify this looks like a CQL packet

            // The first byte must be CQL_V1_REQUEST. Version 2 of the CQL protocol isn't supported by our gateway.
            if (recv(connfd, packet, 1, 0) < 0) { //The first byte of the potential CQL header
                fprintf(stderr, "%u: Error reading first byte from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            if (packet->version != CQL_V1_REQUEST) { // Verify version
                #if DEBUG
                printf("%u: First byte from client is not CQL_V1_REQUEST, closing connections and killing thread.\n", (uint32_t)tid);
                #endif

                free(packet);
                close(connfd);
                break;
            }

            // Set the protocol version to v1
            protocol_version_in_use = 1;

            // Now, read in the remaining 7 bytes of the header.
            if (recv(connfd, ((char *)packet) + 1, 7, 0) < 0) { //The remainder of the CQL header
                fprintf(stderr, "%u: Error reading remainder of header from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            if (packet->stream < 0) { // Client request stream ids must be postitive
                                      // FIXME the python client library seems to start stream ids with "0", which isn't positive or negative
                char msg[] = "Invalid stream id";
                SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                free(packet);
                close(connfd);
                break;
            }

            #if DEBUG
            printf("%u: Header information -- version: %d; flags: %d; stream: %d; opcode: %s; length: %u\n", (uint32_t)tid, packet->version, packet->flags, packet->stream, printable_opcodes[packet->opcode], ntohl(packet->length));
            #endif

            body_len = ntohl(packet->length);

            if (body_len > 0) {
                // Allocate more memory for rest of packet
                cql_packet_t *newpacket = (cql_packet_t *)realloc(packet, header_len + body_len);
                if (newpacket == NULL) {
                    fprintf(stderr, "%u: Failed to realloc memory for packet body!\n", (uint32_t)tid);
                    exit(1);
                }
                else {
                    packet = newpacket;
                }

                // Read in body of packet (possibly over more than one recv() call)
                uint32_t body_bytes_read = 0;
                while (body_bytes_read < body_len) { // Get the rest of the body
                    int32_t bytes_in = recv(connfd, (char *)packet + header_len + body_bytes_read, body_len - body_bytes_read, 0);
                    if (bytes_in < 0) {
                        fprintf(stderr, "%u: Error reading packet body from client: %s\n", (uint32_t)tid, strerror(errno));
                        exit(1);
                    }
                    else {
                        body_bytes_read += bytes_in;
                    }
                }
            }

            #if DEBUG
            printf("%u: Full packet received, beginning processing.\n", (uint32_t)tid);
            #endif

            // If the packet is compressed, decompress the body. Note that we always send uncompressed packets to Cassandra itself, since
            // we're communicating directly on the same host.
            if (packet->flags & CQL_FLAG_COMPRESSION) {
                #if DEBUG
                printf("%u:   Packet body is compressed, decompressing.\n", (uint32_t)tid);
                #endif

                if (compression_type == CQL_COMPRESSION_LZ4) {
                    #if DEBUG
                    printf("%u:   It's lz4 compression!\n", (uint32_t)tid);
                    #endif

                    // TODO
                }
                else if (compression_type == CQL_COMPRESSION_SNAPPY) {
                    #if DEBUG
                    printf("%u:   It's snappy compression!\n", (uint32_t)tid);
                    #endif

                    // TODO
                }
                else {
                    // Either the client is trying to use an unsupported compression algorithm, or compression wasn't properly configured when the STARTUP command was sent. Error in either case.

                    #if DEBUG
                    printf("%u:   Error - Unknown compression method / compression not negotiated.\n", (uint32_t)tid);
                    #endif

                    char msg[] = "Unknown compression method / compression not negotiated";
                    SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                    free(packet);
                    close(connfd);
                    break;
                }

                packet->flags &= ~CQL_FLAG_COMPRESSION;
            }

            // Modify packet (if needed)
            if (packet->opcode == CQL_OPCODE_STARTUP) { // Handle STARTUP packet here, since we may need to set variables for the connection regarding compression

                #if DEBUG
                printf("%u:   Handling STARTUP packet to detect whether to enable compression support.\n", (uint32_t)tid);
                #endif

                cql_string_map_t *sm = ReadStringMap((char *)packet + header_len);
                cql_string_map_t *head = sm;

                if (sm == NULL) { // Malformed STARTUP, since there must always be a CQL_VERSION sent. Send back an error
                    #if DEBUG
                    printf("%u:     Error - Malformed STARTUP.\n", (uint32_t)tid);
                    #endif

                    char msg[] = "Malformed STARTUP";
                    SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                    free(packet);
                    close(connfd);
                    break;
                }

                while (sm != NULL) {
                    #if DEBUG
                    printf("%u:     %s -> %s\n", (uint32_t)tid, sm->key, sm->value);
                    #endif

                    if (strcmp(sm->key, "COMPRESSION") == 0) {
                        if (strcmp(sm->value, "lz4") == 0) {
                            compression_type = CQL_COMPRESSION_LZ4;
                        }
                        else if (strcmp(sm->value, "snappy") == 0) {
                            compression_type = CQL_COMPRESSION_SNAPPY;
                        }
                        else {
                            #if DEBUG
                            printf("%u:     Error - Unknown compression method '%s'.\n", (uint32_t)tid, sm->value);
                            #endif

                            char msg[] = "Unknown compression method";
                            SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                            FreeStringMap(head);
                            head = NULL; // We need to be sneaky and break out the the main processing loop. c++ doesn't allow labels on loops, so use head == NULL as the conditional for another break below.
                            free(packet);
                            close(connfd);
                            break;
                        }

                        // Strip compression from the STARTUP message before passing to Cassandra
                        cql_string_map_t *next = sm->next;
                        free(sm->key);
                        free(sm->value);
                        free(sm);
                        sm = next;
                    }
                    else {
                        sm = sm->next;
                    }
                }

                if (head == NULL) { // Previously seen error in getting compression method
                    break;
                }

                uint32_t new_len = 0;
                char *new_body = WriteStringMap(head, &new_len);
                memcpy((char *)packet + header_len, new_body, new_len); // We know that the body length can only ever remain the same or decrease if the compression option was removed, so no chance of writing past the end of allocated memory.
                free(new_body);
                packet->length = htonl(new_len);

                FreeStringMap(head);

                #if DEBUG
                printf("%u:   Finished with STARTUP, passing to Cassandra.\n", (uint32_t)tid);
                #endif
            }
            else if (packet->opcode == CQL_OPCODE_CREDENTIALS) { // Modify CREDENTIALS packet to get the instance prefix
                if (protocol_version_in_use != 1) { // CREDENTIALS is only used in v1 of the CQL protocol
                    char msg[] = "CREDENTIALS not supported in this version of CQL";
                    SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                    free(packet);
                    close(connfd);
                    break;
                }

                #if DEBUG
                printf("%u:   Handling CREDENTIALS packet to get tenant's token.\n", (uint32_t)tid);
                #endif

                cql_string_map_t *sm = ReadStringMap((char *)packet + header_len); // Get the username / password pair
                cql_string_map_t *head = sm;

                if (sm == NULL) { // No credentials were provided. Send back an error
                    #if DEBUG
                    printf("%u:     Error - No credentials supplied.\n", (uint32_t)tid);
                    #endif

                    char msg[] = "No credentials supplied";
                    SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_BAD_CREDENTIALS, msg);

                    free(packet);
                    close(connfd);
                    break;
                }

                while (sm != NULL) {
                    printf("%u:     %s -> %s\n", (uint32_t)tid, sm->key, sm->value);

                    if (strcmp(sm->key, "username") == 0) {
                        if (strlen(sm->value) <= TOKEN_LENGTH) { // The supplied username must be at least TOKEN_LENGTH + 1 characters long, so we can properly grab the token and still have at least one character remaining to pass on to Cassandra.
                            #if DEBUG
                            printf("%u:       Error - Invalid token + username supplied.\n", (uint32_t)tid);
                            #endif

                            char msg[] = "Token + username is too short";
                            SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_BAD_CREDENTIALS, msg);

                            FreeStringMap(head);
                            head = NULL; // We need to be sneaky and break out the the main processing loop. c++ doesn't allow labels on loops, so use head == NULL as the conditional for another break below.
                            free(packet);
                            close(connfd);
                            break;
                        }
                        else {
                            strncpy(token, sm->value, TOKEN_LENGTH); //Copy the token into the variable for user later on
                            char *username = (char *)malloc(strlen(sm->value) - TOKEN_LENGTH + 1); //Allocate temp storage incase username > TOKEN_LENGTH
                            memset(username, 0, strlen(sm->value) - TOKEN_LENGTH + 1);
                            strncpy(username, sm->value + TOKEN_LENGTH, strlen(sm->value) - TOKEN_LENGTH);
                            strncpy(sm->value, username, strlen(username)); // Move the actual username to the front
                            memset(sm->value + strlen(username), 0, 1); // NULL terminate the string
                            free(username);

                            #if DEBUG
                            printf("%u:       Token: %s\n", (uint32_t)tid, token);
                            printf("%u:       Username: %s\n", (uint32_t)tid, sm->value);
                            #endif

                            // FIXME need to validate supplied token here. If valid, continue, otherwise send back error to client
                        }
                    }

                    sm = sm->next;
                }

                if (head == NULL) { // Previously seen error in getting compression method
                    break;
                }

                uint32_t new_len = 0;
                char *new_body = WriteStringMap(head, &new_len);
                memcpy((char *)packet + header_len, new_body, new_len); // We know that the body length will decrease by TOKEN_LENGTH bytes, so memory allocation will be fine.
                free(new_body);
                packet->length = htonl(new_len);

                FreeStringMap(head);

                #if DEBUG
                printf("%u:   Finished with CREDENTIALS, passing to Cassandra.\n", (uint32_t)tid);
                #endif
            }
            else { // All other packets get processed elsewhere
                // TODO
            }

            // Send packet to Cassandra (body length may have changed, so re-get value from header)
            if (send(cassandrafd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%u: Error sending packet to Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            free(packet);

            #if DEBUG
            printf("%u: Packet successfully sent to Cassandra.\n\n", (uint32_t)tid);
            #endif
        }

        // Test to see if any bytes have arrived from Cassandra
        ioctl(cassandrafd, FIONREAD, &bytes_avail);
        if (bytes_avail > 0) {

            #if DEBUG
            printf("%u: Processing packet from Cassandra.\n", (uint32_t)tid);
            #endif

            // Get packet back from Cassandra. We assume Cassandra will always give us properly formed packets.
            packet = (cql_packet_t *)malloc(header_len);
            if (recv(cassandrafd, packet, header_len, 0) < 0) { // The CQL header is 8 bytes
                fprintf(stderr, "%u: Error reading packet header from Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            #if DEBUG
            assert(packet->version == CQL_V1_RESPONSE);

            printf("%u: Header information -- version: %d; flags: %d; stream: %d; opcode: %s; length: %u\n", (uint32_t)tid, packet->version, packet->flags, packet->stream, printable_opcodes[packet->opcode], ntohl(packet->length));
            #endif

            body_len = ntohl(packet->length);

            if (body_len > 0) {
                // Allocate more memory for rest of packet
                cql_packet_t *newpacket = (cql_packet_t *)realloc(packet, header_len + body_len);
                if (newpacket == NULL) {
                    fprintf(stderr, "%u: Failed to realloc memory for packet body!\n", (uint32_t)tid);
                    exit(1);
                }
                else {
                    packet = newpacket;
                }

                // Read in body of packet (possibly over more than one recv() call)
                uint32_t body_bytes_read = 0;
                while (body_bytes_read < body_len) { // Get the rest of the body
                    int32_t bytes_in = recv(cassandrafd, (char *)packet + header_len + body_bytes_read, body_len - body_bytes_read, 0);
                    if (bytes_in < 0) {
                        fprintf(stderr, "%u: Error reading packet body from Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                        exit(1);
                    }
                    else {
                        body_bytes_read += bytes_in;
                    }
                }
            }

            #if DEBUG
            printf("%u: Full packet received, beginning processing.\n", (uint32_t)tid);
            #endif

            // Modify packet (if needed)
            if (packet->opcode == CQL_OPCODE_AUTHENTICATE) { // Print body of AUTHENTICATE packet
                #if DEBUG
                printf("%u:   Handling AUTHENTICATE packet from Cassandra.\n", (uint32_t)tid);

                uint16_t str_len = 0;
                memcpy(&str_len, (char *)packet + header_len, 2);
                str_len = ntohs(str_len);

                char *body = (char *)malloc(str_len + 1);
                memset(body, 0, str_len + 1);
                strncpy(body, (char *)packet + header_len + 2, str_len);
                printf("%u:     %s\n", (uint32_t)tid, body);
                free(body);

                printf("%u:   Finished with AUTHENTICATE, passing to client.\n", (uint32_t)tid);
                #endif
            }
            else { // All other packets get processed elsewhere
                // TODO
            }

            // If compression was negotiated with the client, compress the body before sending it back
            if (compression_type != CQL_COMPRESSION_NONE) {
                #if DEBUG
                printf("%u:   Need to compress packet before sending back to client.\n", (uint32_t)tid);
                #endif

                if (compression_type == CQL_COMPRESSION_LZ4) {
                    #if DEBUG
                    printf("%u:   Using lz4 compression!\n", (uint32_t)tid);
                    #endif

                    // TODO
                }
                else if (compression_type == CQL_COMPRESSION_SNAPPY) {
                    #if DEBUG
                    printf("%u:   Using snappy compression!\n", (uint32_t)tid);
                    #endif

                    // TODO
                }

                packet->flags |= CQL_FLAG_COMPRESSION;
            }

            // Send packet to client (body length may have changed, so re-get value from header)
            if (send(connfd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%u: Error sending packet to client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            free(packet);

            #if DEBUG
            printf("%u: Packet successfully sent to client.\n\n", (uint32_t)tid);
            #endif
        }
    }

    #if DEBUG
    printf("%u: Client connection terminated, thread dying.\n", (uint32_t)tid);
    #endif

    // Properly close connection to Cassandra server
    close(cassandrafd);

    free(token);

    return NULL;
}
using namespace std;
std::string process_cql_cmd(std::string st, const std::string prefix) {
	//cout << "Searching " << st << endl;
	std::string use("USE");
	std::string from("from");
	boost::regex exp("USE (.*?);");
	int i = 0;
	//create array of regex expressions
	std::vector<std::string> my_exps;
	//my_exps.push_back(boost::regex("USE (.*?);"));
	my_exps.push_back(std::string("from (.*?)[//s|;]"));
	my_exps.push_back(std::string("USE (.*?);"));
	string::const_iterator start, end;
	start = st.begin();
	end = st.end();
	boost::match_results<std::string::const_iterator> what;
	boost::match_flag_type flags = boost::match_default;
	std::string s("");
	int size = my_exps.size();
	for (; i < size ;i++){
		boost::regex exp(my_exps.at(i));
		cout << i << my_exps.at(i) << endl;
		while(boost::regex_search(start, end, what, exp, flags))
		{
			std::string str(what.str());
			vector <string> fields;
			boost::split_regex( fields, str, boost::regex( "[ ]{1,}" ) );
			cout << str << endl;
			if(fields[0].compare(use) == 0){
				std::string app( "USE " + prefix + fields[1]);
				//              cout << "Post processing: " << app << endl;
				custom_replace(st, str, app);
			} else if (fields[0].compare(from) == 0){
				cout << "Tirimo" << endl;
				std::string app( "from " + prefix + fields[1]);
				custom_replace(st, str, app);
			}
			s += str;
			//              cout << str << endl;

			start = what[0].second;
		}
		start = st.begin();
		end = st.end();
	}
	return st;
}


bool custom_replace(std::string& str, const std::string& from, const std::string& to) {
	size_t start_pos = str.find(from);
	if(start_pos == std::string::npos)
		return false;
	str.replace(start_pos, from.length(), to);
	return true;
}







