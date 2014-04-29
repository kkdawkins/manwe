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

#include "cassandra.hpp"
#include "gateway.hpp"
#include "helpers.hpp"

#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <string>

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

    // Prep the socket
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        fprintf(stderr, "Socket creation error: %s\n", strerror(errno));
        exit(1);
    }
    
    // Clear the serv_addr struct
    struct sockaddr_in serv_addr;
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

    // Thread setup for the gateway. There are two threads per connection: one for the client and one for Cassandra
    pthread_attr_t attr;
    pthread_t thread_client;

    // Do some quick pthread attr setup
    pthread_attr_init(&attr);
    
    // Main listen loop
    while (1) {
        cql_thread_t *thread_data = (cql_thread_t *)malloc(sizeof(cql_thread_t));

        // This is a blocking call!
        thread_data->clientfd = accept(listenfd, (struct sockaddr*)NULL, NULL);
        #if DEBUG
        printf("Got a connection from a client in main event loop.\n");
        #endif
        
        // Get a connection to Cassandra
        // Assumes the real Cassandra instance is listening on CASSANDRA_IP:(CASSANDRA_PORT + 1)
        #if DEBUG
        printf("Establishing connection to Cassandra listening on %s:%d.\n", CASSANDRA_IP, CASSANDRA_PORT + 1);
        #endif

        thread_data->cassandrafd = socket(AF_INET, SOCK_STREAM, 0);
        if (thread_data->cassandrafd < 0) {
            fprintf(stderr, "Socket creation error when connecting to Cassandra: %s\n", strerror(errno));
            exit(1);
        }

        // Setup sockaddr struct
        struct sockaddr_in cassandra_addr;
        memset(&cassandra_addr, 0, sizeof(cassandra_addr));
        cassandra_addr.sin_family = AF_INET;
        cassandra_addr.sin_addr.s_addr = inet_addr(CASSANDRA_IP);
        cassandra_addr.sin_port = htons(CASSANDRA_PORT + 1);

        // Bind the socket and port to the name
        if (connect(thread_data->cassandrafd, (struct sockaddr*)&cassandra_addr, sizeof(cassandra_addr)) < 0) {
            fprintf(stderr, "Socket bind error when connecting to Cassandra: %s\n", strerror(errno));
            exit(1);
        }

        // Finally, set the shared variables in thread_data so the two threads can communicate

        pthread_mutex_init(&thread_data->mutex, NULL);
        thread_data->compression_type = CQL_COMPRESSION_NONE;
        thread_data->token = (char *)malloc(TOKEN_LENGTH + 1);
        memset(thread_data->token, 0, TOKEN_LENGTH + 1);
        thread_data->interestingPackets = NULL;
        
        if (pthread_create(&thread_client, &attr, HandleConnClient, (void *)thread_data) != 0) {
            fprintf(stderr, "pthread_create failed for client thread.\n");
            exit(1);
        }
        if (pthread_create(&thread_data->cassandra, &attr, HandleConnCassandra, (void *)thread_data) != 0) {
            fprintf(stderr, "pthread_create failed for Cassandra thread.\n");
            exit(1);
        }

        /*
         * Creating the client thread as detached for 2 reasons:
         *  1. It will never rejoin
         *  2. This will save some sys resources
         */
        pthread_detach(thread_client);
    }

    // Execution will never reach here, but we need to return a value
    return 0;
}

/*
 * This method handles packets from the client, processing and rewriting queries as needed, and then forwards new packets onto the actaul Cassandra instance.
 */
void* HandleConnClient(void* td) {
    cql_thread_t *thread_data = (cql_thread_t *)td;

    // Save this thread's ID to prefix all messages with
    pthread_t tid = pthread_self();

    #if DEBUG
    printf("%u: Thread spawned for client.\n", (uint32_t)tid);
    #endif

    uint8_t header_len = sizeof(cql_packet_t); // Length of the header
    uint32_t body_len = 0; // Length of packet body
    cql_packet_t *packet = (cql_packet_t *)malloc(header_len); // Raw packet data

    int recv_ret; // Save the return value of the recv() call below, since we may need to take action in case of error

    // At the top of the loop, we are expecting the start of another CQL packet. If it doesn't look right, send back an error and close the connection.
    // INVARIANT: Before recv() is called, packet will be allocated with (cql_packet_t *)malloc(header_len).
    while (recv_ret = recv(thread_data->clientfd, packet, 1, 0), recv_ret == 1) { // Read in the first byte of the potential CQL header from the client. A value of 0 indicates clean shutdown, and less than 0 is an error
        #if DEBUG
        printf("%u: Processing packet from client.\n", (uint32_t)tid);
        #endif

        int protocol_version_in_use = -1; // Currently, the gateway supports v1 only

        // Perform some basic sanity checks to verify this looks like a CQL packet

        // The first byte must be CQL_V1_REQUEST. Version 2 of the CQL protocol isn't supported by our gateway.
        if (packet->version != CQL_V1_REQUEST) {
            #if DEBUG
            printf("%u: First byte from client is not CQL_V1_REQUEST, closing connections and killing thread.\n", (uint32_t)tid);
            #endif

            break;
        }

        // Set the protocol version to v1
        protocol_version_in_use = CQL_V1;

        // Now, read in the remaining 7 bytes of the header.
        if (recv(thread_data->clientfd, ((char *)packet) + 1, 7, 0) < 0) { //The remainder of the CQL header
            fprintf(stderr, "%u: Error reading remainder of header from client: %s\n", (uint32_t)tid, strerror(errno));

            break;
        }
        if (packet->stream < 0) { // Client request stream ids must be postitive
                                  // FIXME the python client library seems to start stream ids with "0", which isn't positive or negative
            char msg[] = "Invalid stream id";
            SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

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
                int32_t bytes_in = recv(thread_data->clientfd, (char *)packet + header_len + body_bytes_read, body_len - body_bytes_read, 0);
                if (bytes_in < 0) {
                    fprintf(stderr, "%u: Error reading packet body from client: %s\n", (uint32_t)tid, strerror(errno));

                    break;
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
            printf("%u:   Compression is not yet implemented -- exiting.\n", (uint32_t)tid);
            exit(1);
            #endif

            #if DEBUG
            printf("%u:   Packet body is compressed, decompressing.\n", (uint32_t)tid);
            #endif

            // Compression type is only ever sent once, at the beginning of the session in the first packet, so we don't need to do anything special to share between threads.
            if (thread_data->compression_type == CQL_COMPRESSION_LZ4) {
                #if DEBUG
                printf("%u:   It's lz4 compression!\n", (uint32_t)tid);
                #endif

                // TODO
            }
            else if (thread_data->compression_type == CQL_COMPRESSION_SNAPPY) {
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
                SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

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
                SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                break;
            }

            while (sm != NULL) {
                #if DEBUG
                printf("%u:     %s -> %s\n", (uint32_t)tid, sm->key, sm->value);
                #endif

                if (strcmp(sm->key, "COMPRESSION") == 0) {
                    // Compression is only set in the very first packet of the session, so we can safely write without needing to worry about the other thread
                    if (strcmp(sm->value, "lz4") == 0) {
                        thread_data->compression_type = CQL_COMPRESSION_LZ4;
                    }
                    else if (strcmp(sm->value, "snappy") == 0) {
                        thread_data->compression_type = CQL_COMPRESSION_SNAPPY;
                    }
                    else {
                        #if DEBUG
                        printf("%u:     Error - Unknown compression method '%s'.\n", (uint32_t)tid, sm->value);
                        #endif

                        char msg[] = "Unknown compression method";
                        SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                        FreeStringMap(head);
                        head = NULL; // We need to be sneaky and break out the the main processing loop. c++ doesn't allow labels on loops, so use head == NULL as the conditional for another break below.

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
            if (protocol_version_in_use != CQL_V1) { // CREDENTIALS is only used in v1 of the CQL protocol
                char msg[] = "CREDENTIALS not supported in this version of CQL";
                SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

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
                SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_BAD_CREDENTIALS, msg);

                break;
            }

            while (sm != NULL) {
                #if DEBUG
                printf("%u:     %s -> %s\n", (uint32_t)tid, sm->key, sm->value);
                #endif

                if (strcmp(sm->key, "username") == 0) {
                    if (strlen(sm->value) <= TOKEN_LENGTH) { // The supplied username must be at least TOKEN_LENGTH + 1 characters long, so we can properly grab the token and still have at least one character remaining to pass on to Cassandra.
                        #if DEBUG
                        printf("%u:       Error - Invalid token + username supplied.\n", (uint32_t)tid);
                        #endif

                        char msg[] = "Token + username is too short";
                        SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_BAD_CREDENTIALS, msg);

                        FreeStringMap(head);
                        head = NULL; // We need to be sneaky and break out the the main processing loop. c++ doesn't allow labels on loops, so use head == NULL as the conditional for another break below.

                        break;
                    }
                    else {
                        char *userToken = (char *)malloc(TOKEN_LENGTH + 1);
                        memset(userToken, 0, TOKEN_LENGTH + 1);
                        strncpy(userToken, sm->value, TOKEN_LENGTH); //Copy the token into the variable for user later on

                        #if DEBUG
                        printf("%u:       Token: %s\n", (uint32_t)tid, userToken);
                        #endif

                        // Now, validate that the supplied token is valid
                        pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
                        bool isValid = checkToken(userToken, thread_data->token, false); // The checkToken function sets the contents of 'thread_data->token' before returning
                        pthread_mutex_unlock(&thread_data->mutex); // Release mutex

                        free(userToken);

                        if (isValid) { // User token is valid
                            #if DEBUG
                            printf("%u:       Internal Token: %s\n", (uint32_t)tid, thread_data->token);
                            #endif

                            // Replace the user-supplied token with the internal one for prefixing the username
                            memcpy(sm->value, thread_data->token, TOKEN_LENGTH); // Safe to copy without mutex
                        }
                        else { // User token is invalid
                            #if DEBUG
                            printf("%u:       Error - Token supplied is not valid.\n", (uint32_t)tid);
                            #endif

                            char msg[] = "Token supplied is not valid";
                            SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_BAD_CREDENTIALS, msg);

                            FreeStringMap(head);
                            head = NULL; // We need to be sneaky and break out the the main processing loop. c++ doesn't allow labels on loops, so use head == NULL as the conditional for another break below.

                            break;
                        }

                        #if DEBUG
                        printf("%u:       Internal username: %s\n", (uint32_t)tid, sm->value);
                        #endif
                    }
                }

                sm = sm->next;
            }

            if (head == NULL) { // Previously seen error in getting user info
                break;
            }

            uint32_t new_len = 0;
            char *new_body = WriteStringMap(head, &new_len);
            memcpy((char *)packet + header_len, new_body, new_len); // We know that the body length will not change, so memory allocation will be fine.
            free(new_body);

            FreeStringMap(head);

            #if DEBUG
            printf("%u:   Finished with CREDENTIALS, passing to Cassandra.\n", (uint32_t)tid);
            #endif
        }
        else if (packet->opcode == CQL_OPCODE_OPTIONS) { // CQL OPTIONS packet
            // Nothing to do here

            #if DEBUG
            printf("%u:   Saw OPTIONS packet.\n", (uint32_t)tid);
            #endif
        }
        else if (packet->opcode == CQL_OPCODE_QUERY) { // Rewrite CQL queries if needed

            #if DEBUG
            printf("%u:   Handling QUERY packet to (possibly) prepend the internal token.\n", (uint32_t)tid);
            #endif

            int32_t query_len;
            memcpy(&query_len, (char *)packet + header_len, 4);
            query_len = ntohl(query_len);
            char *query = (char *)malloc(query_len + 1);
            memset(query, 0, query_len + 1);
            memcpy(query, (char *)packet + header_len + 4, query_len);
            uint16_t consistency;
            memcpy(&consistency, (char *)packet + header_len + 4 + query_len, 2);

            #if DEBUG
            printf("%u:     Query before rewrite: %s\n", (uint32_t)tid, query);
            #endif

            // Now, fixup the query before passing into Cassandra
            pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
            std::string cpp_string = process_cql_cmd(query, thread_data->token);
            pthread_mutex_unlock(&thread_data->mutex); // Release mutex
            const char *new_query = cpp_string.c_str();

            #if DEBUG
            printf("%u:     Query after rewrite: %s\n", (uint32_t)tid, new_query);
            #endif
                
            if (interestingPacket(cpp_string)) {
                
                #if DEBUG
                printf("%u:       Found interesting packet %d going to cassandra.\n", (uint32_t)tid, packet->stream);
                #endif
                    
                node *interesting_packet = (node *)malloc(sizeof(node));
                interesting_packet->id = packet->stream;
                interesting_packet->next = NULL;
                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the linked list
                thread_data->interestingPackets = addNode(thread_data->interestingPackets, interesting_packet);
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex
            }

            query_len = strlen(new_query);
            query_len = htonl(query_len);

            cql_packet_t *new_packet = (cql_packet_t *)malloc(14 + strlen(new_query)); // 8 byte header, 4 byte int, new_query, 2 byte consistency
            memcpy((char *)new_packet, packet, 8); // Copy header
            new_packet->length = 6 + strlen(new_query); // Fix the length field
            new_packet->length = htonl(new_packet->length);
            memcpy((char *)new_packet + 8, &query_len, 4);
            memcpy((char *)new_packet + 12, new_query, strlen(new_query));
            memcpy((char *)new_packet + 12 + strlen(new_query), &consistency, 2);

            free(packet);
            packet = new_packet;
            free(query);

            #if DEBUG
            printf("%u:   Finished with QUERY, passing to Cassandra.\n", (uint32_t)tid);
            #endif

        }
        else if (packet->opcode == CQL_OPCODE_PREPARE) { // Rewrite CQL queries if needed

            #if DEBUG
            printf("%u:   Handling PREPARE packet to (possibly) prepend the internal token.\n", (uint32_t)tid);
            #endif

            int32_t query_len;
            memcpy(&query_len, (char *)packet + header_len, 4);
            query_len = ntohl(query_len);
            char *query = (char *)malloc(query_len + 1);
            memset(query, 0, query_len + 1);
            memcpy(query, (char *)packet + header_len + 4, query_len);

            #if DEBUG
            printf("%u:     Query before rewrite: %s\n", (uint32_t)tid, query);
            #endif

            // Now, fixup the query before passing into Cassandra
            pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
            std::string cpp_string = process_cql_cmd(query, thread_data->token);
            pthread_mutex_unlock(&thread_data->mutex); // Release mutex
            const char *new_query = cpp_string.c_str();

            #if DEBUG
            printf("%u:     Query after rewrite: %s\n", (uint32_t)tid, new_query);
            #endif

            if (interestingPacket(cpp_string)) {
                
                #if DEBUG
                printf("%u:       Found interesting packet %d going to cassandra.\n", (uint32_t)tid, packet->stream);
                #endif
                    
                node *interesting_packet = (node *)malloc(sizeof(node));
                interesting_packet->id = packet->stream;
                interesting_packet->next = NULL;
                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the linked list
                thread_data->interestingPackets = addNode(thread_data->interestingPackets, interesting_packet);
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex
            }

            query_len = strlen(new_query);
            query_len = htonl(query_len);

            cql_packet_t *new_packet = (cql_packet_t *)malloc(12 + strlen(new_query)); // 8 byte header, 4 byte int, new_query
            memcpy((char *)new_packet, packet, 8); // Copy header
            new_packet->length = 4 + strlen(new_query); // Fix the length field
            new_packet->length = htonl(new_packet->length);
            memcpy((char *)new_packet + 8, &query_len, 4);
            memcpy((char *)new_packet + 12, new_query, strlen(new_query));

            free(packet);
            packet = new_packet;
            free(query);

            #if DEBUG
            printf("%u:   Finished with PREPARE, passing to Cassandra.\n", (uint32_t)tid);
            #endif

        }
        else if (packet->opcode == CQL_OPCODE_EXECUTE) { // Verify that this prepared statement belongs to the tenant submitting it

            #if DEBUG
            printf("%u:   Handling EXECUTE packet to verify user can call prepared method.\n", (uint32_t)tid);
            #endif

            uint16_t num_bytes = 0;
            memcpy(&num_bytes, (char *)packet + header_len, 2);
            num_bytes = ntohs(num_bytes);

            #if DEBUG
            assert(num_bytes > 0); // It makes no sense to supply no bytes back for the id, but the spec doesn't outlaw this
            #endif

            char *prepared_id = (char *)malloc(num_bytes);
            memcpy(prepared_id, (char *)packet + header_len + 2, num_bytes);

            // FIXME now, check that this prepared id is valid for this tenant

            // After checking the prepared id, we can ignore the rest of the packet, since it's just data being sent to Cassandra

            #if DEBUG
            printf("%u:   Finished with EXECUTE, passing to Cassandra.\n", (uint32_t)tid);
            #endif

        }
        else if (packet->opcode == CQL_OPCODE_REGISTER) { // CQL REGISTER packet
            // Nothing to do here

            #if DEBUG
            printf("%u:   Saw REGISTER packet.\n", (uint32_t)tid);
            #endif
        }
        else { // This is an error -- we got an unexpected packet from the client
            #if DEBUG
            printf("%u:   Got unexpected packet type %d from client.\n", (uint32_t)tid, packet->opcode);
            #endif

            char msg[] = "Got unexpected packet";
            SendCQLError(thread_data->clientfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

            break;
        }

        // Send packet to Cassandra (body length may have changed, so re-get value from header)
        if (send(thread_data->cassandrafd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
            fprintf(stderr, "%u: Error sending packet to Cassandra: %s\n", (uint32_t)tid, strerror(errno));
            exit(1);
        }

        free(packet);
        packet = (cql_packet_t *)malloc(header_len); // Maintain invariant for top of loop

        #if DEBUG
        printf("%u: Packet successfully sent to Cassandra.\n\n", (uint32_t)tid);
        #endif

    }

    if (recv_ret == 0) { // A clean shutdown from the client's end
        #if DEBUG
        printf("%u: Client has closed the socket.\n", (uint32_t)tid);
        #endif
    }
    else if (recv_ret == 1) { // Some error occured while processing the packet. An error has been sent to the client or stdout, so clean up things before killing threads.
        #if DEBUG
        printf("%u: Client sent the wrong first byte or some other error has already been reported.\n", (uint32_t)tid);
        #endif

        close(thread_data->clientfd);
    }
    else { // Some sort of error occurred (or the recv() timed out) when getting the first byte, so recv_ret < 0
        // TODO -- check if time out or different error

        fprintf(stderr, "%u:   Error/time out reading first byte from client: %s\n", (uint32_t)tid, strerror(errno));
    }

    #if DEBUG
    printf("%u: Client connection terminated, killing self and Cassandra thread.\n", (uint32_t)tid);
    #endif

    free(packet);

    // Kill the Cassandra thread
    pthread_cancel(thread_data->cassandra);
    pthread_join(thread_data->cassandra, NULL);

    // The client thread takes care of cleaning up shared memory
    close(thread_data->cassandrafd);

    pthread_mutex_destroy(&thread_data->mutex);

    node *head = thread_data->interestingPackets;
    while (head != NULL) {
        node *tmp = head->next;
        free(head);
        head = tmp;
    }
    free(thread_data->token);
    free(thread_data);

    #if DEBUG
    printf("%u: Both threads are dead and cleaned up.\n", (uint32_t)tid);
    #endif

    return NULL;
}

/*
 * This method handles packets from Cassandra, processing and rewriting results as needed, and then forwards new packets back to the client.
 */
void* HandleConnCassandra(void* td) {
    cql_thread_t *thread_data = (cql_thread_t *)td;

    // Save this thread's ID to prefix all messages with
    pthread_t tid = pthread_self();

    #if DEBUG
    printf("%u: Thread spawned for Cassandra.\n", (uint32_t)tid);
    #endif

    uint8_t header_len = sizeof(cql_packet_t); // Length of the header
    uint32_t body_len = 0; // Length of packet body
    cql_packet_t *packet = (cql_packet_t *)malloc(header_len); // Raw packet data

    int recv_ret; // Save the return value of the recv() call below, since we may need to take action in case of error

    // Setup a callback to free the packet buffer when this thread is killed
    pthread_cleanup_push(cassandra_thread_cleanup_handler, &packet);

    // At the top of the loop, we are expecting the start of another CQL packet. We assume Cassandra will always give us properly formed packets.
    // INVARIANT: Before recv() is called, packet will be allocated with (cql_packet_t *)malloc(header_len).
    while (recv_ret = recv(thread_data->cassandrafd, packet, header_len, 0), recv_ret == header_len) { // Read in the header from Cassandra. A value of 0 indicates clean shutdown, and less than 0 is an error
        #if DEBUG
        printf("%u: Processing packet from Cassandra.\n", (uint32_t)tid);

        assert(packet->version == CQL_V1_RESPONSE); // Currently we only support v1 of the CQL protocol, since that's what the drivers use

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
                int32_t bytes_in = recv(thread_data->cassandrafd, (char *)packet + header_len + body_bytes_read, body_len - body_bytes_read, 0);
                if (bytes_in < 0) {
                    fprintf(stderr, "%u: Error reading packet body from Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                    exit(1); // Die here, unlike when dealing with a client, since Cassandra shouldn't give bad data
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
        if (packet->opcode == CQL_OPCODE_ERROR) { // CQL ERROR packet
            #if DEBUG
            printf("%u:   Handling ERROR packet from Cassandra.\n", (uint32_t)tid);
            #endif

            int32_t error_code = 0;
            memcpy(&error_code, (char *)packet + header_len, 4);
            error_code = ntohl(error_code);

            uint16_t str_len = 0;
            memcpy(&str_len, (char *)packet + header_len + 4, 2);
            str_len = ntohs(str_len);

            char *err = (char *)malloc(str_len + 1);
            memset(err, 0, str_len + 1);
            memcpy(err, (char *)packet + header_len + 6, str_len);

            #if DEBUG
            printf("%u:     Error code: 0x%04X; msg: %s\n", (uint32_t)tid, error_code, err);
            #endif

            // Strip out the prefix token from the error string
            while (strstr(err, thread_data->token) != NULL) {
                char *p = strstr(err, thread_data->token);
                memmove(p, p + TOKEN_LENGTH, 1 + strlen(p + TOKEN_LENGTH));
            }

            #if DEBUG
            printf("%u:     Error code: 0x%04X; msg: %s\n", (uint32_t)tid, error_code, err);
            #endif

            // Now, rebuild the packet
            uint16_t new_str_len = strlen(err);

            packet->length = htonl(body_len - (str_len - new_str_len));
            new_str_len = htons(new_str_len);
            memcpy((char *)packet + header_len + 4, &new_str_len, 2);
            new_str_len = strlen(err);
            memcpy((char *)packet + header_len + 6, err, new_str_len);

            free(err);

            if (body_len > (uint32_t)str_len + 6) { // Per spec, there may be additional data after the error code and string. If so, shift that data so it remains in the packet
                memmove((char *)packet + header_len + 6 + new_str_len, (char *)packet + header_len + 6 + str_len, body_len - 6 - str_len);
            }

            if (error_code == 0x2400) { // This is an "already exists" error, and the rest of the body contains the affected keyspace and table. Need to filter the keyspace name.
                char *b = (char *)packet + header_len + 6 + new_str_len;
                uint32_t b_len = body_len - str_len - 6;
                memcpy(&str_len, b, 2);
                str_len = ntohs(str_len);

                char *ks = (char *)malloc(str_len + 1);
                memset(ks, 0, str_len + 1);
                memcpy(ks, b + 2, str_len);

                #if DEBUG
                printf("%u:       Keyspace is '%s'.\n", (uint32_t)tid, ks);
                #endif

                char *new_ks = (char *)malloc(strlen(ks) - TOKEN_LENGTH + 1);
                memset(new_ks, 0, strlen(ks) - TOKEN_LENGTH + 1);
                memcpy(new_ks, ks + TOKEN_LENGTH, strlen(ks) - TOKEN_LENGTH);
                free(ks);
                ks = new_ks;
                str_len -= TOKEN_LENGTH;

                #if DEBUG
                printf("%u:       Keyspace changed to '%s'.\n", (uint32_t)tid, ks);
                #endif

                str_len = htons(str_len);
                memcpy(b, &str_len, 2);
                memcpy(b + 2, ks, strlen(ks));
                memmove(b + 2 + strlen(ks), b + 2 + strlen(ks) + TOKEN_LENGTH, b_len - 2 - strlen(ks) - TOKEN_LENGTH);

                packet->length = htonl(ntohl(packet->length) - TOKEN_LENGTH);

                free(ks);
            }

            #if DEBUG
            printf("%u:   Finished with ERROR, passing to client.\n", (uint32_t)tid);
            #endif
        }
        else if (packet->opcode == CQL_OPCODE_READY) { // CQL READY packet
            // Nothing to do here

            #if DEBUG
            printf("%u:   Saw READY packet.\n", (uint32_t)tid);
            #endif
        }
        else if (packet->opcode == CQL_OPCODE_AUTHENTICATE) { // Print body of AUTHENTICATE packet
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
        else if (packet->opcode == CQL_OPCODE_SUPPORTED) { // CQL SUPPORTED packet
            // Nothing to do here

            #if DEBUG
            printf("%u:   Saw SUPPORTED packet.\n", (uint32_t)tid);
            #endif
        }
        else if (packet->opcode == CQL_OPCODE_RESULT) { // Process the result of a query and possibly filter if needed

            // FIXME need to consider that the tracing flag may be set. If so, there will be a [uuid] before the rest of the packet body

            #if DEBUG
            printf("%u:   Handling RESULT packet from Cassandra.\n", (uint32_t)tid);
            printf("%u:   Was not an interesting packet %d.\n", (uint32_t)tid, packet->stream);
            #endif

            int32_t result_type = 0;
            memcpy(&result_type, (char *)packet + header_len, 4); // Get the result type
            result_type = ntohl(result_type);

            if (result_type == CQL_RESULT_VOID) {
                #if DEBUG
                printf("%u:     It is a VOID result.\n", (uint32_t)tid);
                #endif

                // Nothing to do
            }
            else if (result_type == CQL_RESULT_ROWS) {
                #if DEBUG
                printf("%u:     It is a ROWS result.\n", (uint32_t)tid);
                #endif

                uint32_t offset = header_len + 4; // Because there can be a varied number of items before the rows begin, need to keep track of the offset in the packet

                // Begin by getting the metadata for the rows
                cql_result_metadata_t *metadata = ReadResultMetadata((char *)packet + offset, (uint32_t)tid);
                offset += metadata->offset; // Move the offset to the end of the metadata block

                int32_t rows_count = 0;
                memcpy(&rows_count, (char *)packet + offset, 4);
                rows_count = ntohl(rows_count);
                offset += 4;

                #if DEBUG
                printf("%u:       There are %d rows and %d columns.\n", (uint32_t)tid, rows_count, metadata->columns_count);
                #endif

                // Get the actual result data
                cql_result_cell_t *parsed_table = ReadCQLResults((char *)packet + offset, rows_count, metadata->columns_count);

                // TODO - Kevin, filter rows here
                
                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the linked list
                // An interesting packet was tagged on the way to Cassandra AND impacts a "private table"
                bool isInterestingPacket = findNode(thread_data->interestingPackets, packet->stream) && isImportantTable(metadata->table);
                thread_data->interestingPackets = removeNode(thread_data->interestingPackets, packet->stream);
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex
                
                if (isInterestingPacket) { // TODO False for now, so Mathias can ignore/delete this code to be used later
                    cql_result_cell_t *rowPtr = parsed_table;
                    cql_result_cell_t *colPtr = parsed_table;
                    cql_column_spec_t *colTypeMap = metadata->column;
                    #if DEBUG
                    printf("%u:   Begin filtering interesting packet with stream ID %d.\n", (uint32_t)tid,packet->stream);
                    #endif
                    
                    /*
                    * Scan row by row and iterate through each col
                    * As we iterate through each col, iterate through the map of col->type looking for string
                    * TODO: Should I adhear to the Cassandra system table doc?
                    */
                    int i = 0;
                    int j = 0;
                    while(rowPtr != NULL && i < rows_count){
                        colPtr = rowPtr;
                        j = 0;
                        while(colPtr != NULL && j < metadata->columns_count){
                            if(isImportantColumn(colTypeMap->name)){
                                char *terminated_content = (char *)malloc(colPtr->len + 1);
                                memset(terminated_content, 0, colPtr->len + 1);
                                memcpy(terminated_content, colPtr->content, colPtr->len);
                                if(!scanForInternalToken(terminated_content, thread_data->token) || scanforRestrictedKeyspaces(terminated_content)){
                                    // False, so the internal token did not appear in the column data, must remove
                                    #if DEBUG
                                    printf("%u:   Found a column that requires removal: %s.\n", (uint32_t)tid, terminated_content);
                                    #endif
                                    rowPtr->remove = true;
                                    //rows_count --;
                                }
                                free(terminated_content);
                            }
                            colTypeMap = colTypeMap->next;
                            colPtr = colPtr->next_col;
                            j = j + 1;
                        }
                        colTypeMap = metadata->column;
                        rowPtr = rowPtr->next_row;
                        
                        i = i + 1;
                    }
                    #if DEBUG
                    printf("Going to cleanup\n");
                    #endif
                    parsed_table = cleanup(parsed_table,(uint32_t)tid);
                    #if DEBUG
                    printf("Finished cleanup\n");
                    #endif
                }
                
            
                uint32_t buf_len = 0;
                char *new_rows = WriteCQLResults(parsed_table, &buf_len, &rows_count);

                #if DEBUG
                printf("%u:       After filtering, there are now %d rows and %d columns.\n", (uint32_t)tid, rows_count, metadata->columns_count);
                #endif

                // Now, update the packet with the new rows. Since we will only ever remove them, we don't have to worry about overflowing allocated memory.
                rows_count = htonl(rows_count);
                memcpy((char *)packet + offset - 4, &rows_count, 4);
                memcpy((char *)packet + offset, new_rows, buf_len);
                packet->length = htonl(offset - header_len + buf_len);

                free(new_rows);
                FreeCQLResults(parsed_table);
                FreeResultMetadata(metadata);
            }
            else if (result_type == CQL_RESULT_SET_KEYSPACE) {
                #if DEBUG
                printf("%u:     It is a SET_KEYSPACE result.\n", (uint32_t)tid);
                #endif

                uint16_t str_len = 0;
                memcpy(&str_len, (char *)packet + header_len + 4, 2);
                str_len = ntohs(str_len);

                char *str = (char *)malloc(str_len + 1);
                memset(str, 0, str_len + 1);
                memcpy(str, (char *)packet + header_len + 6, str_len);

                #if DEBUG
                printf("%u:       Before: '%s'.\n", (uint32_t)tid, str);
                #endif

                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
                if (strncmp(thread_data->token, str, TOKEN_LENGTH) == 0) { // keyspace begins with the internal token
                    char *new_str = (char *)malloc(strlen(str) - TOKEN_LENGTH + 1);
                    memset(new_str, 0, strlen(str) - TOKEN_LENGTH + 1);
                    memcpy(new_str, str + TOKEN_LENGTH, strlen(str) - TOKEN_LENGTH);
                    free(str);
                    str = new_str;
                }
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex

                str_len = strlen(str);
                str_len = htons(str_len);
                memcpy((char *)packet + header_len + 4, &str_len, 2);
                memcpy((char *)packet + header_len + 6, str, strlen(str));

                packet->length = 6 + strlen(str);
                packet->length = htonl(packet->length);

                #if DEBUG
                printf("%u:       After: '%s'.\n", (uint32_t)tid, str);
                #endif

                free(str);
            }
            else if (result_type == CQL_RESULT_PREPARED) {
                #if DEBUG
                printf("%u:     It is a PREPARED result.\n", (uint32_t)tid);
                #endif

                uint32_t offset = header_len + 4; // Because there can be a varied number of items, need to keep track of the offset in the packet

                uint16_t num_bytes = 0;
                memcpy(&num_bytes, (char *)packet + offset, 2);
                num_bytes = ntohs(num_bytes);
                offset += 2;

                #if DEBUG
                assert(num_bytes > 0); // It makes no sense to get no bytes back for the id, but the spec doesn't outlaw this
                #endif

                char *prepared_id = (char *)malloc(num_bytes);
                memcpy(prepared_id, (char *)packet + offset, num_bytes);
                offset += num_bytes;

                // FIXME now that we have the prepared statement id, store it so future attempts to execute it can be verified to come from the same user

                cql_result_metadata_t *metadata = ReadResultMetadata((char *)packet + offset, (uint32_t)tid);
                offset += metadata->offset; // Move the offset to the end of the metadata block

                free(prepared_id);
                FreeResultMetadata(metadata);
            }
            else if (result_type == CQL_RESULT_SCHEMA_CHANGE) {
                #if DEBUG
                printf("%u:     It is a SCHEMA_CHANGE result.\n", (uint32_t)tid);
                #endif

                uint16_t str_len = 0;
                memcpy(&str_len, (char *)packet + header_len + 4, 2);
                str_len = ntohs(str_len);

                char *change = (char *)malloc(str_len + 1);
                memset(change, 0, str_len + 1);
                memcpy(change, (char *)packet + header_len + 6, str_len);

                memcpy(&str_len, (char *)packet + header_len + 6 + strlen(change), 2);
                str_len = ntohs(str_len);

                char *keyspace = (char *)malloc(str_len + 1);
                memset(keyspace, 0, str_len + 1);
                memcpy(keyspace, (char *)packet + header_len + 8 + strlen(change), str_len);

                memcpy(&str_len, (char *)packet + header_len + 8 + strlen(change) + strlen(keyspace), 2);
                str_len = ntohs(str_len);

                char *table = (char *)malloc(str_len + 1);
                memset(table, 0, str_len + 1);
                memcpy(table, (char *)packet + header_len + 10 + strlen(change) + strlen(keyspace), str_len);

                #if DEBUG
                printf("%u:       Before: %s '%s'.'%s'.\n", (uint32_t)tid, change, keyspace, table);
                #endif

                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
                if (strncmp(thread_data->token, keyspace, TOKEN_LENGTH) == 0) { // keyspace begins with the internal token
                    char *new_keyspace = (char *)malloc(strlen(keyspace) - TOKEN_LENGTH + 1);
                    memset(new_keyspace, 0, strlen(keyspace) - TOKEN_LENGTH + 1);
                    memcpy(new_keyspace, keyspace + TOKEN_LENGTH, strlen(keyspace) - TOKEN_LENGTH);
                    free(keyspace);
                    keyspace = new_keyspace;
                }
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex

                #if DEBUG
                printf("%u:       After: %s '%s'.'%s'.\n", (uint32_t)tid, change, keyspace, table);
                #endif

                // Since we are stripping data from the strings, we don't have to worry about overflowing the packet buffer
                str_len = strlen(keyspace);
                str_len = htons(str_len);
                memcpy((char *)packet + header_len + 6 + strlen(change), &str_len, 2);
                memcpy((char *)packet + header_len + 8 + strlen(change), keyspace, strlen(keyspace));

                str_len = strlen(table);
                str_len = htons(str_len);
                memcpy((char *)packet + header_len + 8 + strlen(change) + strlen(keyspace), &str_len, 2);
                memcpy((char *)packet + header_len + 10 + strlen(change) + strlen(keyspace), table, strlen(table));

                packet->length = 10 + strlen(change) + strlen(keyspace) + strlen(table);
                packet->length = htonl(packet->length);

                free(change);
                free(keyspace);
                free(table);
            }
            else { // Error!
                #if DEBUG
                printf("%u:       Got unexpected result kind %d from Cassandra -- exiting.\n", (uint32_t)tid, result_type);
                exit(1);
                #endif
            }

            #if DEBUG
            printf("%u:   Finished with RESULT, passing to client.\n", (uint32_t)tid);
            #endif

        }
        else if (packet->opcode == CQL_OPCODE_EVENT) { // Process EVENT packet and possibly forward to client
            #if DEBUG
            printf("%u:   Handling EVENT packet from Cassandra.\n", (uint32_t)tid);
            #endif

            uint16_t str_len = 0;
            memcpy(&str_len, (char *)packet + header_len, 2);
            str_len = ntohs(str_len);

            char *event_type = (char *)malloc(str_len + 1);
            memset(event_type, 0, str_len + 1);
            strncpy(event_type, (char *)packet + header_len + 2, str_len);

            // We only want a client to know about schema changes that affect their instance. If this is for another tenant, don't send packet to client
            if (strncmp(event_type, "SCHEMA_CHANGE", 13) == 0) {
                int offset = header_len + 2 + str_len;

                memcpy(&str_len, (char *)packet + offset, 2);
                str_len = ntohs(str_len);
                offset += 2;

                char *change = (char *)malloc(str_len + 1);
                memset(change, 0, str_len + 1);
                memcpy(change, (char *)packet + offset, str_len);
                offset += str_len;

                memcpy(&str_len, (char *)packet + offset, 2);
                str_len = ntohs(str_len);
                offset += 2;

                char *keyspace = (char *)malloc(str_len + 1);
                memset(keyspace, 0, str_len + 1);
                memcpy(keyspace, (char *)packet + offset, str_len);
                offset += str_len;

                memcpy(&str_len, (char *)packet + offset, 2);
                str_len = ntohs(str_len);
                offset += 2;
 
                char *table = (char *)malloc(str_len + 1);
                memset(table, 0, str_len + 1);
                memcpy(table, (char *)packet + offset, str_len);

                #if DEBUG
                printf("%u:     Before: %s '%s'.'%s'.\n", (uint32_t)tid, change, keyspace, table);
                #endif

                pthread_mutex_lock(&thread_data->mutex); // Acquire the mutex before changing the token
                if (strncmp(thread_data->token, keyspace, TOKEN_LENGTH) == 0) { // keyspace begins with the internal token, so strip and forward packet to client
                    char *new_keyspace = (char *)malloc(strlen(keyspace) - TOKEN_LENGTH + 1);
                    memset(new_keyspace, 0, strlen(keyspace) - TOKEN_LENGTH + 1);
                    memcpy(new_keyspace, keyspace + TOKEN_LENGTH, strlen(keyspace) - TOKEN_LENGTH);
                    free(keyspace);
                    keyspace = new_keyspace;
                }
                else { // keyspace is not tenant's -- drop packet
                    #if DEBUG
                    printf("%u:     This schema change is not for this client -- dropping packet.\n", (uint32_t)tid);
                    #endif

                    free(change);
                    free(keyspace);
                    free(table);
                    free(event_type);

                    free(packet);
                    packet = (cql_packet_t *)malloc(header_len);

                    pthread_mutex_unlock(&thread_data->mutex); // Release mutex before continuing next loop iteration

                    continue;
                }
                pthread_mutex_unlock(&thread_data->mutex); // Release mutex

                #if DEBUG
                printf("%u:     After: %s '%s'.'%s'.\n", (uint32_t)tid, change, keyspace, table);
                #endif

                // Since we are stripping data from the strings, we don't have to worry about overflowing the packet buffer
                str_len = strlen(keyspace);
                str_len = htons(str_len);
                memcpy((char *)packet + header_len + 4 + strlen(event_type) + strlen(change), &str_len, 2);
                memcpy((char *)packet + header_len + 6 + strlen(event_type) + strlen(change), keyspace, strlen(keyspace));

                str_len = strlen(table);
                str_len = htons(str_len);
                memcpy((char *)packet + header_len + 6 + strlen(event_type) + strlen(change) + strlen(keyspace), &str_len, 2);
                memcpy((char *)packet + header_len + 8 + strlen(event_type) + strlen(change) + strlen(keyspace), table, strlen(table));

                packet->length = 8 + strlen(event_type) + strlen(change) + strlen(keyspace) + strlen(table);
                packet->length = htonl(packet->length);

                free(change);
                free(keyspace);
                free(table);
            }

            free(event_type);

            #if DEBUG
            printf("%u:   Finished with EVENT, passing to client.\n", (uint32_t)tid);
            #endif
        }
        else { // This is an error -- we got an unexpected packet from Cassandra
            #if DEBUG
            printf("%u:   Got unexpected packet type %d from Cassandra -- exiting.\n", (uint32_t)tid, packet->opcode);
            exit(1);
            #endif
        }

        // If compression was negotiated with the client, compress the body before sending it back
        // Don't need to lock since the variable is only set once at the beginning of the session
        if (thread_data->compression_type != CQL_COMPRESSION_NONE) {
            #if DEBUG
            printf("%u:   Compression is not yet implemented -- exiting.\n", (uint32_t)tid);
            exit(1);
            #endif

            #if DEBUG
            printf("%u:   Need to compress packet before sending back to client.\n", (uint32_t)tid);
            #endif

            if (thread_data->compression_type == CQL_COMPRESSION_LZ4) {
                #if DEBUG
                printf("%u:   Using lz4 compression!\n", (uint32_t)tid);
                #endif

                // TODO
            }
            else if (thread_data->compression_type == CQL_COMPRESSION_SNAPPY) {
                #if DEBUG
                printf("%u:   Using snappy compression!\n", (uint32_t)tid);
                #endif

                // TODO
            }

            packet->flags |= CQL_FLAG_COMPRESSION;
        }

        // Send packet to client (body length may have changed, so re-get value from header)
        if (send(thread_data->clientfd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
            fprintf(stderr, "%u: Error sending packet to client: %s\n", (uint32_t)tid, strerror(errno));
            exit(1);
        }

        free(packet);
        packet = (cql_packet_t *)malloc(header_len); // Maintain invariant for top of loop

        #if DEBUG
        printf("%u: Packet successfully sent to client.\n\n", (uint32_t)tid);
        #endif
    }

    // The client thread will take are of closing sockets and freeing shared memory. We should never reach this point in the Cassandra thread.

    #if DEBUG
    printf("%u: Unexpectedly reached end of Cassandra processing loop.\n", (uint32_t)tid);
    exit(1);
    #endif

    pthread_cleanup_pop(0); // Need a matching pop() to the push() above, since on Linux systems these calls are really macros
    return NULL;
}

using namespace std;
std::string process_cql_cmd(string st, string prefix) {
	std::string use("USE");
	std::string from("FROM");
	std::string keyspace("KEYSPACE");
	std::string user("USER");
	std::string into("INTO");
	std::string update("UPDATE");
	std::string schema("SCHEMA");
	std::string table("TABLE");
	std::string on("ON");
	std::string to("TO");
	std::string of("OF");
	std::string stable = prefix;
	//initialize map of replacements
	std::map<std::string, std::string> replacements;
	std::map<std::string, std::string>::iterator traverser;
	int i = 0;
	//int flag = 0;
	std::size_t found;
	//create array of regex expressions
	std::vector<std::string> my_exps;
	std::string sys("system");
	//push various regular expressions
	my_exps.push_back(std::string("FROM (.*?)(([ ]{1,})|;)"));
	my_exps.push_back(std::string("INTO (.*?)[ ]{1,}"));
	my_exps.push_back(std::string("USE[\\s]+[A-Za-z0-9\"]+(;)*"));
	my_exps.push_back(std::string("(KEYSPACE|SCHEMA) (IF NOT EXISTS )*[A-Za-z0-9]+(([ ]{1,})|;)"));
	my_exps.push_back(std::string("USER[\\s]+[A-Za-z0-9\']+(([ ]{1,})|;)"));
	my_exps.push_back(std::string("TO[\\s]+[A-Za-z0-9\']+(([ ]{1,})|;)"));
	my_exps.push_back(std::string("OF[\\s]+[A-Za-z0-9\']+(([ ]{1,})|;)"));
	my_exps.push_back(std::string("UPDATE (.*?)[ ]{1,}"));
	my_exps.push_back(std::string("TABLE (.*?)(([ ]{1,})|;)"));
	my_exps.push_back(std::string("ON (.*?)(([ ]{1,})|;)"));
	string::const_iterator start, end;
	start = st.begin();
	end = st.end();
	boost::match_results<std::string::const_iterator> what;
	boost::match_flag_type flags = boost::match_default;
	std::string dot(".");
	int size = my_exps.size();
	//Find matches and prefix keyspaces
	for (; i < size ;i++){
		boost::regex exp(my_exps.at(i), boost::regex_constants::icase);
		while(boost::regex_search(start, end, what, exp, flags))
		{
			std::string str(what.str());
			boost::trim(str);
			vector <string> fields;
			std::string holder("");
			boost::split_regex( fields, str, boost::regex( "[\\s]{1,}" ) );
			holder = fields[1];
			boost::to_lower(holder);
			boost::to_upper(fields[0]);
			std::string colon(";");
			found = holder.find(sys);
	                if (found != std::string::npos || 
(fields.size() == 2 && fields[1].compare(colon) == 0)){
                                #if DEBUG
                                cout << "System table found at pos: " << found << endl;
                                #endif
                                start = what[0].second;
				continue;
                        }
			int bx = fields.size();
			std::string quote("\"");
			std::string single("\'");
			if ( custom_replace(fields[bx - 1], std::string("\""), std::string(""))){
				prefix = quote + prefix; 
			}
			if ( custom_replace(fields[bx - 1], std::string("\'"), std::string(""))){
				prefix = single + prefix; 
			}
			if(fields[0].compare(use) == 0){
				std::string app( "USE " + prefix + fields[1]);
				replacements[str] = app;			
			} else if(fields[0].compare(to) == 0){
				std::string app( "TO " + prefix + fields[1]);
				replacements[str] = app;
			} else if(fields[0].compare(of) == 0){
				
				std::string app( "OF " + prefix + fields[1]);
				replacements[str] = app;
			} else if(fields[0].compare(table) == 0){
				found = fields[1].find('.');
				if (found!=std::string::npos){
                                std::string app( "TABLE " + prefix + fields[1]);
                                replacements[str] = app;
				}
                        } else if (fields[0].compare(from) == 0 ){
				found = fields[1].find('.');
				if (found!=std::string::npos){
					std::string app( "FROM " + prefix + fields[1]);
					replacements[str] = app; 
				}
			} else if(fields[0].compare(on) == 0){
				found = fields[1].find('.');
				if (found!=std::string::npos){
                                std::string app( "ON " + prefix + fields[1]);
                                replacements[str] = app;
				}
                        } else if (fields[0].compare(keyspace) == 0 || fields[0].compare(schema) == 0){
				int n = fields.size();
				std::string feed("");
				int j = 0;
				fields[n - 1] = prefix + fields[n - 1];
				while ( j < n ){
					feed = feed + fields[j] + ((j == n - 1) ? "" : " ");
					j++;
				}
				replacements[str] = feed;  
			} else if (fields[0].compare(user) == 0 ){
				int n = fields.size();
				std::string feed("");
				int j = 0;
				fields[n - 1] = prefix + fields[n - 1];
				while ( j < n ){
					feed = feed + fields[j] + ((j == n - 1) ? "" : " ");
					j++;
				}
				replacements[str] = feed;  
			} else if(fields[0].compare(into) == 0 ) {
                                std::string app( "INTO " + prefix + fields[1]);
                                replacements[str] = app;
                        } else if(fields[0].compare(update) == 0 ){
                                std::string app( "UPDATE " + prefix + fields[1]);
                                replacements[str] = app;
                        }
			start = what[0].second;
			prefix = stable;
		}
		start = st.begin();
		end = st.end();
		prefix = stable;
	}
	//perform replacements on supplied query
	for (traverser = replacements.begin(); traverser != replacements.end(); ++traverser){	
		found = traverser->second.find('.');
                        if (found!=std::string::npos){
                                #if DEBUG
                                cout << "Dot found" << endl;
                                #endif
                                // custom_replace(traverser->second, dot, dot+prefix);
                        } 
		custom_replace(st, traverser->first, traverser->second);
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

bool interestingPacket(std::string st){
    std::string sys("system");
    std::string permissions("permissions");
    std::string users("users");
    
    boost::to_lower(st); // for case insensitivity
    
    if(strMatch(st.find(sys), st)){
        return true;
    }else if(strMatch(st.find(permissions), st)){
        return true;
    }else if(strMatch(st.find(users), st)){
        return true;
    }else
        return false;
}

bool strMatch(std::size_t match, std::string st){
    if (match == std::string::npos){
        // The system was not found in the query
        return false;
    }else if(st.at(match-1) == ' '){ // this is to prevent similar keyspaces (eg mysystem) from being caught)
        return true;
    }else
        return true;
}
