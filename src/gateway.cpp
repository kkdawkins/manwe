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
// #include "cassandra.hpp"
/*
 * Main processing loop of gateway. Spawns individual threads to handle each incoming TCP connection from a client.
 * Return 0 on success (never reached, since it will listen for connections until killed), 1 on error.
*/
int main(int argc, char *argv[]) {
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
 * Assumes the real Cassandra instance is listening on 127.0.0.1:(CASSANDRA_PORT + 1).
 */
void* HandleConn(void* thread_data) {
    int connfd = *(int *)thread_data;

    // Need to free asap
    free(thread_data);

    // Save this thread's ID to prefix all messages with
    pthread_t tid = pthread_self();

    #if DEBUG
    printf("%lu: Thread spawned.\n", (unsigned long)tid);
    #endif

    // Before entering the processing loop, we need to establish a connection to the real Cassandra

    #if DEBUG
    printf("%lu: Establishing connection to Cassandra listening on 127.0.0.1:%d.\n", (unsigned long)tid, CASSANDRA_PORT + 1);
    #endif

    struct sockaddr_in cassandra_addr;
    int cassandrafd = socket(AF_INET, SOCK_STREAM, 0);
    if (cassandrafd < 0) {
        fprintf(stderr, "%lu: Socket creation error: %s\n", (unsigned long)tid, strerror(errno));
        exit(1);
    }

    // Setup sockaddr struct
    memset(&cassandra_addr, 0, sizeof(cassandra_addr));
    cassandra_addr.sin_family = AF_INET;
    cassandra_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    cassandra_addr.sin_port = htons(CASSANDRA_PORT + 1);

    // Bind the socket and port to the name
    if (connect(cassandrafd, (struct sockaddr*)&cassandra_addr, sizeof(cassandra_addr)) < 0) {
        fprintf(stderr, "%lu: Socket bind error: %s\n", (unsigned long)tid, strerror(errno));
        exit(1);
    }

    // Now, enter the packet processing loop

    #if DEBUG
    printf("%lu: Entering the packet processing loop.\n", (unsigned long)tid);
    #endif

    cql_packet_t *packet = NULL;
    uint8_t header_len = sizeof(cql_packet_t);
    uint32_t body_len = 0;
    int compression_type = CQL_COMPRESSION_NONE;
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
            printf("%lu: Processing packet from client.\n", (unsigned long)tid);
            #endif

            // Get packet from client
            packet = (cql_packet_t *)malloc(header_len);

            // Perform some basic sanity checks to verify this looks like a CQL packet

            // The first byte must be CQL_V2_REQUEST. Version 1 of the CQL protocol isn't supported by our gateway.
            if (recv(connfd, packet, 1, 0) < 0) { //The first byte of the potential CQL header
                fprintf(stderr, "%u: Error reading first byte from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            if (ntohs(packet->version) != CQL_V2_REQUEST) { // Verify version
                #if DEBUG
                printf("%u: First byte from client is not CQL_V2_REQUEST, closing connections and killing thread.\n", (uint32_t)tid);
                #endif

                free(packet);
                close(connfd);
                break;
            }

            // Now, read in the remaining 7 bytes of the header.
            if (recv(connfd, ((char *)packet) + 1, 7, 0) < 0) { //The remainder of the CQL header
                fprintf(stderr, "%u: Error reading remainder of header from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            if (ntohs(packet->stream) <= 0) { // Client request stream ids must be postitive
                char msg[] = "Invalid stream id";
                SendCQLError(connfd, (uint32_t)tid, CQL_ERROR_PROTOCOL_ERROR, msg);

                free(packet);
                close(connfd);
                break;
            }

            body_len = ntohl(packet->length);

            // Allocate more memory for rest of packet
            cql_packet_t *newpacket = (cql_packet_t *)realloc(packet, header_len + body_len);
            if (newpacket == NULL) {
                fprintf(stderr, "%lu: Failed to realloc memory for packet body!\n", (unsigned long)tid);
                exit(1);
            }
            else {
                free(packet);
                packet = newpacket;
            }

            // Read in body of packet
            if (recv(connfd, packet + header_len, body_len, 0) < 0) { // Get the rest of the body
                fprintf(stderr, "%lu: Error reading packet body from client: %s\n", (unsigned long)tid, strerror(errno));
                exit(1);
            }

            // If the packet is compressed, decompress the body. Note that we always send uncompressed packets to Cassandra itself, since
            // we're communicating directly on the same host.
            if (ntohs(packet->flags) & CQL_FLAG_COMPRESSION) {
                if (compression_type == CQL_COMPRESSION_LZ4) {
                    // TODO
                }
                else if (compression_type == CQL_COMPRESSION_SNAPPY) {
                    // TODO
                }

                packet->flags = htons(ntohs(packet->flags) & ~CQL_FLAG_COMPRESSION);
            }

            // Modify packet (if needed)
            if (ntohs(packet->opcode) == CQL_OPCODE_STARTUP) { // Handle STARTUP packet here, since we may need to set variables for the connection regarding compression
                cql_string_map_t *sm = ReadStringMap((char *)packet + header_len);
                cql_string_map_t *head = sm;

                while (sm != NULL) {
                    if (strcmp(sm->key, "COMPRESSION") == 0) {
                        if (strcmp(sm->value, "lz4") == 0) {
                            compression_type = CQL_COMPRESSION_LZ4;
                        }
                        else if (strcmp(sm->value, "snappy") == 0) {
                            compression_type = CQL_COMPRESSION_SNAPPY;
                        }
                        else {
                            // This is an error
                        }

                        // Strip compression from the STARTUP message before passing to Cassandra
                        cql_string_map_t *next = sm->next;
                        free(sm->key);
                        free(sm->value);
                        free(sm);
                        sm = next;
                    }
                }

                uint32_t new_len = WriteStringMap(head, (char *)packet + header_len);
                packet->length = htonl(new_len);

                free(head);
            }
            else { // All other packets get processed elsewhere
                // TODO
            }

            // Send packet to Cassandra (body length may have changed, so re-get value from header)
            if (send(cassandrafd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%lu: Error sending packet to Cassandra: %s\n", (unsigned long)tid, strerror(errno));
                exit(1);
            }

            free(packet);
        }

        // Test to see if any bytes have arrived from Cassandra
        ioctl(cassandrafd, FIONREAD, &bytes_avail);
        if (bytes_avail > 0) {

            #if DEBUG
            printf("%lu: Processing packet from Cassandra.\n", (unsigned long)tid);
            #endif

            // Get packet back from Cassandra. We assume Cassandra will always give us properly formed packets.
            packet = (cql_packet_t *)malloc(header_len);
            if (recv(cassandrafd, packet, header_len, 0) < 0) { // The CQL header is 8 bytes
                fprintf(stderr, "%lu: Error reading packet header from Cassandra: %s\n", (unsigned long)tid, strerror(errno));
                exit(1);
            }
            body_len = ntohl(packet->length);

            // Allocate more memory for rest of packet
            cql_packet_t *newpacket = (cql_packet_t *)realloc(packet, header_len + body_len);
            if (newpacket == NULL) {
                fprintf(stderr, "%lu: Failed to realloc memory for packet body!\n", (unsigned long)tid);
                exit(1);
            }
            else {
                free(packet);
                packet = newpacket;
            }

            // Read in body of packet
            if (recv(cassandrafd, packet + header_len, body_len, 0) < 0) { // Get the rest of the body
                fprintf(stderr, "%lu: Error reading packet body from Cassandra: %s\n", (unsigned long)tid, strerror(errno));
                exit(1);
            }

            // Modify packet (if needed)
            // TODO call function to process packet

            // If compression was negotiated with the client, compress the body before sending it back
            if (compression_type != CQL_COMPRESSION_NONE) {
                if (compression_type == CQL_COMPRESSION_LZ4) {
                    // TODO
                }
                else if (compression_type == CQL_COMPRESSION_SNAPPY) {
                    // TODO
                }

                packet->flags = htons(ntohs(packet->flags) | CQL_FLAG_COMPRESSION);
            }

            // Send packet to client (body length may have changed, so re-get value from header)
            if (send(connfd, packet, header_len + ntohl(packet->length), 0) < 0) { // Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%lu: Error sending packet to client: %s\n", (unsigned long)tid, strerror(errno));
                exit(1);
            }

            free(packet);
        }
    }

    #if DEBUG
    printf("%lu: Client connection terminated, thread dying.\n", (unsigned long)tid);
    #endif

    // Properly close connection to Cassandra server
    close(cassandrafd);

    return NULL;
}

char* prefix_cmd(char *cql_cmd, char *prefix){
	pcre *reCompiled;
	pcre_extra *pcreExtra;
	const char *pcreErrorStr;
	int pcreErrorOffset;
	int subStrVec[30];
	const char *psubStrMatchStr;
	char *p_string = (char *)malloc(100);
	int i = 0, j = 0;
	int length;
	int pcreExecRet;
	const char *aStrRegex;

	//chomp newline character and replace whenever found
	length = strlen(cql_cmd);
	while(j < length){
		if(cql_cmd[j] == '\n'){
			cql_cmd[j] = ' ';
		}
		j++;
	}

	//Regex to match
	aStrRegex = "USE[ ]+(.*);";
	printf("Regex to use: %s, %s\n", aStrRegex, prefix);

	reCompiled = pcre_compile(aStrRegex, 0, &pcreErrorStr,
			&pcreErrorOffset, NULL );

	if(reCompiled == NULL){
		printf("ERROR: Compilation failed: '%s'", pcreErrorStr);
		exit(1);
	}

	// Regex optimization
	pcreExtra = pcre_study(reCompiled, 0, &pcreErrorStr);
	if(pcreErrorStr != NULL) {
		printf("ERROR: Something went wrong in optimization attempt:'%s'\n", pcreErrorStr);
		exit(1);
	}

	pcreExecRet = pcre_exec(reCompiled,
			pcreExtra,
			cql_cmd,
			strlen(cql_cmd),
			0,
			0,
			subStrVec,
			30);
	if(pcreExecRet < 0) {
		switch(pcreExecRet){
			case PCRE_ERROR_NOMATCH     :printf("String did not match\n");
						     break;
			case PCRE_ERROR_NULL        :printf("Encountered null\n");
						     break;
			case PCRE_ERROR_BADOPTION   :printf("A bad option was passed\n");
						     break;
			case PCRE_ERROR_BADMAGIC    :printf("Magic number bad\n");
						     break;
			case PCRE_ERROR_UNKNOWN_NODE:printf("Something bad in recompiled regex\n");
						     break;
			case PCRE_ERROR_NOMEMORY    :printf("Ran out of memory\n");
						     break;
			default                     :printf("Unknown error\n");
						     break;
		}
	}
	else{
		printf("######A match was found#######\n");

		if(pcreExecRet == 0){
			printf("Too many substrings for substring vector\n");
			pcreExecRet = 10;
		}

		for(i=0;i<pcreExecRet;i++){
			pcre_get_substring(cql_cmd, subStrVec, pcreExecRet, i, &(psubStrMatchStr));
			printf("Match(%2d/%2d): (%2d,%2d): '%s'\n", i, pcreExecRet-1,subStrVec[i*2],subStrVec[i*2+1], psubStrMatchStr);
			if(i){
				strcpy(p_string, psubStrMatchStr);
				prepend(p_string, "USE 123abc");
				printf("Result of processing: %s\n", p_string);
			}

		}
		pcre_free_substring(psubStrMatchStr);
	}
	pcre_free(reCompiled);

	if(pcreExtra != NULL){
		pcre_free(pcreExtra);
	}

	return p_string;
}

void prepend(char* s, const char* t)
{
	size_t len = strlen(t);
	size_t i;

	memmove(s + len, s, len + 1);

	for (i = 0; i < len; ++i)
	{
		s[i] = t[i];
	}
}

const char* process_cql_command(char *cql_cmd, char *prefix){
	std::string cmd = cql_cmd;	
	std::string pfix = prefix;
	//std::string use_regex = "USE[ ]+(.*);";
	boost::regex expression("USE[ ]+(.*);");
	//check if CQL command matches regex
	if(boost::regex_match(cmd, expression)){		
		cmd.replace(4, 0, pfix); 
	}

	return cmd.c_str();

}










