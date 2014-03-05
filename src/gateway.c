/*
 * gateway.c - Gateway for Cassandra
 * CSC 652 - 2014
 */

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

#include "gateway.h"

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
        thread_data = malloc(sizeof(int));
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
    printf("%u: Thread spawned.\n", (uint32_t)tid);
    #endif

    // Before entering the processing loop, we need to establish a connection to the real Cassandra

    #if DEBUG
    printf("%u: Establishing connection to Cassandra listening on 127.0.0.1:%d.\n", (uint32_t)tid, CASSANDRA_PORT + 1);
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
    cassandra_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    cassandra_addr.sin_port = htons(CASSANDRA_PORT + 1);

    // Bind the socket and port to the name
    if (connect(cassandrafd, (struct sockaddr*)&cassandra_addr, sizeof(cassandra_addr)) < 0) {
        fprintf(stderr, "%u: Socket bind error: %s\n", (uint32_t)tid, strerror(errno));
        exit(1);
    }

    // Now, enter the packet processing loop

    #if DEBUG
    printf("%u: Entering the packet processing loop.\n", (uint32_t)tid);
    #endif

    cql_packet_t *packet;
    uint32_t header_len = sizeof(cql_packet_t);
    uint32_t body_len;
    int bytesAvail = 0;

    while (1) {
        // At the top of the loop, we are expecting the start of another CQL packet. If it doesn't look right, send back an error and close the connection.
        // We poll each socket to see if any data is present for us to read. If not, we check the other one, then repeat. Hopefully this won't tax the CPU too
        // much and will give good performance by not being stuck processing lots of packets from only one direction.

        // FIXME There's not a one-to-one correspondance of client to server packets / this first approach may be too CPU intensive.
        //       We may need to use non-blocking I/O.

        // Test to see if any bytes have arrived from the client
        // FIXME when client closes connection, we should break out of this loop, otherwise we may crash when reading a closed socket!
        ioctl(connfd, FIONREAD, &bytesAvail);
        if (bytesAvail > 0) {

            #if DEBUG
            printf("%u: Processing packet from client.\n", (uint32_t)tid);
            #endif

            // Get packet from client
            packet = malloc(header_len);
            // FIXME need to come back and do proper sanity checks on data from client (ie, verify proper CQL version, etc)
            if (recv(connfd, packet, header_len, 0) < 0) { //The CQL header is 8 bytes
                fprintf(stderr, "%u: Error reading packet header from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            body_len = ntohl(packet->length);

            // Allocate more memory for rest of packet
            void *newpacket = realloc(packet, header_len + body_len);
            if (newpacket == NULL) {
                fprintf(stderr, "%u: Filed to realloc memory for packet body!\n", (uint32_t)tid);
                exit(1);
            }
            else {
                free(packet);
                packet = newpacket;
            }

            // Read in body of packet
            if (recv(connfd, packet + header_len, body_len, 0) < 0) { //Get the rest of the body
                fprintf(stderr, "%u: Error reading packet body from client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            // Modify packet (if needed)

            // Send packet to Cassandra
            if (send(cassandrafd, packet, header_len + body_len, 0) < 0) { //Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%u: Error sending packet to Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            free(packet);
        }

        // Test to see if any bytes have arrived from Cassandra
        ioctl(cassandrafd, FIONREAD, &bytesAvail);
        if (bytesAvail > 0) {

            #if DEBUG
            printf("%u: Processing packet from Cassandra.\n", (uint32_t)tid);
            #endif

            // Get packet back from Cassandra. We assume Cassandra will always give us properly formed packets.
            packet = malloc(header_len);
            if (recv(cassandrafd, packet, header_len, 0) < 0) { //The CQL header is 8 bytes
                fprintf(stderr, "%u: Error reading packet header from Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }
            body_len = ntohl(packet->length);

            // Allocate more memory for rest of packet
            void *newpacket = realloc(packet, header_len + body_len);
            if (newpacket == NULL) {
                fprintf(stderr, "%u: Filed to realloc memory for packet body!\n", (uint32_t)tid);
                exit(1);
            }
            else {
                free(packet);
                packet = newpacket;
            }

            // Read in body of packet
            if (recv(cassandrafd, packet + header_len, body_len, 0) < 0) { //Get the rest of the body
                fprintf(stderr, "%u: Error reading packet body from Cassandra: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            // Modify packet (if needed)

            // Send packet to client
            if (send(connfd, packet, header_len + body_len, 0) < 0) { //Packet total size is header + body => 8 + packet->length
                fprintf(stderr, "%u: Error sending packet to client: %s\n", (uint32_t)tid, strerror(errno));
                exit(1);
            }

            free(packet);
        }
    }

    #if DEBUG
    printf("%u: Client connection terminated, thread dying.\n", (uint32_t)tid);
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
        int i;
        int pcreExecRet;
        char *aStrRegex;

	//chomp newline character and replace whenever found
	int length = strlen(cql_cmd);
	int j = 0;
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

