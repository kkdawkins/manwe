/*
 * gateway.c - Gateway for Cassandra
 * CSC 652 - 2014
 */
#include "gateway.h"



void* HandleConn(void* thread_data){
    char *buf;
    unsigned int data_length;
    int connfd = *(int *)thread_data;
    
    // Need to free asap
    free(thread_data);
    
    // Need to put in a loop
    
    // Step 1: Read the 4 byte header to get length
    if(recv(connfd, (void *) &data_length, sizeof(data_length), NULL) < 0){
        fprintf(stderr,"Error reading header length: %s\n",strerror(errno));
        exit(-1);
    }
    
    buf = malloc(data_length);
    if(!buf){
        fprintf(stderr, "Malloc failed during TCP recv\n");
        exit(-1);
    }
    
    // Step 2: Read that amount of data
    if(recv(connfd, buf, data_length, NULL)  < 0){
        fprintf(stderr,"Error reading TCP data: %s\n",strerror(errno));
        exit(-1);
    }
    
    
}


/*
 * Main loop of gateway
 * Return 1 on success, -1 on error.
*/
int main(){
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
    if(listenfd < 0){
        fprintf(stderr,"Socket creation error: %s\n",strerror(errno));
        exit(-1);
    }
    
    // Clear the serv_addr struct
    memset(&serv_addr, '0', sizeof(serv_addr));
    
    serv_addr.sin_family = AF_INET;
    
    /*
     * TODO: Is this correct? Accept *any* connections?
     */
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(CASSANDRA_PORT);
    
    
    // Bind the socket and port to the name
    if(bind(listenfd, (struct sockaddr*)&serv_addr,sizeof(serv_addr)) < 0){
        fprintf(stderr,"Socket bind error: %s\n",strerror(errno));
        exit(-1);
    }
    
    // Listen for connections, with a backlog of 10
    /*
     * TODO: Is 10 proper? Should we do more/less?
     */
    if(listen(listenfd, 10) == -1){
        fprintf(stderr,"Socket listen error: %s\n",strerror(errno));

        exit(-1);
    }
    
    // Main listen loop
    while(1){
        // This is a blocking call!
        connfd = accept(listenfd, (struct sockaddr*)NULL ,NULL);
        
        // Must free in function, now threads have their own conn addr.
        thread_data = malloc(sizeof(int));
        *thread_data = connfd;
        
        if(pthread_create(&thread, &attr, HandleConn, (void *)thread_data) != 0){
            fprintf(stderr,"pthread_create failed.\n");
            exit(-1);
        }
        /*
         * Creating the threads as detached for 2 reasons:
         *  1. They will never rejoin
         *  2. This will save some sys resources
         */
        pthread_detach(thread);
        
    }
    
    return 0;
}
