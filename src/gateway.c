#include "gateway.h"



void* HandleConn(void* thread_data){
    int connfd = *(int *)thread_data;
    free(thread_data);
    
    
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
     * TODO: Is 10 proper?
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
