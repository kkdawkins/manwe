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

const char* prefix_cmd(char *cql_cmd){
	pcre *reCompiled;
        pcre_extra *pcreExtra;
        const char *pcreErrorStr;
        int pcreErrorOffset;
        int subStrVec[30];
        const char *psubStrMatchStr;
        int i;
        int pcreExecRet;
        char *aStrRegex;
        char **aLineToMatch;

	aStrRegex = "(.*)(hello)+";
        printf("Regex to use: %s\n", aStrRegex);

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
          pcre_get_substring(*aLineToMatch, subStrVec, pcreExecRet, i, &(psubStrMatchStr));
          printf("Match(%2d/%2d): (%2d,%2d): '%s'\n", i, pcreExecRet-1,subStrVec[i*2],subStrVec[i*2+1], psubStrMatchStr);
        }

        pcre_free_substring(psubStrMatchStr);
        }
	 pcre_free(reCompiled);

        if(pcreExtra != NULL){
                pcre_free(pcreExtra);
        }

	return psubStrMatchStr;
}
