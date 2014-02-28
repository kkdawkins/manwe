
// SYSTEM DEFINE SECTION

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <pcre.h>
// CONSTANT DEFINE SECTION

#define CASSANDRA_PORT 9160

const char* prefix_cmd(char *cql_cmd);
