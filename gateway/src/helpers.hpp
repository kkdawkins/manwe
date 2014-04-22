#ifndef _HELPERS_H
#define _HELPERS_H

#include <stdint.h>

// Represent string maps that CQL uses
typedef struct cql_string_map {
  char *key;
  char *value;
  struct cql_string_map *next;
} cql_string_map_t;

// Represent the rows / columns of a CQL result
typedef struct cql_result_cell {
  char *content; // generic storage of the contents of this particular cell
  int32_t len;   // length of content buffer
  struct cql_result_cell *next_col; // pointer to the next column in this row
  struct cql_result_cell *next_row; // pointer to the next row (first column only)
} cql_result_cell_t;

// Node for linked list to keep track of "interesting" packets
typedef struct node {
  int8_t id; // Stream filed from cql_packet
  node *next;
} node;

void SendCQLError(int sock, uint32_t tid, uint32_t err, char *msg);

cql_string_map_t* ReadStringMap(char *buf);
char* WriteStringMap(cql_string_map_t *sm, uint32_t *new_len);
void FreeStringMap(cql_string_map_t *sm);

cql_result_cell_t* ReadCQLResults(char *buf, int32_t rows, int32_t cols);
char* WriteCQLResults(cql_result_cell_t *rows, uint32_t *new_len, int32_t *new_rows);
void FreeCQLResults(cql_result_cell_t *rows);

void gracefulExit(int sig);
void cassandra_thread_cleanup_handler(void *arg);

node* addNode(node *head, node *toAdd);
bool findNode(node *head, int8_t stream_id);

#endif
