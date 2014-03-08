#ifndef _HELPERS_H
#define _HELPERS_H

#include <stdint.h>

// Represent string maps that CQL uses
typedef struct cql_string_map {
  char *key;
  char *value;
  struct cql_string_map *next;
} cql_string_map_t;

void SendCQLError(int sock, uint32_t tid, uint32_t err, char *msg);

cql_string_map_t* ReadStringMap(char *buf);
char* WriteStringMap(cql_string_map_t *sm, uint32_t *new_len);
void FreeStringMap(cql_string_map_t *sm);

#endif
