/*
 * helpers.c - Helper methods for the gateway
 * CSC 652 - 2014
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "gateway.hpp"
#include "helpers.hpp"


/*
 * This method creates an appropriate CQL error packet to send on a specified socket. This does not close the socket before returning.
 */
void SendCQLError(int sock, uint32_t tid, uint32_t err, char *msg) {
    int p_len = sizeof(cql_packet_t) + 4 + 2 + strlen(msg); // Header + int + short + msg length
    cql_packet_t *p = malloc(p_len);
    memset(p, 0, p_len);

    p->version = htons(CQL_V2_RESPONSE);
    p->opcode = htons(CQL_OPCODE_ERROR);
    p->length = htonl(6 + strlen(msg));
    memset((void *)p + sizeof(cql_packet_t), htonl(err), 1);
    uint16_t str_len = htons(strlen(msg));
    memcpy((void *)p + sizeof(cql_packet_t) + 4, &str_len, 1);
    memcpy((void *)p + sizeof(cql_packet_t) + 6, msg, strlen(msg));

    if (send(sock, p, p_len, 0) < 0) { // Send the packet
        fprintf(stderr, "%u: Error sending error packet to client: %s\n", tid, strerror(errno));
        exit(1);
    }
}

/*
 * Coverts a CQL string map into a linked list of key/value strings.
 */
cql_string_map_t * ReadStringMap(void *buf) {
    if (buf == NULL) {
        return NULL;
    }

    uint16_t num_pairs;
    memcpy(&num_pairs, buf, 2);
    num_pairs = ntohs(num_pairs);

    int offset = 2; // Keep track as we move through the buffer
    uint16_t str_len = 0; // Length of each string we process

    cql_string_map_t *sm = malloc(sizeof(cql_string_map_t));
    cql_string_map_t *head = sm;

    while (num_pairs > 0) {
        // Key
        memcpy(&str_len, buf + offset, 2);
        offset += 2;
        str_len = ntohs(str_len);
        sm->key = malloc(str_len + 1);
        memset(sm->key, 0, str_len + 1);
        memcpy(sm->key, buf + offset, str_len);
        offset += str_len;

        // Value
        memcpy(&str_len, buf + offset, 2);
        offset += 2;
        str_len = ntohs(str_len);
        sm->value = malloc(str_len + 1);
        memset(sm->value, 0, str_len + 1);
        memcpy(sm->value, buf + offset, str_len);
        offset += str_len;

        num_pairs--;

        if (num_pairs > 0) {
            sm->next = malloc(sizeof(cql_string_map_t));
            sm = sm->next;
        }
    }

    return head;
}

/*
 * Coverts a linked list of key/value strings into the CQL string map format. Returns size of new buffer.
 */
uint32_t WriteStringMap(cql_string_map_t *sm, void *buf) {
    if (sm == NULL) {
        buf = NULL;
        return 0;
    }

    cql_string_map_t *head = sm;
    uint16_t num_pairs = 0;
    uint32_t buf_size = 2;
    int offset = 2;
    uint16_t str_len = 0;

    while (sm != NULL) {
        num_pairs++;
        buf_size += strlen(sm->key) + 2; // Length of string plus two byte short in front of it
        buf_size += strlen(sm->value) + 2;
        sm = sm->next;
    }

    buf = malloc(buf_size);
    num_pairs = htons(num_pairs);
    memcpy(buf, &num_pairs, 2);

    sm = head;
    while (sm != NULL) {
        // Key
        str_len = strlen(sm->key);
        str_len = htons(str_len);
        memcpy(buf + offset, &str_len, 2);
        offset += 2;
        memcpy(buf + offset, sm->key, strlen(sm->key));
        offset += strlen(sm->key);
        free(sm->key);

        // Value
        str_len = strlen(sm->value);
        str_len = htons(str_len);
        memcpy(buf + offset, &str_len, 2);
        offset += 2;
        memcpy(buf + offset, sm->value, strlen(sm->value));
        offset += strlen(sm->value);
        free(sm->value);

        cql_string_map_t *next = sm->next;
        free(sm);
        sm = next;
    }

    return buf_size;
}
