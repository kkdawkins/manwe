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
void SendCQLError(int sock, uint32_t tid, uint32_t err, char* msg) {
    int p_len = sizeof(cql_packet_t) + 4 + 2 + strlen(msg); // Header + int + short + msg length
    cql_packet_t *p = (cql_packet_t *)malloc(p_len);
    memset(p, 0, p_len);

    p->version = CQL_V1_RESPONSE;
    p->opcode = CQL_OPCODE_ERROR;
    p->length = htonl(6 + strlen(msg));
    err = htonl(err);
    memcpy((char *)p + sizeof(cql_packet_t), &err, 1);
    uint16_t str_len = htons(strlen(msg));
    memcpy((char *)p + sizeof(cql_packet_t) + 4, &str_len, 1);
    memcpy((char *)p + sizeof(cql_packet_t) + 6, msg, strlen(msg));

    #if DEBUG
    printf("%u: Sending error to client: '%s'.\n", (uint32_t)tid, msg);
    #endif

    if (send(sock, p, p_len, 0) < 0) { // Send the packet
        fprintf(stderr, "%u: Error sending error packet to client: %s\n", tid, strerror(errno));
        exit(1);
    }

    free(p);
}

/*
 * Coverts a CQL string map into a linked list of key/value strings.
 */
cql_string_map_t * ReadStringMap(char *buf) {
    if (buf == NULL) {
        return NULL;
    }

    uint16_t num_pairs;
    memcpy(&num_pairs, buf, 2);
    num_pairs = ntohs(num_pairs);

    if (num_pairs == 0) {
        return NULL; // An empty string map can sometimes be sent, like in the cpp driver when sending no auth data
    }

    int offset = 2; // Keep track as we move through the buffer
    uint16_t str_len = 0; // Length of each string we process

    cql_string_map_t *sm = (cql_string_map_t *)malloc(sizeof(cql_string_map_t));
    cql_string_map_t *head = sm;

    while (num_pairs > 0) {
        // Key
        memcpy(&str_len, buf + offset, 2);
        offset += 2;
        str_len = ntohs(str_len);
        sm->key = (char *)malloc(str_len + 1);
        memset(sm->key, 0, str_len + 1);
        memcpy(sm->key, buf + offset, str_len);
        offset += str_len;

        // Value
        memcpy(&str_len, buf + offset, 2);
        offset += 2;
        str_len = ntohs(str_len);
        sm->value = (char *)malloc(str_len + 1);
        memset(sm->value, 0, str_len + 1);
        memcpy(sm->value, buf + offset, str_len);
        offset += str_len;

        num_pairs--;

        if (num_pairs > 0) {
            sm->next = (cql_string_map_t *)malloc(sizeof(cql_string_map_t));
            sm = sm->next;
        }
        else {
            sm->next = NULL;
        }
    }

    return head;
}

/*
 * Coverts a linked list of key/value strings into the CQL string map format. Returns size of new buffer.
 */
char* WriteStringMap(cql_string_map_t *sm, uint32_t *new_len) {
    if (sm == NULL) {
        *new_len = 0;
        return NULL;
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

    char *buf = (char *)malloc(buf_size);
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

        // Value
        str_len = strlen(sm->value);
        str_len = htons(str_len);
        memcpy(buf + offset, &str_len, 2);
        offset += 2;
        memcpy(buf + offset, sm->value, strlen(sm->value));
        offset += strlen(sm->value);

        sm = sm->next;
    }

    *new_len = buf_size;
    return buf;
}

void FreeStringMap(cql_string_map_t *sm) {
    while (sm != NULL) {
        cql_string_map_t *next = sm->next;
        free(sm->key);
        free(sm->value);
        free(sm);
        sm = next;
    }
}

void gracefulExit(int sig) {
    fprintf(stderr, "\nCaught sig %d -- exiting.\n", sig);

    exit(0);
}



bool addNode(node *head, node *toAdd){
    node *tmp;
    if(head == NULL){
        head = toAdd;
        return true;
    }else{
        tmp = head;
        while(tmp->next != NULL){
            tmp = tmp->next;
        }
        tmp->next = toAdd;
        return true;
    }
    return false;
}

bool removeNode(node *head, node *toRemove){
    node *tmp;
    node *tmp2;
    if(head->id == toRemove->id){
        if(head->next == NULL){
            free(head);
            return true;
        }else{
            tmp = head->next;
            free(head);
            head = tmp;
            return true;
        }
    }else{
        tmp = head;
        while(tmp->next != NULL){
            if(tmp->next->id != toRemove->id){
                tmp2 = tmp->next->next;
                free(tmp->next);
                tmp->next = tmp2; // Works even if tmp->next is the end, it will be null
                return true;
            }
            tmp = tmp->next;
        }
        // if we got here, we never found it
        return false;
    }
    return false;
}

bool findNode(node *head, int8_t stream_id){
    node *tmp = head;
    
    while(tmp != NULL){
        if(tmp->id == stream_id){
            removeNode(head, tmp);
            return true;
        }
    }
    return false;
}
