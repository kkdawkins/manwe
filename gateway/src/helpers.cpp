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

/*
 * Coverts a CQL results block of memory into a 2D linked list representation.
 */
cql_result_cell_t* ReadCQLResults(char *buf, int32_t rows, int32_t cols) {
    if (buf == NULL || rows == 0) {
        return NULL;
    }

    cql_result_cell_t *row = (cql_result_cell_t *)malloc(sizeof(cql_result_cell_t));
    row->remove = false;
    row->next_row = NULL;
    cql_result_cell_t *ret = row;

    uint32_t offset = 0;

    int i,j;
    int32_t num_bytes = 0;

    for (i = 0; i < rows; i++) {
        cql_result_cell_t *curr = row;

        for (j = 0; j < cols; j++) {
            curr->next_col = NULL;
            curr->next_row = NULL;

            memcpy(&num_bytes, buf + offset, 4);
            num_bytes = ntohl(num_bytes);
            offset += 4;

            if (num_bytes > 0) {
                curr->content = (char *)malloc(num_bytes);
                memcpy(curr->content, buf + offset, num_bytes);
                curr->len = num_bytes;
                offset += num_bytes;
            }
            else {
                curr->content = NULL;
                curr->len = 0;
            }

            if (j + 1 < cols) {
                curr->next_col = (cql_result_cell_t *)malloc(sizeof(cql_result_cell_t));
                curr = curr->next_col;
            }
        }

        if (i + 1 < rows) {
            row->next_row = (cql_result_cell_t *)malloc(sizeof(cql_result_cell_t));
            row->next_row->remove = false;
            row->next_row->next_row = NULL;
            row = row->next_row;
        }
    }

    return ret;
}

/*
 * Coverts a 2D linked list representation of results into a chunk of memory. Returns size of new buffer.
 */
char* WriteCQLResults(cql_result_cell_t *rows, uint32_t *new_len, int32_t *new_rows) {
    if (rows == NULL) {
        *new_len = 0;
        *new_rows = 0;
        return NULL;
    }

    uint32_t offset = 0;

    // First, count the number of bytes total we will need to allocate
    *new_len = 0;
    *new_rows = 0;
    cql_result_cell_t *row_head = rows;
    while (row_head != NULL) {
        *new_rows = *new_rows + 1; // can't use ++, since that triggers -Werror=unused-value
        cql_result_cell_t *curr = row_head;
        cql_result_cell_t *next_row = curr->next_row;

        while (curr != NULL) {
            *new_len += 4 + curr->len;

            curr = curr->next_col;
        }

        row_head = next_row;
    }

    // Make the chunk of memory
    char *ret = (char *)malloc(*new_len);

    // Set values in memory
    row_head = rows;
    while (row_head != NULL) {
        cql_result_cell_t *curr = row_head;
        cql_result_cell_t *next_row = curr->next_row;

        while (curr != NULL) {

            int32_t len = htonl(curr->len);

            memcpy(ret + offset, &len, 4);
            memcpy(ret + offset + 4, curr->content, curr->len);

            offset += 4 + curr->len;

            curr = curr->next_col;
        }

        row_head = next_row;
    }

    return ret;
}

void FreeCQLResults(cql_result_cell_t *rows) {
    while (rows != NULL) {
        cql_result_cell_t *curr_cell = rows;
        cql_result_cell_t *next_row = curr_cell->next_row;

        while (curr_cell != NULL) {
            cql_result_cell_t *next_cell = curr_cell->next_col;
            free(curr_cell->content);
            free(curr_cell);
            curr_cell = next_cell;
        }

        rows = next_row;
    }
}


/*
 * Reads and parses the metadata of a result packet.
 */
cql_result_metadata_t * ReadResultMetadata(char *buf, uint32_t tid) {
    if (buf == NULL) {
        return NULL;
    }

    #ifndef DEBUG
    (void)tid; // Need to reference variable if not debugging to prevent error
    #endif

    cql_result_metadata_t *m = (cql_result_metadata_t *)malloc(sizeof(cql_result_metadata_t));
    m->offset = 0;

    memcpy(&m->flags, buf, 4);
    m->flags = ntohl(m->flags);
    m->offset += 4;

    memcpy(&m->columns_count, buf + m->offset, 4);
    m->columns_count = ntohl(m->columns_count);
    m->offset += 4;

    uint16_t str_len = 0;

    if (m->flags & CQL_RESULT_ROWS_FLAG_GLOBAL_TABLES_SPEC) { // Get keyspace and table from global spec
        // Get the keyspace
        memcpy(&str_len, buf + m->offset, 2);
        str_len = ntohs(str_len);
        m->offset += 2;

        m->keyspace = (char *)malloc(str_len + 1);
        memset(m->keyspace, 0, str_len + 1);
        memcpy(m->keyspace, buf + m->offset, str_len);
        m->offset += str_len;

        // Get the table
        memcpy(&str_len, buf + m->offset, 2);
        str_len = ntohs(str_len);
        m->offset += 2;

        m->table = (char *)malloc(str_len + 1);
        memset(m->table, 0, str_len + 1);
        memcpy(m->table, buf + m->offset, str_len);
        m->offset += str_len;

        #if DEBUG
        printf("%u:       From the global tables spec, keyspace is '%s', table is '%s'.\n", tid, m->keyspace, m->table);
        #endif
    }

    m->column = (cql_column_spec_t *)malloc(sizeof(cql_column_spec_t));
    cql_column_spec_t *curr = m->column;

    int i;
    for (i = 0; i < m->columns_count; i++) {

        if (i == 0 && !(m->flags & CQL_RESULT_ROWS_FLAG_GLOBAL_TABLES_SPEC)) { // Get keyspace and table from first column spec
            // Get the keyspace
            memcpy(&str_len, buf + m->offset, 2);
            str_len = ntohs(str_len);
            m->offset += 2;

            m->keyspace = (char *)malloc(str_len + 1);
            memset(m->keyspace, 0, str_len + 1);
            memcpy(m->keyspace, buf + m->offset, str_len);
            m->offset += str_len;

            // Get the table
            memcpy(&str_len, buf + m->offset, 2);
            str_len = ntohs(str_len);
            m->offset += 2;

            m->table = (char *)malloc(str_len + 1);
            memset(m->table, 0, str_len + 1);
            memcpy(m->table, buf + m->offset, str_len);
            m->offset += str_len;

            #if DEBUG
            printf("%u:       From the first column, keyspace is '%s', table is '%s'.\n", tid, m->keyspace, m->table);
            #endif
        }

        // Get the column name
        memcpy(&str_len, buf + m->offset, 2);
        str_len = ntohs(str_len);
        m->offset += 2;

        curr->name = (char *)malloc(str_len + 1);
        memset(curr->name, 0, str_len + 1);
        memcpy(curr->name, buf + m->offset, str_len);
        m->offset += str_len;

        memcpy(&curr->type, buf + m->offset, 2);
        curr->type = ntohs(curr->type);
        m->offset += 2;

        #if DEBUG
        printf("%u:       Column name and type: '%s' %d.\n", tid, curr->name, curr->type);
        #endif

        // Currently, we don't really care about what type each column is, but we need to advance the offset
        if (curr->type == 0x0000) { // Custom type
            memcpy(&str_len, buf + m->offset, 2);
            str_len = ntohs(str_len);
            m->offset += 2 + str_len;
        }
        // FIXME currently assumes no more than a 2D list/map/set
        else if (curr->type == 0x0020) { // List type
            uint16_t list_type_id = 0;
            memcpy(&list_type_id, buf + m->offset, 2);
            list_type_id = ntohs(list_type_id);
            m->offset += 2;

            if (list_type_id == 0x0000) {
                memcpy(&str_len, buf + m->offset, 2);
                str_len = ntohs(str_len);
                m->offset += 2 + str_len;
            }
        }
        else if (curr->type == 0x0021) { // Map type
            // The key
            uint16_t map_type_id = 0;
            memcpy(&map_type_id, buf + m->offset, 2);
            map_type_id = ntohs(map_type_id);
            m->offset += 2;

            if (map_type_id == 0x0000) {
                memcpy(&str_len, buf + m->offset, 2);
                str_len = ntohs(str_len);
                m->offset += 2 + str_len;
            }

            // The value
            memcpy(&map_type_id, buf + m->offset, 2);
            map_type_id = ntohs(map_type_id);
            m->offset += 2;

            if (map_type_id == 0x0000) {
                memcpy(&str_len, buf + m->offset, 2);
                str_len = ntohs(str_len);
                m->offset += 2 + str_len;
            }
        }
        else if (curr->type == 0x0022) { // Set type
            uint16_t set_type_id = 0;
            memcpy(&set_type_id, buf + m->offset, 2);
            set_type_id = ntohs(set_type_id);
            m->offset += 2;

            if (set_type_id == 0x0000) {
                memcpy(&str_len, buf + m->offset, 2);
                str_len = ntohs(str_len);
                m->offset += 2 + str_len;
            }
        }

        if (i + 1 < m->columns_count) {
            curr->next = (cql_column_spec_t *)malloc(sizeof(cql_column_spec_t));
            curr = curr->next;
        }
        else {
            curr->next = NULL;
        }
    }

    return m;
}

void FreeResultMetadata(cql_result_metadata_t *m) {
    if (m != NULL) {
        free(m->keyspace);
        free(m->table);
        cql_column_spec_t *c = m->column;
        while (c != NULL) {
            cql_column_spec_t *n = c->next;
            free(c->name);
            free(c);
            c = n;
        }
        free(m);
    }
}

void gracefulExit(int sig) {
    fprintf(stderr, "\nCaught sig %d -- exiting.\n", sig);

    exit(0);
}

void cassandra_thread_cleanup_handler(void *arg) {
    cql_packet_t **packet = (cql_packet_t **)arg;
    free(*packet);
}



node* addNode(node *head, node *toAdd) {
    if (toAdd == NULL) {
        return head;
    }

    if (head == NULL) {
        head = toAdd;
        return head;
    }
    else {
        node *tmp = head;
        while (tmp->next != NULL) {
            tmp = tmp->next;
        }
        tmp->next = toAdd;
    }
    return head;
}

node* removeNode(node *head, int8_t stream_id) {
    if (head == NULL) {
        return NULL;
    }

    node *tmp;
    node *tmp2;
    if(head->id == stream_id){

        if(head->next == NULL){

            free(head);
            head = NULL;
            return NULL;
        }else{

            tmp = head->next;
            free(head);
            head = tmp;
            return head;
        }
    }else{
        tmp = head;
        while(tmp->next != NULL){
            if(tmp->next->id == stream_id){
                tmp2 = tmp->next->next;
                free(tmp->next);
                tmp->next = tmp2; // Works even if tmp->next is the end, it will be null
                return head;
            }
            tmp = tmp->next;
        }
        // if we got here, we never found it
        return head;
    }
    return head;
}

bool findNode(node *head, int8_t stream_id) {
    if (head == NULL) {
        return false;
    }

    node *tmp = head;
    
    while (tmp != NULL) {
        #if DEBUG
        printf("----------- tmp %d stream %d\n", tmp->id, stream_id);
        #endif
        if (tmp->id == stream_id) {
            //head = removeNode(head, tmp);
            return true;
        }
        tmp = tmp->next;
    }
    return false;
}

// Check if the internal token is present in this column data
bool scanForInternalToken(char *cellInQuestion, char *internalToken){
    std::string cell(cellInQuestion);
    std::string iToken(internalToken);
    
    std::size_t found = cell.find(iToken);
    if (found == std::string::npos){
        // The user's token was not found in the cell data
        // Apparently there are some exceptions to this rule:
        std::string system("system"); // FIXME must be case-insensitive equal of "system", "system_auth", or "system_traces"
        if(cell.find(system) != std::string::npos){
            return true;
        }
        return false;
    }else{
        return true;
    }
}

bool scanforRestrictedKeyspaces(char *cellInQuestion){
    std::string cell(cellInQuestion);
    std::string multiTennant("multitenantcassandra");
    std::size_t found = cell.find(multiTennant);
    if(found == std::string::npos){ // FIXME must be case-insensitive equal, not just position match
        return false;
    }else{
        return true;
    }
}

void free_row(cql_result_cell_t *row){
    cql_result_cell_t *curr;
    curr = row;
    while(curr != NULL){
        row = row->next_col;
        free(curr->content);
        free(curr);
        curr = row;
    }
}

// Return's a pointer to a new cleaned-up list
cql_result_cell_t *cleanup(cql_result_cell_t *parsed_table, uint32_t tid){
    cql_result_cell_t *curr_row = parsed_table;
    cql_result_cell_t *tmp;
    #ifndef DEBUG
    (void)tid; // Need to reference variable if not debugging to prevent error
    #endif    
    #if DEBUG
    printf("%u:   Begin cleanup of table.\n", tid);
    #endif
    
    if(parsed_table == NULL){
        return NULL;
    }
    // First, check the head
    while(curr_row != NULL && curr_row->remove == true){
        tmp = curr_row;
        curr_row = curr_row->next_row;
        free_row(tmp);
    }
    
    if(curr_row == NULL){
        return NULL;
    }
    
    // update the head pointer
    parsed_table = curr_row;
    
    
    // Second, scan and remove the rest
    tmp = curr_row->next_row; // Walk tmp pointer one ahead
    while(tmp != NULL){
        while(tmp != NULL && tmp->remove == true){
            curr_row->next_row = tmp->next_row;
            free_row(tmp);
            tmp = curr_row->next_row;
        }
        if(tmp == NULL){
            break;
        }
        curr_row = tmp;
        tmp = curr_row->next_row;
    }
    
    return parsed_table;
}

bool isImportantTable(char *keyspace, char *tableName){
    if (strcmp(keyspace, "system") == 0) {
        if(strcmp(tableName,"schema_keyspaces") == 0){
            return true;
        }else if(strcmp(tableName,"schema_columnfamilies") == 0){
            return true;
        }else if(strcmp(tableName,"schema_columns") == 0){
            return true;
        }
    }
    else if (strcmp(keyspace, "system_auth") == 0) {
        if(strcmp(tableName,"users") == 0){
            return true;
        }
    }
    return false;
}

bool isImportantColumn(char *name){
    if(strcmp(name,"keyspace_name") == 0){
        return true;
    }else if(strcmp(name, "name") == 0){
        return true;
    }else{
        return false;
    }
}