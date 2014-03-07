#ifndef _GATEWAY_H
#define _GATEWAY_H

//
// SYSTEM DEFINE SECTION
//
extern "C" {
#include <stdint.h>

#if DEBUG
//Debugging enables lots of asserts in the code that are normally not included.
#include <assert.h>
#endif
}
//
// CONSTANT DEFINE SECTION
//

//We need to know if we're on a little-endian machine, since that will require us to call the appropriate hton/ntoh functions. Note this only works with gcc.
#define IS_LITTLE_ENDIAN (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

#define CASSANDRA_PORT 9160

//
// Documentation for the CQL binary protocol is avaiable at <https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol_v2.spec;hb=29670eb6692f239a3e9b0db05f2d5a1b5d4eb8b0>
//

//Cassandra CQL binary protocol packet
typedef struct {
  uint8_t version;
  uint8_t flags;
  int8_t  stream; //Per doc, this is a signed byte
  uint8_t opcode;
  int32_t length; //Per doc, looks like it is signed
  //void    *body; The body will need to be allocated right after the fixed length header
} cql_packet_t;

//Define constants for the different fields in a CQL packet
#define CQL_V1_REQUEST  0x01
#define CQL_V1_RESPONSE 0x81
#define CQL_V2_REQUEST  0x02
#define CQL_V2_RESPONSE 0x82

#define CQL_FLAG_NONE        0x00
#define CQL_FLAG_COMPRESSION 0x01
#define CQL_FLAG_TRACING     0x02

#define CQL_COMPRESSION_NONE   0x00
#define CQL_COMPRESSION_LZ4    0x01
#define CQL_COMPRESSION_SNAPPY 0x02

#define CQL_OPCODE_ERROR          0x00
#define CQL_OPCODE_STARTUP        0x01
#define CQL_OPCODE_READY          0x02
#define CQL_OPCODE_AUTHENTICATE   0x03
//No 0x04 opcode
#define CQL_OPCODE_OPTIONS        0x05
#define CQL_OPCODE_SUPPORTED      0x06
#define CQL_OPCODE_QUERY          0x07
#define CQL_OPCODE_RESULT         0x08
#define CQL_OPCODE_PREPARE        0x09
#define CQL_OPCODE_EXECUTE        0x0A
#define CQL_OPCODE_REGISTER       0x0B
#define CQL_OPCODE_EVENT          0x0C
#define CQL_OPCODE_BATCH          0x0D
#define CQL_OPCODE_AUTH_CHALLENGE 0x0E
#define CQL_OPCODE_AUTH_RESPONSE  0x0F
#define CQL_OPCODE_AUTH_SUCCESS   0x10

#define CQL_ERROR_SERVER_ERROR          0x0000
#define CQL_ERROR_PROTOCOL_ERROR        0x000A
#define CQL_ERROR_BAD_CREDENTIALS       0x0100
#define CQL_ERROR_UNAVAILABLE_EXCEPTION 0x1000
#define CQL_ERROR_OVERLOADED            0x1001
#define CQL_ERROR_IS_BOOTSTRAPPING      0x1002
#define CQL_ERROR_TRUNCATE_ERROR        0x1003
#define CQL_ERROR_WRITE_TIMEOUT         0x1100
#define CQL_ERROR_READ_TIMEOUT          0x1200
#define CQL_ERROR_SYNTAX_ERROR          0x2000
#define CQL_ERROR_UNAUTHORIZED          0x2100
#define CQL_ERROR_INVALID               0x2200
#define CQL_ERROR_CONFIG_ERROR          0x2300
#define CQL_ERROR_ALREADY_EXISTS        0x2400
#define CQL_ERROR_UNPREPARED            0x2500

void* HandleConn(void* thread_data);
char* prefix_cmd(char *cql_cmd, char *prefix);
void prepend(char* s, const char* t);

#endif
