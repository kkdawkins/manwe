CC = g++
VALGRIND = valgrind

CFLAGS = -Wall -Wextra -Werror -O3 -lpthread -lpcre -lcql -lboost_system -lboost_thread -lboost_regex -lssl -lcrypto
DEBUG_FLAGS = -g -O0 -DDEBUG #Define the DEBUG flag at compile time

all:	gateway

gateway:	gateway.o helpers.o cassandra.o
	$(CC) -o gateway helpers.o gateway.o cassandra.o $(CFLAGS)

gateway.o:	gateway.hpp gateway.cpp
	$(CC) -c gateway.cpp $(CFLAGS)

helpers.o:	helpers.hpp helpers.cpp
	$(CC) -c helpers.cpp $(CFLAGS)
	
cassandra.o: cassandra.hpp cassandra.cpp
	$(CC) -c cassandra.cpp $(CFLAGS)

debug:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) $(DEBUG_FLAGS)"

clean:
	rm -rf *.o gateway
