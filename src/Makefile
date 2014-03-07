CC = g++
VALGRIND = valgrind

CFLAGS = -Wall -Wextra -Werror -O3 -lpthread -lpcre -lboost_regex
DEBUG_FLAGS = -g -O0 -DDEBUG #Define the DEBUG flag at compile time

all:	gateway

gateway:	gateway.o helpers.o
	$(CC) -o gateway helpers.o gateway.o $(CFLAGS)

gateway.o:	gateway.hpp gateway.cpp
	$(CC) -c gateway.cpp $(CFLAGS)

helpers.o:	helpers.hpp helpers.cpp
	$(CC) -c helpers.cpp $(CFLAGS)

debug:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) $(DEBUG_FLAGS)"

clean:
	rm -rf *.o gateway
