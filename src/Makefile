CC = g++
VALGRIND = valgrind

CFLAGS = -Wall -Wextra -Werror -O3 -lpthread -lpcre
DEBUG_FLAGS = -g -O0 -DDEBUG #Define the DEBUG flag at compile time


all:	manwe

manwe:	gateway.hpp gateway.cpp
	$(CC) -o gateway gateway.cpp $(CFLAGS)

debug:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) $(DEBUG_FLAGS)"

clean:
	rm -rf *.o gateway
