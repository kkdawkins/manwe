CC = gcc
VALGRIND = valgrind

CFLAGS = -Wall -Wextra -Werror -O3 -lpthread
DEBUG_FLAGS = -g -O0 -DDEBUG #Define the DEBUG flag at compile time


all:	manwe

manwe:	gateway.h gateway.c
	$(CC) -o gateway gateway.c $(CFLAGS)

debug:
	$(MAKE) clean
	$(MAKE) CFLAGS="$(CFLAGS) $(DEBUG_FLAGS)"

clean:
	rm -rf *.o gateway
