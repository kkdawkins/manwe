CC = g++

CFLAGS = -lcql -lboost_system -lboost_thread -lboost_program_options -lssl -lcrypto

all:	test

test:	test_main.cpp test_main_v2.cpp
	$(CC) -o test_main test_main.cpp $(CFLAGS)
	$(CC) -o test_main_v2 test_main_v2.cpp $(CFLAGS)

clean:
	rm -rf test_main test_main_v2
