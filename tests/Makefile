CC = g++

CFLAGS = -lcql -lboost_system -lboost_thread -lboost_program_options -lssl -lcrypto

all:	test

test:	test_cpp_auth.cpp test_main.cpp
	$(CC) -o test_main test_main.cpp $(CFLAGS)
	$(CC) -o test_cpp_auth test_cpp_auth.cpp $(CFLAGS)

clean:
	rm -rf test_cpp_auth test_main
