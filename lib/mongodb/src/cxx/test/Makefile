

PROGRAM = testrun
SRC = main.cpp ../mongodb_cxxrunner.cpp
OBJS = $(SRC:.cpp=.o)

TEST_PROGRAM = formatTest
TEST_SRC = formatTest.cpp 
TEST_OBJS = $(TEST_SRC:.cpp=.o)

CPPFLAGS = -Wall -std=c++11 \
	-I/usr/local/include/mongocxx/v_noabi \
	-I/usr/local/include/libmongoc-1.0 \
	-I/usr/local/include/bsoncxx/v_noabi -I/usr/local/include/libbson-1.0 -L/usr/local/lib -lmongocxx -lbsoncxx
#	-I/usr/include \
	-I/usr/local/include/ \
	-I./include/ \
	-I/usr/local/include/mongocxx/v_noabi \
	-I/usr/local/include/libbson-1.0 \
	-I/usr/local/include/libmongoc-1.0 \
	-I/usr/local/include/bsoncxx/v_noabi 


LDLIBS = -L/usr/lib \
	-L/usr/lib64 \
	-L/usr/local/lib/ 

LDFLAGS = -lstdc++ \
	-lmongocxx \
	-lbsoncxx

#	-lmongoclient \
        -pthread \
	-lboost_thread-mt \
	-lboost_system \
	-lboost_regex \

CXX = g++

all : $(PROGRAM) $(TEST_PROGRAM)

$(PROGRAM) : $(OBJS)
	$(CXX) $(CPPFLAGS) -o $(PROGRAM) $(OBJS) $(LDLIBS) $(LDFLAGS) $(LIBS)

$(TEST_PROGRAM) : $(TEST_OBJS)
	$(CXX) $(CPPFLAGS) -o $(TEST_PROGRAM) $(TEST_OBJS) $(LDLIBS) $(LDFLAGS) $(LIBS)

clean :
	rm -vf $(OBJS) $(PROGRAM) $(TEST_PROGRAM)
