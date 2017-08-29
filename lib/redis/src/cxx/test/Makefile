
PROGRAM = testrun
CPPFLAGS = -Wall -std=c++0x \
	-I../hiredis/ \
	-I../hiredis/adapters/

LDLIBS = -L/usr/local/lib/ 


SRC = test.cpp ../redis_cxxrunner.cpp

OBJS = $(SRC:.cpp=.o)

LDFLAGS = -lhiredis -lstdc++ -levent

CXX = g++

all : $(PROGRAM)

$(PROGRAM) : $(OBJS)
	$(CXX) $(CPPFLAGS) -o $(PROGRAM) $(OBJS) $(LDLIBS) $(LDFLAGS) 

clean :
	rm -vf $(OBJS) $(PROGRAM)

