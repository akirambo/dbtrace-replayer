require "mkmf"

$libs += " -lstdc++ -lhiredis -levent"
$CXXFLAGS += " -std=c++0x -I./hiredis -I./hiredis/adapters/ -L./hiredis"
$LDFLAGS += " -L/usr/local/lib/"
create_makefile("redisCxxRunner")
