require "mkmf"

$libs += " -lstdc++ -lmemcached "
$CXXFLAGS += " -std=c++0x "
create_makefile("memcachedCxxRunner")
