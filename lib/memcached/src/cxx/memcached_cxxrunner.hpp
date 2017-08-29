/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */



#ifndef __MEMCACHED_CXXRUNNER_HPP__
#define __MEMCACHED_CXXRUNNER_HPP__

#include <libmemcached/memcached.hpp>
#include <map>

class MemcachedCxxRunner
{
private:
    static const int _signature = 0x123f3f7c;

public:
    MemcachedCxxRunner();
    ~MemcachedCxxRunner();
    bool connect(const char* url, const bool binaryProtocol);
    bool syncExecuter(const char* query,
		      const char* key,
		      const char* value,
		      const int   expire);
    bool close();

    bool commitGetKey(const char* key);
    bool resetGetKeys();
    bool mget();
    std::string keys();
    const char* mgetReply(const char* key);
    const char* keylist();
    double getDuration();
    const char* getReply();
    bool isLegal(){return this->_sig == _signature;};
private:
    int _sig;
    bool set(const char* key, const char* value, const int expire);
    bool get(const char* key);
    bool incr(const char* key, const char* value);
    bool decr(const char* key, const char* value);
    bool flush();
    bool replace(const char* key, const char* value);
    bool deleteOperation(const char* key);
    static memcached_return_t dumper(const memcached_st *memc, 
				     const char *key,
				     size_t key_length,
				     void* context);
    double _duration;
    char* _reply;
    memcached_st *_memc;
    std::vector <const char*> _getKeys;
    std::map <std::string, const char*> _values;
};


#endif // __MEMCACHED_CXXRUNNER_HPP__

