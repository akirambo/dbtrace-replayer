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

#ifndef __CASSANDRA_CXXRUNNER_HPP__
#define __CASSANDRA_CXXRUNNER_HPP__

#include <iostream>
#include <sstream>
#include <vector>
#include <cassandra.h>

class CassandraCxxRunner
{
private:
    static const int _signature = 0x123f3f7c;
    
public:
    CassandraCxxRunner();
    ~CassandraCxxRunner();
    void connect(const char* ip);
    void close();
    bool syncExecuter(const char* command);

    void commitQuery(std::string command);
    bool resetDatabase();
    void resetQuery();
    bool asyncExecuter();

    double getDuration();
    //const char* getReply(unsigned int number);
    std::string getReply(unsigned int number);
    bool isLegal(){return this->_sig == _signature;};
    
private:
    int _sig;
    double _duration;
    std::string _reply;
    
    CassCluster* _cluster = NULL;
    CassSession* _session = NULL;
    CassFuture* _connect_future;
    std::vector <std::string> _queries;
    std::vector<const CassResult*> _results;

    void resetResults();
    const char* getResult(const CassResult* result);
    std::string parseResult(const CassResult* result);
};

#endif // __CASSANDRA_CXXRUNNER_HPP__

