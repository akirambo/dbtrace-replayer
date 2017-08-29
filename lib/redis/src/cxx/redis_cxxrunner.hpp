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

#ifndef __REDIS_CXX_RUNNER_HPP__
#define __REDIS_CXX_RUNNER_HPP__

#include <iostream>
#include <vector>
#include <chrono>
#include <string.h>
#include <stdio.h>

#include <hiredis.h>
#include <async.h>
#include <adapters/libevent.h>
#include <algorithm>

typedef int ECommandType;

class RedisCxxRunner
{
private:
    static const int _signature = 0x123f3f7c;
    //ECommandType
    enum {TYPE_GET,TYPE_OTHER};
    
public:
    RedisCxxRunner();
    ~RedisCxxRunner();
    void syncConnect(const char* ip, const int port);
    bool syncExecuter(const char* command);
    void syncClose();
    
    void asyncConnect(const char* ip, int port);
    bool asyncExecuter();
    void asyncClose();
    void commitQuery(const char* query);
    unsigned int pooledQuerySize();
    void resetDuration();
    
    double getDuration();
    const char* getReply();
    const char* getAsyncReply();
    bool isLegal(){return this->_sig == _signature;};
    
private:
    int _sig;
    double _duration;
    std::string _reply;
    redisContext* _syncConnect;
    redisAsyncContext* _asyncConnect;
    std::vector<std::string> _queries;
    std::vector<const char *> _replies;
    const char* _ip = "127.0.0.1";
    int _port = 6379;
        
    ECommandType commandType(std::string command);

    
};


#endif // __REDIS_CXX_RUNNER_HPP__
