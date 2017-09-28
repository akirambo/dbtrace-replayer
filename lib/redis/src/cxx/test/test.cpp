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

#include <iostream>
#include <string.h>
#include <unistd.h>
#include "../redis_cxxrunner.hpp"

bool checker(const char* reply_, const char* check){
    std::string reply = reply_;
    int length = reply.length();
    int ans = reply.find(check);
    if(ans > 0 and ans <= length){
	return true;
    }
    return false;
}

void showDetail(const char* reply){
    std::cout << "=================" << std::endl;
    std::cout << "Detail" << std::endl;
    std::cout << reply << std::endl;
    std::cout << "=================" << std::endl;
}

void syncTest(RedisCxxRunner *client, char *ip){
    // SET
    const char *env_ip = getenv("REDIS_IPADDRESS");
    if(env_ip == NULL){
      env_ip = "127.0.0.1";
    }
    client->syncConnect(env_ip,6379);
    if(client->syncExecuter("set aaa BBBB")){
	std::cout << "TEST(cxx) [set data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [set data] :: FAILED" << std::endl;
    }
    // GET
    if(client->syncExecuter("get aaa")){
	if(!strcmp(client->getReply(),"BBBB")){
	    std::cout << "TEST(cxx) [get data] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [get data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [get data] :: FAILED" << std::endl;
    }

    // HMSET
    if(client->syncExecuter("hmset key00 field0 value0 field1 value1")){
	std::cout << "TEST(cxx) [hmset data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [hmset data] :: FAILED" << std::endl;
    }
    // HMGET
    if(client->syncExecuter("hmget key00 field0 field1")){
	if(!strcmp(client->getReply(),"value0,value1")){
	    std::cout << "TEST(cxx) [hmget data] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [hmget data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [hmget data] :: FAILED" << std::endl;
    }
    // HVALS
    if(client->syncExecuter("hvals key00")){
	if(!strcmp(client->getReply(),"value0,value1")){
	    std::cout << "TEST(cxx) [hvals data] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [hvals data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [hvals data] :: FAILED" << std::endl;
    }
    // HKEYS
    if(client->syncExecuter("hkeys key00")){
	if(!strcmp(client->getReply(),"field0,field1")){
	    std::cout << "TEST(cxx) [hkeys data] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [hkeys data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [hkeys data] :: FAILED" << std::endl;
    }
    // SADD
    if(client->syncExecuter("sadd set00 '{num:1}'") and
       client->syncExecuter("sadd set00 '{num:2}'") and
       client->syncExecuter("sadd set00 '{num:3}'")){
	std::cout << "TEST(cxx) [sadd data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [sadd data] :: FAILED" << std::endl;
    }
    // SMEMBERS
    if(client->syncExecuter("smembers set00")){
	std::cout << "TEST(cxx) [smembers] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [smembers] :: FAILED" << std::endl;
    }
    // STRLEN
    if(client->syncExecuter("set test mmmm") and
       client->syncExecuter("strlen test")){
	if(!strcmp(client->getReply(),"4")){
	    std::cout << "TEST(cxx) [strlen] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [strlen] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [strlen] :: FAILED" << std::endl;
    }
    
    
    // ZADD & ZRANGE
    std::cout << "TEST(cxx) [zadd & zrank] " << std::endl;
    if(client->syncExecuter("zadd zranks 200 e1") and 
       client->syncExecuter("zadd zranks 300 e2") and
       client->syncExecuter("zadd zranks 400 e3") and
       client->syncExecuter("zrank zranks e3") and
       !strcmp(client->getReply(),"2") and 
       client->syncExecuter("zrank zranks e2") and
       !strcmp(client->getReply(),"1") and
       client->syncExecuter("zrank zranks e1") and
       !strcmp(client->getReply(),"0")){
	std::cout << "TEST(cxx) [zrank] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [zrank] :: FAILED" << std::endl;
    }
    // FLUSHALL
    client->syncExecuter("flushall");
    client->syncClose();
}

void asyncTest(RedisCxxRunner* client, char *ip){
    /** Async Test(1st) **/
    client->resetDuration();
    const char *env_ip = getenv("REDIS_IPADDRESS");
    if(env_ip == NULL){
      env_ip = "127.0.0.1";
    }
    client->asyncConnect(env_ip,6379);
    client->commitQuery("set test11 GOOD");
    client->commitQuery("get test11");
    client->asyncExecuter();
    if(strcmp(client->getAsyncReply(),"GOOD") == 0){
	std::cout << "TEST(cxx) [async single-set/get data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [async single-set/get data] :: FAILED" << std::endl;
    }
    client->asyncClose();

    /** Async Test(2nd) **/
    client->resetDuration();
    client->asyncConnect(env_ip,6379);

    client->commitQuery("set test00 AAAA");
    client->commitQuery("set test01 BBBB");
    client->commitQuery("set test02 CCCC");
    client->commitQuery("set test03 DDDD");
    client->commitQuery("set test04 EEEE");
    client->commitQuery("get test04");
    client->commitQuery("get test03");
    client->commitQuery("get test02");
    client->commitQuery("get test01");
    client->commitQuery("get test00");
    client->asyncExecuter();
    std::string ans = std::string(client->getAsyncReply());
    if(ans.find("AAAA") != std::string::npos &&
       ans.find("BBBB") != std::string::npos &&
       ans.find("CCCC") != std::string::npos &&
       ans.find("DDDD") != std::string::npos &&
       ans.find("EEEE") != std::string::npos){
      std::cout << "TEST(cxx) [async multi-set/get data] :: PASSED" << std::endl;
    }else{
      std::cout << "TEST(cxx) [async multi-set/get data] :: FAILED" << std::endl;
    }
    client->asyncClose();

    /** Async Test(3rd) **/
    client->asyncConnect(env_ip,6379);

    client->commitQuery("hmset key00 f0 A f1 B");
    client->commitQuery("hmset key00 f2 C f3 D");
    client->commitQuery("hmget key00 f0 f1");
    client->commitQuery("hmget key00 f2 f3");
    client->commitQuery("hgetall key00");
    client->commitQuery("hvals key00");
    client->commitQuery("hkeys key00");
    client->asyncExecuter();
    if(strcmp(client->getAsyncReply(),"A,B\nC,D\nf0,A,f1,B,f2,C,f3,D\nA,B,C,D\nf0,f1,f2,f3") == 0){
	std::cout << "TEST(cxx) [async hmset/hmget/hgetall/hvals/hkeys data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [async hmset/hmget/hgetall/hvals/hkeys data] :: FAILED" << std::endl;
	std::cout << client->getAsyncReply() << std::endl;
    }
    client->asyncClose();

    /** Async Test **/
    // SADD
    client->asyncConnect(env_ip,6379);

    client->commitQuery("sadd set00 AAA");
    client->commitQuery("sadd set00 BBB");
    client->commitQuery("sadd set00 CCC");
    // SMEMBERS
    client->commitQuery("smembers set00");
    client->asyncExecuter();
    if(strcmp(client->getAsyncReply(),"") != 0){
	std::cout << "TEST(cxx) [async sadd/smembers] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [async sadd/smembers] :: FAILED" << std::endl;
	std::cout << client->getAsyncReply() << std::endl;
    }

    client->asyncClose();
}


int main(){
    RedisCxxRunner* client ;
    client = new RedisCxxRunner();
    char *env_ip;
    if(!(env_ip = getenv("REDIS_IPADDRESS"))){
      env_ip = "127.0.0.1";
    }
    /*************
     * Sync Test *
     *************/
    syncTest(client, env_ip);
    
    /****************
     * Asynchronous *
     ****************/
    asyncTest(client, env_ip);

    // Specific Test 
    const char *env_ip = getenv("REDIS_IPADDRESS");
    if(env_ip == NULL){
      env_ip = "127.0.0.1";
    }
    client->syncConnect(env_ip,6379);
    client->syncExecuter("FLUSHALL");
    client->syncExecuter("SADD test001 '{\"name\":\"p001\"}'");
    client->syncExecuter("SMEMBERS test001 ");
    std::cout << client->getReply() << std::endl;
    client->syncExecuter("FLUSHALL");
    client->syncClose();
    return 0;
}
