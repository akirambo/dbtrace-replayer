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

#include <algorithm>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include "../memcached_cxxrunner.hpp"

int main(){
    MemcachedCxxRunner* client;
    client = new MemcachedCxxRunner();
    const char *env_ip = getenv("MEMCACHED_IPADDRESS");
    if(env_ip == NULL){
      env_ip = "127.0.0.1";
    }
    std::string server = std::string(env_ip) + ":11211";
    client->connect(server.c_str(),true);
    if(client->syncExecuter("flush",NULL,NULL,0)){		
	std::cout << "TEST(cxx) [flush data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [flush data] :: FAILED" << std::endl;
    }
    if(client->syncExecuter("set","aaa","BBBB",0)){	
	std::cout << "TEST(cxx) [set data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [set data] :: FAILED" << std::endl;
    }
    if(client->syncExecuter("get","aaa",NULL,0)){
	if(!strcmp(client->getReply(),"BBBB")){
	    std::cout << "TEST(cxx) [get data] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [get data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [get data] :: FAILED" << std::endl;
    }
    if(client->syncExecuter("replace","aaa","CCCC",0)){	
	if(client->syncExecuter("get","aaa",NULL,0)){
	    if(!strcmp(client->getReply(),"CCCC")){
		std::cout << "TEST(cxx) [replace data] :: PASSED" << std::endl;
	    }else{
		std::cout << "TEST(cxx) [replace data] :: FAILED" << std::endl;
	    }
	}else{
	    std::cout << "TEST(cxx) [replace data] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [replace data] :: FAILED" << std::endl;
    }
    if(client->syncExecuter("delete","aaa","",0)){	
	std::cout << "TEST(cxx) [delete data] :: PASSED" << std::endl;
    }else{
	std::cout << "TEST(cxx) [delete data] :: FAILED" << std::endl;
    }

    
    client->syncExecuter("set","test00","AAAA",0);
    client->syncExecuter("set","test01","BBBB",0);
    client->syncExecuter("set","test02","CCCC",0);
    client->syncExecuter("set","test03","DDDD",0);
    sleep(1);
    std::string keys = client->keys();
    if(keys.length() > 0){
	if(keys.find("test00") != std::string::npos and
	   keys.find("test01") != std::string::npos and
	   keys.find("test02") != std::string::npos and
	   keys.find("test03") != std::string::npos){
	    std::cout << "TEST(cxx) [keys] :: PASSED" << std::endl;
	}else{
	    std::cout << "TEST(cxx) [keys] :: FAILED" << std::endl;
	}
    }else{
	std::cout << "TEST(cxx) [keys] :: FAILED" << std::endl;
    }


    // Async Mget
    /// setup 
    client->commitGetKey("test00");
    client->commitGetKey("test01");
    client->commitGetKey("test02");
    client->commitGetKey("test03");
    client->mget();
    
    if( strcmp(client->mgetReply("test00"),"AAAA") != 0 or
	strcmp(client->mgetReply("test01"),"BBBB") != 0 or 
	strcmp(client->mgetReply("test02"),"CCCC") != 0 or
	strcmp(client->mgetReply("test03"),"DDDD") != 0 or
	strcmp(client->mgetReply("testXX"),"") != 0){
	std::cout << "TEST(cxx) [mget data] :: FAILED" << std::endl;
    }else{
    	std::cout << "TEST(cxx) [mget data] :: PASSED" << std::endl;
    }
    client->syncExecuter("flush",NULL,NULL,0);
    client->close();
}
