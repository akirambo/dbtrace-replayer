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
#include <chrono>
#include <libmemcached/memcached.hpp>
#include "memcached_cxxrunner.hpp"

using namespace std;

/***************
 * Constructor *
 ***************/
MemcachedCxxRunner::MemcachedCxxRunner(){
}

/***************
 * Distructor *
 ***************/
MemcachedCxxRunner::~MemcachedCxxRunner(){
    free(this->_reply);
}

/***************
 * getDuration *
 ***************/
double MemcachedCxxRunner::getDuration(){
    return this->_duration;
}

/************
 * getReply *
 ************/
const char* MemcachedCxxRunner::getReply(){
    return this->_reply;
}

/******************
 * Syncronous API *
 ******************/
/* Connect */
// url = "IP_ADDRESS:PORT"
bool MemcachedCxxRunner::connect(const char* url, const bool binaryProtocol=true){
    memcached_return rc;
    if((this->_memc = memcached_create(NULL)) == NULL){
	return false;
    }
    memcached_server_st* server = memcached_servers_parse(url);
    rc = memcached_server_push(this->_memc, server);
    memcached_server_list_free(server);
    if(rc != MEMCACHED_SUCCESS){
	return false;
    }
    // Enable Binary Protocol
    if(binaryProtocol){
	rc = memcached_behavior_set(this->_memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 0);
	if(rc != MEMCACHED_SUCCESS){
	    return false;
}
    }
    return true;
}

/* Close */
bool MemcachedCxxRunner::close(){
    memcached_free(this->_memc);
    return true;
}


bool MemcachedCxxRunner::syncExecuter(const char* query, const char* key,
				      const char* value, const int expire)
{
    bool returnFlag = true;
#ifdef __DEBUG__
    cout <<"["  << query << "]:: " << key <<"," << value << endl;;
#endif // __DEBUG__
    if(!strcmp(query,"SET") or !strcmp(query, "set")){
	returnFlag = set(key, value, expire);
    }else if(!strcmp(query,"GET") or !strcmp(query, "get")){
	returnFlag = get(key);
    }else if(!strcmp(query,"FLUSH") or !strcmp(query, "flush")){
	returnFlag = flush();
    }else if(!strcmp(query,"INCR") or !strcmp(query,"incr")){
	returnFlag = incr(key, value);
    }else if(!strcmp(query,"DECR") or !strcmp(query,"decr")){
	returnFlag = decr(key, value);
    }else if(!strcmp(query,"REPLACE") or !strcmp(query,"replace")){
	returnFlag = replace(key, value);
    }else if(!strcmp(query,"DELETE") or !strcmp(query,"delete")){
	returnFlag = deleteOperation(key);
    }else{
	cerr << "Unsupported Command " << query << endl;
	returnFlag = false;
    }
    return returnFlag;
}

bool MemcachedCxxRunner::set(const char* key, 
			     const char* value,
			     const int expire)
{
    uint32_t flags = 0;
    memcached_return rc;
    auto start = std::chrono::system_clock::now();
    rc = memcached_set(this->_memc, key, strlen(key), value, strlen(value), expire, flags);
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    this->_duration = nsec / (1000.0*1000.0*1000.0);   
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }else{
	cerr << "Cannot Set Data (" << key << "," << value << ") " << endl;
    }
    return false;
}

bool MemcachedCxxRunner::get(const char* key)
{
    size_t len = 0;
    uint32_t flags = 0;
    memcached_return rc;
    this->_reply = NULL;
    auto start = std::chrono::system_clock::now();
    this->_reply = memcached_get(this->_memc, key, strlen(key), &len, &flags, &rc);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc != MEMCACHED_SUCCESS){
	this->_reply = NULL;
 	return false;
    }
    return true;
}

bool MemcachedCxxRunner::incr(const char* key, const char* value)
{
    memcached_return rc;
    uint32_t offset = (uint32_t)std::atoi(value);
    uint64_t* initValue = 0;
    auto start = std::chrono::system_clock::now();;
    rc = memcached_increment(this->_memc, key, strlen(key), offset, initValue);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }
    return false;
}


bool MemcachedCxxRunner::decr(const char* key, const char* value)
{
    memcached_return rc;
    uint32_t offset = (uint32_t)std::atoi(value);
    uint64_t* initValue = 0;
    auto start = std::chrono::system_clock::now();;
    rc = memcached_decrement(this->_memc, key, strlen(key), offset, initValue);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }
    return false;
}

bool MemcachedCxxRunner::flush()
{
    time_t expire = 0;
    memcached_return rc;
    auto start = std::chrono::system_clock::now();;
    rc = memcached_flush(this->_memc, expire);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }
    return false;
}

bool MemcachedCxxRunner::replace(const char* key, const char* value)
{
    time_t expire = 0;
    uint32_t flags = 0;
    memcached_return rc;
    auto start = std::chrono::system_clock::now();;
    rc = memcached_replace(this->_memc, key, strlen(key), value, strlen(value), expire, flags);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }
    return false;
}

bool MemcachedCxxRunner::deleteOperation(const char* key)
{
    memcached_return rc;
    time_t expire = 0;
    auto start = std::chrono::system_clock::now();;
    rc = memcached_delete(this->_memc, key, strlen(key), expire);
    auto end = std::chrono::system_clock::now();;
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(rc == MEMCACHED_SUCCESS) {
	return true;
    }
    return false;
}

/*********************
 * Specific Function *
 *********************/
std::string storedKeys;
memcached_return_t MemcachedCxxRunner::dumper(const memcached_st *memc,
					      const char *key, 
					      size_t key_length,
					      void* context)
{
    if(storedKeys.length() == 0){
	storedKeys = key;
    }else{
	storedKeys = storedKeys + "," + key;
    }

    return MEMCACHED_SUCCESS;
}

std::string  MemcachedCxxRunner::keys(){
    storedKeys = "";
    memcached_dump_fn callbacks[1];
    callbacks[0] = &dumper;
    memcached_return_t a = memcached_dump(this->_memc, callbacks, NULL, 1);
    return storedKeys;
}


/*****************
 * For Async Get *
 *****************/
bool MemcachedCxxRunner::commitGetKey(const char* key)
{
    this->_getKeys.push_back(key);
    return true;
}

bool MemcachedCxxRunner::resetGetKeys(){
    this->_getKeys.clear();
    return true;
}




const char* MemcachedCxxRunner::mgetReply(const char* key){
    std::string keyString = key;
    if(keyString != ""){
      //std::cout << key << " :: " << this->_values[key] << std::endl;
      if(this->_values[key] != NULL){
	return this->_values[key];
      }
    }else{
	std::string buf;
	bool firstFlag = true;
	for(auto itr = this->_values.begin(); itr != this->_values.end(); ++itr) {
	    if(!firstFlag){
		buf += "\n" + itr->first + ":" + itr->second;
	    }else{
		buf += itr->first + ":" + itr->second;
		firstFlag = false;
	    }
	}
	return buf.c_str();
    }
    return "";
}

bool MemcachedCxxRunner::mget()
{
    memcached_return_t rc;
    const char *keys[this->_getKeys.size()];
    size_t key_length[this->_getKeys.size()];

    uint32_t flags;

    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_length;
    char *return_value = NULL;
    size_t return_value_length;

    int i = 0;
    for(auto itr = this->_getKeys.begin(); itr != this->_getKeys.end(); itr++){
	keys[i] = *itr;
	//std::cout << *itr << std::endl;
	key_length[i] = strlen(*itr);
	i++;
    }
    auto start = std::chrono::system_clock::now();
    rc = memcached_mget(this->_memc, keys, key_length, i);
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    
    if(rc == MEMCACHED_SUCCESS){
	start = std::chrono::system_clock::now();
	while((return_value = memcached_fetch(this->_memc, return_key, &return_key_length,
					      &return_value_length, &flags, &rc)) != NULL){
	    if(rc == MEMCACHED_SUCCESS){
	      //std::cout << "(" << return_key << "," << return_value << ")"<< std::endl;
		this->_values[return_key] = return_value;
	    }
	}
	end = std::chrono::system_clock::now();
	duration = end - start;
	nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
	this->_duration += nsec / (1000.0*1000.0*1000.0);
    }
    return true;
}

