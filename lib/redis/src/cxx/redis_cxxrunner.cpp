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

#include "redis_cxxrunner.hpp"

std::vector<std::string> tmpReplies;

RedisCxxRunner::RedisCxxRunner(){
    this->_sig  = _signature;
}

RedisCxxRunner::~RedisCxxRunner(){
}

void RedisCxxRunner::resetDuration(){
    this->_duration = 0.0;
}

double RedisCxxRunner::getDuration(){
    return this->_duration;
}

const char* RedisCxxRunner::getReply(){
    return this->_reply.c_str();
}

const char* RedisCxxRunner::getAsyncReply(){
    this->_reply = "";
    bool firstFlag = true;
    for(auto itr = tmpReplies.begin(); itr != tmpReplies.end(); itr++){
      if(!firstFlag){
	this->_reply += "\n" + *itr;
      }else{
	this->_reply = *itr;
	firstFlag = false;
      }
    }
    return this->_reply.c_str();
}

/******************
 * Syncronous API *
 ******************/
/* Connect */
void RedisCxxRunner::syncConnect(const char* ip, const int port){
    this->_syncConnect = redisConnect(ip, port);
}

/* Close */
void RedisCxxRunner::syncClose(){
    redisFree(this->_syncConnect);
}

/* Executer */
bool RedisCxxRunner::syncExecuter(const char* command)
{
    // Parse Command
    auto start = std::chrono::system_clock::now();
    redisReply* reply = (redisReply *)redisCommand(this->_syncConnect , command); 
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    this->_duration = nsec / (1000.0*1000.0*1000.0);
    if(reply->type == REDIS_REPLY_STATUS or
       reply->type == REDIS_REPLY_NIL    or
       reply->type == REDIS_REPLY_STRING){
	// Output Result
	if(reply->str){
	    this->_reply = reply->str;
	}else{
	    this->_reply = "";
	}
	freeReplyObject(reply);
	return true;
    }else if(reply->type == REDIS_REPLY_INTEGER){
	std::string s = std::to_string(reply->integer);
	this->_reply = s.c_str();
	freeReplyObject(reply);
	return true;
    }else if(reply->type == REDIS_REPLY_ARRAY){
	std::string result = "";
	for(unsigned int i = 0; i < reply->elements; i++){
	    if(i > 0){
		result += ",";
		result += reply->element[i]->str;
	    }else{
		result = reply->element[i]->str;
	    }
	}
	this->_reply = result;
	freeReplyObject(reply);
	return true;
    }
    this->_reply = "";
    freeReplyObject(reply);
    return false;
};


/******************
 * Asyncronous API *
 ******************/
/* connect Callback */
void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Connected...\n");
}

/* disconnect Callback */
void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Disconnected...\n");
}

/* Get Callback */
// this callback function is executed at calling redisAsyncDisconnect
void getCallback(redisAsyncContext*, void *r, void *privdata) {
    redisReply *reply = static_cast<redisReply *>(r);
    if(reply != NULL){
	if(reply->type == REDIS_REPLY_STATUS or
	   reply->type == REDIS_REPLY_NIL    or
	   reply->type == REDIS_REPLY_STRING){
	    // Output Result
	    if(reply->str){
		tmpReplies.push_back(reply->str);
	    }else{
		tmpReplies.push_back("");
	    }
	}else if(reply->type == REDIS_REPLY_ARRAY){
	    std::string result = "";
	    for(unsigned int i = 0; i < reply->elements; i++){
		if(i > 0){
		    result += ",";
		    result += reply->element[i]->str;
		}else{
		    result = reply->element[i]->str;
		}
	    }
	    tmpReplies.push_back(result);
	}
    }
}

/* Set IP , PORT */
void RedisCxxRunner::asyncConnect(const char* ip, int port){
    this->_ip = ip;
    this->_port = port;
}

/* Async Close */
void RedisCxxRunner::asyncClose(){
    std::vector<std::string>().swap(tmpReplies);
    std::vector<const char*>().swap(this->_replies);
}

/* commitQuery */
void RedisCxxRunner::commitQuery(const char* query_){
    std::string query = query_;
    this->_queries.push_back(query);
}

/* pooledQuerySize */
unsigned int RedisCxxRunner::pooledQuerySize(){
    return this->_queries.size();
}

/* Get Command Type */
ECommandType RedisCxxRunner::commandType(std::string command){

    std::transform(command.begin(), command.end(), command.begin(), ::tolower);
    const int get      = command.find("get");
    const int hvals    = command.find("hvals");
    const int hkeys    = command.find("hkeys");
    const int smembers = command.find("smembers");
    if(get != -1 or hvals != -1 or hkeys != -1 or smembers != -1 ){
	return TYPE_GET;
    }
    return TYPE_OTHER;
}

/* Executer */
bool RedisCxxRunner::asyncExecuter()
{
    struct event_base *base = event_base_new();
    this->_asyncConnect = redisAsyncConnect(this->_ip, this->_port);
    if (this->_asyncConnect->err) {
        /* Let *c leak for now... */
        printf("Error: %s\n", this->_asyncConnect->errstr);
        return 1;
    }
    // Prologue
    redisLibeventAttach(this->_asyncConnect,base);
    redisAsyncSetConnectCallback(this->_asyncConnect,connectCallback);
    redisAsyncSetDisconnectCallback(this->_asyncConnect,disconnectCallback);
    this->_duration = 0.0;
    std::vector<std::string>().swap(tmpReplies);
    std::vector<const char*>().swap(this->_replies);
    //std::cout << "command size :: " << this->_queries.size() << std::endl;
    auto start = std::chrono::system_clock::now();
    auto end   = std::chrono::system_clock::now();
    auto duration = end - start;
    double nsec;
    nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    nsec = 0.0;
    if(this->_queries.size() > 0){
	for(auto itr = this->_queries.begin(); itr != this->_queries.end(); itr++){
	    //std::cout << "Command:: " << *itr << std::endl;
	    ECommandType type = commandType(*itr);
	    switch(type) {
	    case TYPE_GET:
		start = std::chrono::system_clock::now();
		redisAsyncCommand(this->_asyncConnect, getCallback, (char*)"end-1", (*itr).c_str());
		end = std::chrono::system_clock::now();
		duration = end - start;
		nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
		this->_duration += nsec / (1000.0*1000.0*1000.0);
		break;
	    case TYPE_OTHER:
		start = std::chrono::system_clock::now();
		redisAsyncCommand(this->_asyncConnect, NULL, NULL, (*itr).c_str());
		end = std::chrono::system_clock::now();
		duration = end - start;
		nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
		this->_duration += nsec / (1000.0*1000.0*1000.0);
		break;
	    default:
		std::cout << "ERROR :: UNSUPPORTED QUERY [" << *itr << "]" <<  std::endl;
		break;
	    }
	}
    }else{
	std::cout << "There is not a target command." << std::endl;
	std::cout << "Please Set Command (client->commitQuery())" << std::endl;
    }
    start = std::chrono::system_clock::now();
    redisAsyncDisconnect(this->_asyncConnect);
    end = std::chrono::system_clock::now();
    duration = end - start;
    nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    this->_duration += nsec / (1000.0*1000.0*1000.0);
    // Epilogue
    start = std::chrono::system_clock::now();
    event_base_dispatch(base);
    end = std::chrono::system_clock::now();
    duration = end - start;
    nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    this->_duration += nsec / (1000.0*1000.0*1000.0);
    event_base_free(base);
    std::vector<std::string>().swap(this->_queries);
    return true;
};
