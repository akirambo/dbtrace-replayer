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

/***********************************/
/*! @addtogroup MongodbCxxRunner
  @file mongodb_cxxrunner.cpp
  @author Akira Kuroda
***********************************/

#include <iostream>
#include <vector>
#include <chrono>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/exception/exception.hpp>
#include "mongodb_cxxrunner.hpp"

/*! @class MongodbCxxRunner
  @brief Mongodb Adapter Class
*/

MongodbCxxRunner::MongodbCxxRunner(){
}

MongodbCxxRunner::~MongodbCxxRunner(){
}

bool MongodbCxxRunner::connect(const char* uri){
    this->_client = mongocxx::client{mongocxx::uri{uri}};
    return true;
}

bool MongodbCxxRunner::close(){    
    return true;
}

double MongodbCxxRunner::getDuration(){
    return this->_duration;
}

const char* MongodbCxxRunner::getReply(){
    return this->_reply.c_str();
}

void MongodbCxxRunner::setDatabaseName(std::string databaseName){
    this->_databaseName = databaseName;
}

void MongodbCxxRunner::clearDatabaseName(){
    this->_databaseName = "";
}

void MongodbCxxRunner::setCollectionName(std::string collectionName){
    this->_collectionName = collectionName;
}

void MongodbCxxRunner::clearCollectionName(){
    this->_collectionName = "";
}

bool MongodbCxxRunner::syncExecuter(const char* query, 
				    const char* doc=NULL){
    if(!strcmp(query, "INSERT") or !strcmp(query, "insert")){
	return insertOne(doc);
    }
    return false;
}

bsoncxx::document::value MongodbCxxRunner::json(std::string str){
    bsoncxx::document::value ret = bsoncxx::from_json("{}");
    try{
	ret = bsoncxx::from_json(str);
    } catch (const mongocxx::exception& e){
	std::cout << str << std::endl;
	std::cout << e.what() << std::endl;
    }
    return ret;
}

bool MongodbCxxRunner::insertOne(std::string str){
  // Execute Insert Query
  try{
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      auto start = std::chrono::system_clock::now();
      this->_client[this->_databaseName][this->_collectionName].
	insert_one(json(str));
      auto end = std::chrono::system_clock::now();
      auto duration = end - start;
      double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
      this->_duration = nsec / (1000.0*1000.0*1000.0);
    }else{
      return false;
    }
  } catch (const mongocxx::exception& e) {
    return false;
  }
  return true;
}

bool MongodbCxxRunner::find(std::string str){
  try{
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      auto start = std::chrono::system_clock::now();
      auto cursor = this->_client[this->_databaseName][this->_collectionName]
	.find(json(str));
      auto end = std::chrono::system_clock::now();
      auto duration = end - start;
      double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
      this->_duration = nsec / (1000.0*1000.0*1000.0);
      std::string reply = "";
      for(auto&& doc: cursor){
	reply += bsoncxx::to_json(doc) +"\n";
      }
      this->_reply = reply;
    }else{
      return false;
    }
  } catch (const mongocxx::exception& e) {
    return false;
  }
  return true;
}

bool MongodbCxxRunner::update(std::string filter, std::string doc, const bool multiFlag){
  try {
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      if(multiFlag){
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName][this->_collectionName]
	  .update_many(json(filter), json(doc));
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }else{
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName][this->_collectionName]
	  .update_one(json(filter), json(doc));
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }
    }else{
      this->_duration = 0.0;
      return false;
    }
  } catch (const mongocxx::exception& e){
    return false;
  }
  return true;
}

bool MongodbCxxRunner::deleteExecuter(std::string str,const bool multiFlag){
  try {
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      if(multiFlag){
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName][this->_collectionName].
	  delete_many(json(str));
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }else{
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName][this->_collectionName].
	  delete_one(json(str));
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }
    }else{
      this->_duration = 0.0;
      return false;
    }
  } catch (const mongocxx::exception& e){
    return false;
  }
  return true;
}

int MongodbCxxRunner::count(std::string str){
  int count = 0;
  try{
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      auto start = std::chrono::system_clock::now();
      count = this->_client[this->_databaseName][this->_collectionName].
	count(json(str));
      auto end = std::chrono::system_clock::now();
      auto duration = end - start;
      double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
      this->_duration = nsec / (1000.0*1000.0*1000.0);
    }else{
      this->_duration = 0.0;
      return count;
    }
  } catch (const mongocxx::exception& e){
    // Do Nothing
  }
  return count;
}


bool MongodbCxxRunner::drop(){
  try{
    if(this->_databaseName.length() > 0){
      if(this->_collectionName.length() > 0){
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName][this->_collectionName].drop();
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }else{
	auto start = std::chrono::system_clock::now();
	this->_client[this->_databaseName].drop();
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }
    }else{
      this->_duration = 0.0;
      return false;
    }
  } catch (const mongocxx::exception& e){
    return false;
  }
  return true;
}


bool MongodbCxxRunner::aggregate(){
  try{
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      mongocxx::pipeline stages;
      if(this->_match.size() > 0){
	stages.match(json(this->_match));
      }
      if(this->_group.size() > 0){
	stages.group(json(this->_group));
      }
      if(this->_unwind.size() > 0){
	stages.unwind(json(this->_unwind));
      }
      auto start = std::chrono::system_clock::now();
      auto cursor = this->_client[this->_databaseName][this->_collectionName]
	.aggregate(stages);
      auto end = std::chrono::system_clock::now();
      
      std::string reply = "";
      for (auto&& doc : cursor) {
	reply += bsoncxx::to_json(doc) +"\n";
      }
      this->_reply = reply;
      auto duration = end - start;
      double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
      this->_duration = nsec / (1000.0*1000.0*1000.0);
    }else{
      this->_reply = "";
      this->_duration = 0.0;
    }
  } catch (const mongocxx::exception& e){
    // Do Nothing
    this->_reply = "";
    this->_duration = 0.0;
    resetAggregateCommand();
    return false;
  }
  resetAggregateCommand();
  return true;
}

bool MongodbCxxRunner::commitDocument(std::string str){
  bsoncxx::document::value doc = json(str);
  this->_commitDocuments.push_back(doc);
  return true;
}

bool MongodbCxxRunner::setAggregateCommand(std::string type, std::string str){
  if(!strcmp(type.c_str(),"group")){
    this->_group = str;
  }else if(!strcmp(type.c_str(), "match")){
    this->_match = str;
  }else if(!strcmp(type.c_str(), "unwind")){
    this->_unwind = str;
  }else{
    std::cerr << "[ERROR] :: Unsupported Type " << type << " @ mongodb_cxxrunner.cpp" << std::endl;
    return false;
  }
  return true;
}

void MongodbCxxRunner::resetAggregateCommand(){
  this->_group = "";
  this->_match = "";
  this->_unwind = "";
}

bool MongodbCxxRunner::clearDocuments(){
  this->_commitDocuments.clear();
  return true;
}

bool MongodbCxxRunner::insertMany(){
  try{
    auto start = std::chrono::system_clock::now();
    if(this->_databaseName.length() > 0 and this->_collectionName.length() > 0){
      if(this->_commitDocuments.size() > 0){
	this->_client[this->_databaseName][this->_collectionName]
	  .insert_many(this->_commitDocuments);	
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
	this->_duration = nsec / (1000.0*1000.0*1000.0);
      }
    }else{
      return false;
    }
  } catch (const mongocxx::exception& e) {
    std::cout << e.what() << std::endl;
    return false;
  }
  // Clear Documents
  this->_commitDocuments.clear();
  return true;
}
