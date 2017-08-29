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

#ifndef __MONGODB_CXXRUNNER_HPP__
#define __MONGODB_CXXRUNNER_HPP__

#include <mongocxx/client.hpp>
#include <bsoncxx/builder/basic/document.hpp>

class MongodbCxxRunner
{
private:
    static const int _signature = 0x123f3f7c;

public:
  MongodbCxxRunner();
  ~MongodbCxxRunner();
  bool connect(const char* uri);
  bool syncExecuter(const char* query, const char* docs);
  bool close();
  double getDuration();
  const char* getReply();
  bool isLegal(){return this->_sig == _signature;};
  
  //void setDatabaseName(const char* databaseName);
  void setDatabaseName(std::string databaseName);
  void clearDatabaseName();
  //void setCollectionName(const char* collectionName);
  void setCollectionName(std::string collectionName);
  void clearCollectionName();
  
  
  // For Aggregation
  bool setAggregateCommand(std::string type, std::string str);
  void resetAggregateCommand();
  
  // Only for insertMany
  bool commitDocument(std::string s);
  bool clearDocuments();
  // --- Only For insertMany
  
  // Basic Operation
  bool insertOne(std::string docs);
  bool insertMany(); // it must run after commitDocument();
  bool update(std::string filter, std::string doc, const bool multiFlag);
  bool find(const std::string doc);
  int count(std::string filter);
  bool deleteExecuter(const std::string docs,const bool multiFlag);
  bool drop();
  bool aggregate();
  bsoncxx::document::value json(std::string str);    
    
private:
  int _sig;
  double _duration = 0.0;
  std::string  _reply;
  std::string _databaseName = "";
  std::string _collectionName = "";
  std::vector <bsoncxx::document::value> _commitDocuments;
  mongocxx::client _client{mongocxx::uri{}};
  
  // For Aggregation
  std::string _group  = "";
  std::string _match  = "";
  std::string _unwind = "";
};


#endif // __MONGODB_CXXRUNNER_HPP__

