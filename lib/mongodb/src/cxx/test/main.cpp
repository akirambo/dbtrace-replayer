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
#include <sstream>
#include <string>
#include "../mongodb_cxxrunner.hpp"

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

/******************
 * Test Functions *
 ******************/

// [TEST] Get Database List
void testGetDatabaseList(){
    mongocxx::client tclient{mongocxx::uri{"mongodb://127.0.0.1:27017"}};
    mongocxx::cursor dbs = tclient.list_databases();
    for(auto itr = dbs.begin(); itr != dbs.end(); itr++){
	//std::cout << "TEST :: " << std::endl;
	//std::cout << *itr << std::endl;
    }
}

// [TEST] Insert One 
void testInsertOneAndFind(MongodbCxxRunner* client){
    // [RESET]
    client->drop();

    // [TEST] Insert One
    if(client->insertOne("{\"_id\":\"id000\",\"value\":\"AAAA\"}") and 
       client->insertOne("{\"_id\":\"id001\",\"value\":\"BBBB\"}")){
	std::cout << "PASSED :: TEST(cxx) [InsertOne]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [InsertOne]" << std::endl;
    }
    // [TEST] Find w/o condition
    if(client->find("{}")){
	if(checker(client->getReply(),"id000") and 
	   checker(client->getReply(),"AAAA") and 
	   checker(client->getReply(),"id001") and 
	   checker(client->getReply(),"BBBB")){
	    std::cout << "PASSED :: TEST(cxx) [Find w/o condition]" << std::endl;
	}else{
	    std::cout << "FAILED :: TEST(cxx) [Find w/o condition]" << std::endl;
	    showDetail(client->getReply());
	}
    }
    // [TEST] Find w/ condition
    if(client->find("{\"_id\":\"id000\"}")){
	if(checker(client->getReply(),"id000") and checker(client->getReply(),"AAAA") and 
	   !checker(client->getReply(),"id001") and !checker(client->getReply(),"BBBB")){
	    std::cout << "PASSED :: TEST(cxx) [Find w/ condition]" << std::endl;
	}else{
	    std::cout << "FAILED :: TEST(cxx) [Find w/ condition]" << std::endl;
	    showDetail(client->getReply());
	}
    }else{
	std::cout << "FAILED :: TEST(cxx) [Find w/ condition]" << std::endl;
	std::cout << "       >> Fatal Error" << std::endl;
    }

    // [RESET]
    client->drop();
}

// [TEST] Delete
void testDelete(MongodbCxxRunner* client){
    // [SEUP]
    client->drop();
    client->insertOne("{\"_id\":\"id010\",\"value\":\"AAAA\"}");
    client->insertOne("{\"_id\":\"id011\",\"value\":\"BBBB\"}");
    client->insertOne("{\"_id\":\"id012\",\"value\":\"BBBB\"}");
    client->insertOne("{\"_id\":\"id013\",\"value\":\"AAAA\"}");    

    // [TEST] Delete w/ condition (single)
    if(client->deleteExecuter("{\"value\":\"AAAA\"}",false) and 
       client->find("{\"value\":\"AAAA\"}")){
	if(checker(client->getReply(),"id010") and checker(client->getReply(),"AAAA") and 
	   checker(client->getReply(),"id013") and checker(client->getReply(),"AAAA")){
	    std::cout << "FAILED :: TEST(cxx) [Delete(single) w/ condition]" << std::endl;
	    showDetail(client->getReply());
	}else{
	    std::cout << "PASSED :: TEST(cxx) [Delete(single) w/ condition]" << std::endl;
	}
    }else{
	std::cout << "FAILED :: TEST(cxx) [Delete(single) w/ condition]" << std::endl;
	std::cout << "       >> Fatal Error" << std::endl;
    }

    // [TEST] Delete w/ condition (multi)
    if(client->deleteExecuter("{\"value\":\"BBBB\"}",true) and 
       client->find("{\"value\":\"BBBB\"}")){
	if(!checker(client->getReply(),"id011") and 
	   !checker(client->getReply(),"id012")){
	    std::cout << "PASSED :: TEST(cxx) [Delete(multi) w/ condition]" << std::endl;
	}else{
	    std::cout << "FAILED :: TEST(cxx) [Delete(multi) w/ condition]" << std::endl;
	    showDetail(client->getReply());
	}
    }else{
	std::cout << "FAILED :: TEST(cxx) [Delete(multi) w/ condition]" << std::endl;
	std::cout << "       >> Fatal Error" << std::endl;
    }

    // [TEST] Drop
    client->drop();
}

// [TEST] Update
void testUpdate(MongodbCxxRunner* client){
    // [RESET]
    client->drop();
    client->insertOne("{\"value\":\"A\",\"num\":100}");
    client->insertOne("{\"value\":\"A\",\"num\":110}");
    client->insertOne("{\"value\":\"A\",\"num\":120}");

    client->update("{\"num\":100}","{\"$set\":{\"value\":\"B\",\"num\":999}}",false);
    if(client->count("{\"value\":\"B\"}") == 1){
	std::cout << "PASSED :: TEST(cxx) [update]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [update]" << std::endl;
    }
    client->update("{}","{\"$set\":{\"value\":\"C\"}}",true);
    if(client->count("{\"value\":\"C\"}") == 3){
	std::cout << "PASSED :: TEST(cxx) [update]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [update]" << std::endl;
    }
    // [RESET]
    client->drop();
}


// [TEST] Insert Many
void testInsertMany(MongodbCxxRunner* client){
    // [RESET]
    client->drop();
    // insert_many()
    client->commitDocument("{\"_id\":\"id001\",\"value\":\"A\",\"num\":100}");
    client->commitDocument("{\"_id\":\"id002\",\"value\":\"B\",\"num\":20}");
    client->commitDocument("{\"_id\":\"id003\",\"value\":\"C\",\"num\":40}");
    client->commitDocument("{\"_id\":\"id004\",\"value\":\"D\",\"num\":30}");
    client->commitDocument("{\"_id\":\"id005\",\"value\":\"E\",\"num\":60}");
    client->commitDocument("{\"_id\":\"id006\",\"value\":\"F\",\"num\":80}");
    // 5000 docs
    for(int i=0; i<5000; i++){
	std::stringstream ss;
	ss << i;
	std::string json = "{\"_id\":\"memtier-"+ss.str()+"\",\"value\":\"xxxxxxxx\"}";
	client->commitDocument(json);
    }
    client->insertMany();
    client->find("{}");
    const char* ans = client->getReply();
    if(checker(ans,"id001") and checker(ans,"id002") and 
       checker(ans,"id003") and checker(ans,"id004") and 
       checker(ans,"id005") and checker(ans,"id006")){
	std::cout << "PASSED :: TEST(cxx) [insert_many]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [insert_many]" << std::endl;
	showDetail(client->getReply());
    }
    // [RESET]
    client->drop();
}


// [TEST] Insert & Count
void testInsertAndCount(MongodbCxxRunner* client){
    // [RESET]
    client->drop();
    // insert
    client->insertOne("{\"_id\":\"id001\",\"value\":\"A\",\"num\":10}");
    client->insertOne("{\"_id\":\"id002\",\"value\":\"B\",\"num\":10}");
    client->insertOne("{\"_id\":\"id003\",\"value\":\"C\",\"num\":10}");
    client->insertOne("{\"_id\":\"id004\",\"value\":\"D\",\"num\":30}");
    client->insertOne("{\"_id\":\"id005\",\"value\":\"E\",\"num\":60}");
    client->insertOne("{\"_id\":\"id006\",\"value\":\"F\",\"num\":80}");
    // count
    if(client->count("{}") == 6){
	std::cout << "PASSED :: TEST(cxx) [Count w/o condition]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Count w/o condition]" << std::endl;
    }
    if(client->count("{\"num\":10}") == 3){
    	std::cout << "PASSED :: TEST(cxx) [Count w/ condition]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Count w/ condition]" << std::endl;
    }
    // [RESET]
    client->drop();
}


// [TEST] Insert Deep Hierarcy Documents
void testInsertDeepDocument(MongodbCxxRunner* client){
    // [RESET]
    client->drop();
    // insert
    client->insertOne("{\"_id\":\"id001\",\"value\":\"b\",\"num\":10,\"doc\":{\"layer00\":{\"layer01\":{\"layer02\":\"aa\"}} }}");
    client->insertOne("{\"_id\":\"id002\",\"value\":\"b\",\"num\":11,\"doc\":{\"layer00\":{\"layer01\":{\"layer02\":\"bb\"}} }}");
    client->insertOne("{\"_id\":\"id003\",\"value\":\"c\",\"num\":12,\"doc\":{\"layer00\":{\"layer01\":{\"layer02\":\"cc\"}} }}");
    client->insertOne("{\"_id\":\"id004\",\"value\":\"d\",\"num\":13,\"doc\":{\"layer00\":{\"layer01\":{\"layer02\":\"dd\"}} }}");
    // count

    if(client->count("{}") == 4){
	std::cout << "PASSED :: TEST(cxx) [Three Layers Documenton]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Three Layers Documenton]" << std::endl;
    }
    if(client->count("{\"num\":10}") == 1){
	std::cout << "PASSED :: TEST(cxx) [Three Layers Documenton]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Three Layers Documenton]" << std::endl;
    }
    // [RESET]
    client->drop();
}


void insertTestSet(MongodbCxxRunner* client){
    client->insertOne("{\"_id\":\"id010\",\"value\":\"AAAA\",\"num\":10, \"sizes\":[\"S\",\"M\"]}");
    client->insertOne("{\"_id\":\"id011\",\"value\":\"BBBB\",\"num\":20}");
    client->insertOne("{\"_id\":\"id012\",\"value\":\"BBBB\",\"num\":30}");
    client->insertOne("{\"_id\":\"id013\",\"value\":\"AAAA\",\"num\":40, \"sizes\":[\"S\",\"M\"]}");
}

// [TEST] Aggregation
void testAggregation(MongodbCxxRunner* client){
    // [RESET]
    client->drop();
    insertTestSet(client);
    client->setAggregateCommand("group","{\"_id\":\"$name\",\"total\":{\"$sum\":\"$num\"}}");
    client->aggregate();
    if(checker(client->getReply(), "_id") and
       checker(client->getReply(), "null") and
       checker(client->getReply(), "total") and
       checker(client->getReply(), "100")){
	std::cout << "PASSED :: TEST(cxx) [Aggregate($group)]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Aggregate($group)]" << std::endl;
	std::cout << client->getReply() << std::endl;
    }
    
    // [match+aggregation]
    client->setAggregateCommand("match","{\"value\":\"AAAA\"}");
    client->setAggregateCommand("group","{\"_id\":\"$name\",\"total\":{\"$sum\":\"$num\"},\"max\":{\"$max\":\"$num\"},\"min\":{\"$min\":\"$num\"}}");
    client->aggregate();
    if(checker(client->getReply(), "_id") and checker(client->getReply(), "null") and
       checker(client->getReply(), "total") and checker(client->getReply(), "50") and
       checker(client->getReply(), "max") and checker(client->getReply(), "40") and
       checker(client->getReply(), "min") and checker(client->getReply(), "10")){
	std::cout << "PASSED :: TEST(cxx) [Aggregate($match,$group)]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Aggregate($match,$group)]" << std::endl;
	std::cout << client->getReply() << std::endl;
    }
    
    // [unwind]
    client->setAggregateCommand("unwind","{\"path\":\"$sizes\"}");
    client->aggregate();
    if(checker(client->getReply(), "id010") and checker(client->getReply(), "S") and
       checker(client->getReply(), "id013") and checker(client->getReply(), "M")){
	std::cout << "PASSED :: TEST(cxx) [Aggregate($unwind)]" << std::endl;
    }else{
	std::cout << "FAILED :: TEST(cxx) [Aggregate($unwind)]" << std::endl;
    }
    // [RESET]
    client->drop();
}

int main(){
    MongodbCxxRunner* client;
    client = new MongodbCxxRunner();
    client->connect("mongodb://127.0.0.1:27017");
    client->setDatabaseName("testdb");
    client->setCollectionName("testcollection");

    // [TEST] Get Database List
    testGetDatabaseList();
    // [TEST] Insert One & Find
    testInsertOneAndFind(client);
    // [TEST] Delete Test
    testDelete(client);  
    // [TEST] Update Test
    testUpdate(client);
    // [TEST] Insert Many
    testInsertMany(client);
    // [TEST] Insert & Count
    testInsertAndCount(client);

    // [TEST] Insert Deep Hierarcy Documents
    testInsertDeepDocument(client);

    // [TEST] Distinct
    //testDistinct(client);
    // [TEST] Aggregation
    testAggregation(client);

    client->close();
}
