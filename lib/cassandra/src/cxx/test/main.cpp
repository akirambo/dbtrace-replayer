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


#include <string.h>
#include "../cassandra_cxxrunner.hpp"

using namespace std;
int main(){
    /************
     * Prologue *
     ************/
    CassandraCxxRunner* client ;
    client = new CassandraCxxRunner();
    client->connect("127.0.0.1");

    if(client->syncExecuter("drop keyspace if exists testdb")){
	cout << "TEST [DROP Keyspace] : PASSED" << endl; 
    }else{
	cout << "TEST [DROP Keyspace] : FAILED" << endl; 
    }
    if(client->syncExecuter("create keyspace testdb with replication = {'class':'SimpleStrategy','replication_factor':3}")){
	cout << "TEST [CREATE Keyspace] : PASSED" << endl; 
    }else{
	cout << "TEST [CREATE Keyspace] : FAILED" << endl; 
    }
    if(client->syncExecuter("create table testdb.test( id int, value text, primary key (id));")){
	cout << "TEST [CREATE TABLE] : PASSED" << endl; 
    }else{
	cout << "TEST [CREATE TABLE] : FAILED" << endl; 
    }
    if(client->syncExecuter("insert into testdb.test (id,value) values (1, 'AAAA')")){
	cout << "TEST [INSERT] : PASSED" << endl; 
    }else{
	cout << "TEST [INSERT] : FAILED" << endl; 
    }

    if(client->syncExecuter("select id,value from testdb.test")){
	if(!strcmp(client->getReply(0).c_str(),"1,AAAA")){
	    cout << "TEST [SELECT] : PASSED" << endl; 
	}else{
	    cout << "TEST [SELECT] : FAILED" << endl; 
	}
    }else{
	cout << "TEST [SELECT] : FAILED" << endl; 
    }
    if(!client->syncExecuter("select * from testdb.notExist")){
	cout << "TEST [SELECT(not exist table)] : PASSED" << endl; 
    }else{
	cout << "TEST [SELECT(not exist table)] : FAILED" << endl; 
    }
    
    for(int i=0; i < 100; i++){
	std::stringstream ss;
	ss << i;
	std::string q = "insert into testdb.test (id,value) values ("+ ss.str() +", 'BBBB')";
	client->commitQuery(q.c_str());
    }
    if(client->asyncExecuter()){
	cout << "TEST [INSERT (async)] : PASSED" << endl; 
    }else{
	cout << "TEST [INSERT (async)] : FAILED" << endl; 
    }
    // SELECT @Async
    client->commitQuery("select value from testdb.test where id = 0"); 
    client->commitQuery("select count(*) from testdb.test");
    if(client->asyncExecuter()){
	if(strstr(client->getReply(0).c_str(),"BBBB") != NULL){
	    cout << "TEST [SELECT (async)] : PASSED" << endl; 
	}else{
	    cout << "TEST [SELECT (async)] : FAILED" << endl; 
	}
	if(strcmp(client->getReply(1).c_str(),"100") == 0){
	    cout << "TEST [COUNT (async)] : PASSED" << endl; 
	}else{
	    cout << "TEST [COUNT (async)] : FAILED" << endl; 
	}
    }else{
	cout << "ERROR " << endl;
    }
   
    /********************
     * TEST for SET/MAP *
     ********************/
    
    if(client->syncExecuter("CREATE TABLE testdb.collection ( id TEXT, name TEXT, internal MAP<TEXT,TEXT>, pictures SET<TEXT>, primary key(id) );")){
	cout << "TEST [CREATE TABLE] : PASSED" << endl; 
    }else{
	cout << "TEST [CREATE TABLE] : FAILED" << endl; 
    }
    if(client->syncExecuter("INSERT INTO testdb.collection (name,internal,pictures,id) VALUES ('Product 001',{'_id':'product00001','parent':'Category','ancestors':'[]','amount':'10','currency':'USD'},{},'ObjectId590a2652689e344c9ce33582');")){
	cout << "TEST [INSERT] : PASSED" << endl; 
    }else{
	cout << "TEST [INSERT] : FAILED" << endl; 
    }
    
    if(client->syncExecuter("select * from testdb.collection")){
	std::string ans = "ObjectId590a2652689e344c9ce33582,{'_id' : 'product00001','amount' : '10','ancestors' : '[]','currency' : 'USD','parent' : 'Category'},Product 001,{}";
	if(strstr(client->getReply(0).c_str(),ans.c_str()) != NULL){
	    cout << "TEST [SELECT (MAP & SET)] : PASSED" << endl; 
	}else{
	    cout << "TEST [SELECT (MAP & SET)] : FAILED" << endl; 
	    cout << client->getReply(0) << endl;
	}
    }else{
	cout << "TEST [SELECT] : FAILED" << endl; 
    }    
    client->syncExecuter("drop keyspace if exists testdb");

    /***********************
     * TEST For Async-mode *
     ***********************/
    client->syncExecuter("create keyspace testdb with replication = {'class':'SimpleStrategy','replication_factor':3}");
    client->syncExecuter("create table testdb.test( id int, value text, primary key (id));");
    client->syncExecuter("insert into testdb.test (id,value) values (0, 'XXXX')");
    client->syncExecuter("select count(id) from testdb.test");
    std::cout << client->getReply(0) << std::endl;

    client->commitQuery("insert into testdb.test (id,value) values (1, 'AAAA')");
    client->commitQuery("insert into testdb.test (id,value) values (2, 'BBBB')");
    client->commitQuery("insert into testdb.test (id,value) values (3, 'CCCC')");
    client->commitQuery("insert into testdb.test (id,value) values (4, 'DDDD')");
    client->commitQuery("insert into testdb.test (id,value) values (4, 'DDDD')");
    client->commitQuery("select count(*) from testdb.test");
    std::cout << client->getReply(0) << std::endl;
    client->asyncExecuter();
    client->syncExecuter("select count(id) from testdb.test");
    std::cout << client->getReply(0) << std::endl;
    //client->syncExecuter("select * from testdb.test");
    //std::cout << client->getReply(0) << std::endl;
    client->syncExecuter("drop keyspace if exists testdb");
<<<<<<< HEAD

=======
>>>>>>> 098663becb66002e2f403959e3a6593559a636ff

    /************
     * Epiologue *
     ************/
    client->resetDatabase();
    client->close();
    free(client);
    
    return 0;
}
