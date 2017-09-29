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

#include <cassandra.h>
#include <chrono>
#include <iostream>
#include "cassandra_cxxrunner.hpp"

CassandraCxxRunner::CassandraCxxRunner(){
    this->_sig = _signature;
}

CassandraCxxRunner::~CassandraCxxRunner(){
}

double CassandraCxxRunner::getDuration(){
    return this->_duration;
}

std::string CassandraCxxRunner::getReply(unsigned int number){
    const CassResult* result = NULL;
    if(this->_results.size() > number){
	result = this->_results.at(number);
	if(result){
	    return  parseResult(result);
	}
    }
    return "";
}

/* Connect */
void CassandraCxxRunner::connect(const char* ip){
    this->_cluster = cass_cluster_new();
    this->_session = cass_session_new();
    cass_cluster_set_protocol_version(this->_cluster,4);
    cass_cluster_set_contact_points(this->_cluster, ip);
    cass_cluster_set_max_concurrent_requests_threshold(this->_cluster, 00);
    this->_connect_future = 
	cass_session_connect(this->_session, this->_cluster);
    if (cass_future_error_code(this->_connect_future) != CASS_OK) {	
	this->close();
    }
}

/* Close */
void CassandraCxxRunner::close(){
    cass_future_free(this->_connect_future);
    cass_cluster_free(this->_cluster);
    cass_session_free(this->_session);
    resetResults();
}


/* Reset Results */
void CassandraCxxRunner::resetResults(){
    for(auto itr = this->_results.begin(); itr != this->_results.end(); itr++){
	if(*itr != NULL){
	    cass_result_free(*itr);
	}
    }
    this->_results.clear();
}


/******************
 * Syncronous API *
 ******************/

/* Executer */
bool CassandraCxxRunner::syncExecuter(const char* command)
{
  //std::cout << "***********************************" << std::endl;
  //std::cout << "Sync Executer :: " <<  command << std::endl;
    resetResults();
    double nsec = 0.0;
    if(this->_session != NULL){
	auto start = std::chrono::system_clock::now();
	CassStatement* query = cass_statement_new(command,0);
	CassFuture* future = cass_session_execute(this->_session, query);
	cass_future_wait(future);
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
	this->_duration = nsec / (1000.0*1000.0*1000.0);
	//std::cout << " Duration :: " << this->_duration << std::endl;
	if(cass_future_error_code(future) != CASS_OK) {
	    std::cout << "[ERROR on CQL]:: " << command << std::endl;
	    return false;
	}
	this->_results.push_back(cass_future_get_result(future));
    }
    return true;
}

/* Reset Database */
bool CassandraCxxRunner::resetDatabase()
{
    if(this->_session != NULL){
	std::string keyspace = "";
	syncExecuter("select keyspace_name from system.schema_keyspaces;");
	std::string result = getReply(0);
	std::string command = "";
	std::string::size_type pos = 0;
	std::string::size_type prev = 0;
	while ((pos = result.find("\n", prev)) != std::string::npos)
	{
	    keyspace = result.substr(prev, pos - prev);
	    if(keyspace != "system" and 
	       keyspace != "system_traces"){
		command = "drop keyspace " + keyspace;
		syncExecuter(command.c_str());
	    }
	    prev = pos + 1;
	}
	// To get the last substring 
	keyspace =result.substr(prev);
	if(keyspace != "system" and 
	   keyspace != "system_traces"){
	    command = "drop keyspace " + keyspace;
	    syncExecuter(command.c_str());
	}
    }
    return true;
}


/******************
 * Asyncronous API *
 ******************/
void CassandraCxxRunner::commitQuery(std::string command){
    //std::cout << "  commitQuery :" << command <<  std::endl;
    this->_queries.push_back(command);
}

void CassandraCxxRunner::resetQuery(){
    this->_queries.clear();
}

/* Executer */
bool CassandraCxxRunner::asyncExecuter()
{
    std::vector <CassFuture*> futures;
    this->_duration = 0.0;
    // Init Results
    resetResults();
    //std::cout << "Async Executer" << std::endl;
    if(this->_session != NULL){
	CassError rc = CASS_OK;
	bool returnFlag = true;
	auto start = std::chrono::system_clock::now();
	for(auto itr = this->_queries.begin(); itr != this->_queries.end(); itr++){
	    CassStatement* statement = cass_statement_new(itr->c_str(),0);
	    //std::cout << itr->c_str() << std::endl;
	    futures.push_back(cass_session_execute(this->_session, statement));
	    cass_statement_free(statement);
	}

	for(auto future = futures.begin();future != futures.end();future++){
	    cass_future_wait(*future);
	    rc = cass_future_error_code(*future);
	    if (rc == CASS_OK) {
		this->_results.push_back(cass_future_get_result(*future));
	    }else{
		const char* message;
		size_t message_length;
		cass_future_error_message(*future, &message, &message_length);
		fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
		returnFlag = false;
	    }
	    if(!returnFlag){
		resetQuery();
		return false;
	    }
	}
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	double nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
	this->_duration = nsec / (1000.0*1000.0*1000.0);
	for(auto itr = futures.begin(); itr != futures.end();itr++){
	    cass_future_free(*itr);
	}
	futures.clear();
	resetQuery();
    }
    return true;
}

/// Parse Result
std::string CassandraCxxRunner::parseResult(const CassResult* result){
    std::string res = "";    
    CassIterator* itr = cass_iterator_from_result(result);
    CassIterator* collectionItr;
    while(cass_iterator_next(itr)){
	const CassRow* row = cass_iterator_get_row(itr);
       	size_t index = 0;
	bool initFlag = true;
	std::string str;
	while(1){
	    str = "";
	    const CassValue* value = cass_row_get_column(row,index);
	    if(value == NULL){
		break;
	    }
	    int outputInt = 0;
	    long int outputBigInt = 0 ;
	    double outputDouble = 0.0;
	    float outputFloat = 0.0;
 	    const char* outputString = "";
	    const char* outputText = "";
	    size_t value_length_text;
	    size_t value_length;
	    cass_bool_t outputBool = cass_false;

	    // For Hash
	    const char* key0;
	    const char* val0;
	    size_t key0_length;
	    size_t val0_length;
	    std::ostringstream ostr;
	    //unsigned int ostrLength = 0;
	    switch (cass_value_type(value)) {
	    case CASS_VALUE_TYPE_UNKNOWN:
		std::cout << "Unsupported type :: CASS_VALUE_TYPE_UNKNOWN";
		std::cout << " @ cassandra_cxxrunner.cpp" << std::endl;
		break;
	    case CASS_VALUE_TYPE_CUSTOM:
		std::cout << "Uncorfirmed type :: CASS_VALUE_TYPE_CUSTOM";
		std::cout << " @ cassandra_cxxrunner.cpp" << std::endl;
		cass_value_get_string(value, &outputText, &value_length_text);
		str = outputText;
		str = str.substr(0,value_length_text);
		break;
	    case CASS_VALUE_TYPE_INT:
		cass_value_get_int32(value, &outputInt);
		str = std::to_string(outputInt);
		break;
	    case CASS_VALUE_TYPE_BIGINT:
		cass_value_get_int64(value, &outputBigInt);
		str = std::to_string(outputBigInt);
		break;
	    case CASS_VALUE_TYPE_DOUBLE:
		cass_value_get_double(value, &outputDouble);
		str = std::to_string(outputDouble);
		break;
	    case CASS_VALUE_TYPE_FLOAT:
		outputFloat = 0.0;
		cass_value_get_float(value, &outputFloat);
		str = std::to_string(outputFloat);
		break;
	    case CASS_VALUE_TYPE_BOOLEAN:
		cass_value_get_bool(value, &outputBool);
		if(outputBool == cass_false){
		    str = "false";
		}else{
		    str = "true";
		}
		break;
	    case CASS_VALUE_TYPE_TEXT:
		cass_value_get_string(value, &outputText, &value_length_text);
		str = outputText;
		str = str.substr(0,value_length_text);
		break;
	    case CASS_VALUE_TYPE_VARCHAR:
		cass_value_get_string(value, &outputString, &value_length);
		str = outputString;
		str = str.substr(0,value_length);
		break;
	    case CASS_VALUE_TYPE_SET:
		collectionItr = cass_iterator_from_collection(value);
		if(collectionItr != NULL){
		    while(cass_iterator_next(collectionItr)){
			cass_value_get_string(cass_iterator_get_value(collectionItr), 
					      &outputString, &value_length);
			if(outputString){
			    str = outputString;
			    str = str.substr(0,value_length);
			    ostr << "'" << str << "'";
			    ostr << ",";
			}else{
			    ostr << "'',";
			}
		    }
		    str = ostr.str();
		    if(str.length() > 0){
			str.pop_back();
		    }else{
			str = "''";
		    }
		    str = "{" + str + "}";
		}else{
		    str = "{}";
		}
		cass_iterator_free(collectionItr);
		break;
	    case CASS_VALUE_TYPE_MAP:
		collectionItr = cass_iterator_from_map(value);
		while(cass_iterator_next(collectionItr)){
		    cass_value_get_string(cass_iterator_get_map_key(collectionItr),
					  &key0,&key0_length);
		    cass_value_get_string(cass_iterator_get_map_value(collectionItr),
					  &val0,&val0_length);
		    str = key0;
		    str = str.substr(0,key0_length);
		    ostr << "'" << str << "' : '";
		    str = val0;
		    str = str.substr(0,val0_length);
		    ostr << str << "',";

		}
		str = ostr.str();
		if(str.length() > 0){
		    str.pop_back();
		}else{
		    str = "''";
		}
		str = "{" + str + "}";
		cass_iterator_free(collectionItr);
		break;
	    default:
		std::cout << "Unsupported type :: " << cass_value_type(value) ;
		std::cout << " @ cassandra_cxxrunner.cpp" << std::endl;
		break;
	    } 
	    if(initFlag){
		res += str;
		initFlag = false;
	    }else{
		res += ","+ str;
	    }
	    index += 1;
	}
	res += "\n";
    }
    if(res.length() == 0){
	return "";
    }
    res.erase(--res.end());
    //return res.c_str();
    return res;
}
