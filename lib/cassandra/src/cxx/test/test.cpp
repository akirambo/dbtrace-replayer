/*
  This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
    distribute this software, either in source code form or as a compiled
    binary, for any purpose, commercial or non-commercial, and by any
  means.

    In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
    OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
    ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

    For more information, please refer to <http://unlicense.org/>
*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <chrono>
#include <iostream>
#include <vector>
#include "cassandra.h"

#define NUM_CONCURRENT_REQUESTS 100

void print_error(CassFuture* future) {
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}


CassCluster* create_cluster(const char* hosts) {
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, hosts);
    return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
    CassError rc = CASS_OK;
    CassFuture* future = cass_session_connect(session, cluster);
    
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
	print_error(future);
    }
    cass_future_free(future);

    return rc;
}

CassError execute_query(CassSession* session, const char* query) {
    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(query, 0);

    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
	print_error(future);
    }

    cass_future_free(future);
    cass_statement_free(statement);

    return rc;
}
CassError reset_table(CassSession* session){
    CassError rc = CASS_OK;
    rc = execute_query(session,
		  "DROP KEYSPACE IF EXISTS examples;");
    if(rc != CASS_OK){
	return rc;
    }
    rc = execute_query(session,
		  "CREATE KEYSPACE examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '3' };");
    if(rc != CASS_OK){
	return rc;
    }
    rc = execute_query(session,
		       "CREATE TABLE examples.async (key text, \
                                              bln boolean,		\
                                              flt float, dbl double,	\
                                              i32 int, i64 bigint,	\
                                              PRIMARY KEY (key));");
    
    return rc;
}

/// Parse Result
void parse_result(const CassResult* result){
    CassIterator* itr = cass_iterator_from_result(result);
    std::string res = "";
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
	    double outputDouble = 0.0;
	    float outputFloat = 0.0;
 	    const char* outputString = "";
	    const char* outputText = "";
	    size_t value_length_text;
	    size_t value_length;
	    cass_bool_t outputBool = cass_false;
	    switch (cass_value_type(value)) {
	    case CASS_VALUE_TYPE_INT:
		cass_value_get_int32(value, &outputInt);
		str = std::to_string(outputInt);
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
		break;
	    case CASS_VALUE_TYPE_VARCHAR:
		cass_value_get_string(value, &outputString, &value_length);
		str = outputString;
		break;
	    default:
		std::cout << "unsupported type" << std::endl;
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
    res.erase(--res.end());
    //std::cout << res << std::endl;
}

void insert_into_async(CassSession* session, const char* key) {
    CassError rc = CASS_OK;
    CassStatement* statement = NULL;
    const char* query = "INSERT INTO async (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";

    CassFuture* futures[NUM_CONCURRENT_REQUESTS];

    size_t i;
    auto start = std::chrono::system_clock::now();
    for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
	char key_buffer[64];
	statement = cass_statement_new(query, 6);

	sprintf(key_buffer, "%s%u", key, (unsigned int)i);
	cass_statement_bind_string(statement, 0, key_buffer);
	cass_statement_bind_bool(statement, 1, i % 2 == 0 ? cass_true : cass_false);
	cass_statement_bind_float(statement, 2, i / 2.0f);
	cass_statement_bind_double(statement, 3, i / 200.0);
	cass_statement_bind_int32(statement, 4, (cass_int32_t)(i * 10));
	cass_statement_bind_int64(statement, 5, (cass_int64_t)(i * 100));

	futures[i] = cass_session_execute(session, statement);
	cass_statement_free(statement);
    }
    for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
	CassFuture* future = futures[i];

	cass_future_wait(future);

	rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
	    print_error(future);
	}
	cass_future_free(future);
    }
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    float sec = nsec / (1000.0*1000.0*1000.0);
    std::cout << "Async(insert) ::" << sec << " [SEC] " << std::endl;
}


double select_async(CassSession* session, std::vector<const char*> queries) {
    CassError rc = CASS_OK;
    std::vector<CassFuture*> futures;
    std::vector<const CassResult*> results;
    auto start = std::chrono::system_clock::now();
    for(auto itr = queries.begin(); itr != queries.end(); itr++){
	CassStatement* statement = cass_statement_new(*itr,0);	
	futures.push_back(cass_session_execute(session,statement));
	cass_statement_free(statement);
    }
    for(auto future = futures.begin(); future != futures.end(); future++){
	cass_future_wait(*future);
	rc = cass_future_error_code(*future);
	if (rc != CASS_OK) {
	    print_error(*future);
	}else{
	    results.push_back(cass_future_get_result(*future));
	}
	cass_future_free(*future);
    }
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    double totalDuration = nsec / (1000.0*1000.0*1000.0);


    /**GET RESULTS**/
    for(auto result = results.begin(); result != results.end(); result++){
	// Parse One Query
	parse_result(*result);
    }
    futures.clear();
    results.clear();
    return totalDuration;
}


double select_sync(CassSession* session,const char* command) {
    CassError rc = CASS_OK;
    CassStatement* query = cass_statement_new(command,0);
    auto start = std::chrono::system_clock::now();
    CassFuture* future = cass_session_execute(session, query);
    cass_future_wait(future);
    rc = cass_future_error_code(future);
    auto end = std::chrono::system_clock::now();
    auto duration = end - start;
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
    double sec = nsec / (1000.0*1000.0*1000.0);
    
    if (rc != CASS_OK) {
	print_error(future);
    }
    const CassResult* result = cass_future_get_result(future);
    parse_result(result);
    return sec;
}

void insert_into_sync(CassSession* session, const char* key) {
    CassError rc = CASS_OK;
    CassStatement* statement = NULL;
    const char* query = "INSERT INTO async (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);";
    float totalDuration = 0.0;
    CassFuture* future = NULL;

    size_t i;
    for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
	char key_buffer[64];
	statement = cass_statement_new(query, 6);

	sprintf(key_buffer, "%s%u", key, (unsigned int)i);
	cass_statement_bind_string(statement, 0, key_buffer);
	cass_statement_bind_bool(statement, 1, i % 2 == 0 ? cass_true : cass_false);
	cass_statement_bind_float(statement, 2, i / 2.0f);
	cass_statement_bind_double(statement, 3, i / 200.0);
	cass_statement_bind_int32(statement, 4, (cass_int32_t)(i * 10));
	cass_statement_bind_int64(statement, 5, (cass_int64_t)(i * 100));
	auto start = std::chrono::system_clock::now();
	future = cass_session_execute(session, statement);
	cass_future_wait(future);
	auto end = std::chrono::system_clock::now();
	auto duration = end - start;
	auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() ;
	totalDuration += nsec / (1000.0*1000.0*1000.0);
	rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
	    print_error(future);
	}
	cass_statement_free(statement);
    }
    std::cout << "Sync (insert) ::"<< totalDuration << " [SEC] " << std::endl;
}

int main(int argc, char* argv[]) {
    CassCluster* cluster = NULL;
    CassSession* session = cass_session_new();
    CassFuture* close_future = NULL;
    const char* hosts = "127.0.0.1";
    if (argc > 1) {
	hosts = argv[1];
    }
    cluster = create_cluster(hosts);

    if (connect_session(session, cluster) != CASS_OK) {
	cass_cluster_free(cluster);
	cass_session_free(session);
	return -1;
    }
    

    /********
     * SYNC *
     ********/
    reset_table(session);    
    execute_query(session, "USE examples");
    insert_into_sync(session, "test");

    double sec = 0.0;
    // Dummy (cache)
    select_sync(session,"SELECT key FROM async;");
    sec = select_sync(session,"SELECT key FROM async;");
    sec = select_sync(session,"SELECT bln FROM async;");
    std::cout << "Sync (select) ::" << sec << " [SEC] " << std::endl;


    /*********
     * ASYNC *
     *********/
    reset_table(session);    
    execute_query(session, "USE examples");
    insert_into_async(session, "test");
    std::vector <const char*> queries;
    // dummy
    queries.push_back("SELECT key FROM async;");
    select_async(session,queries);
    queries.clear();
    sec = 0.0;
    queries.push_back("SELECT key FROM async;");
    queries.push_back("SELECT bln FROM async;");
    sec = select_async(session,queries);
    std::cout << "Async(select) ::" << sec << " [SEC] " << std::endl;

    
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);

    return 0;
}

