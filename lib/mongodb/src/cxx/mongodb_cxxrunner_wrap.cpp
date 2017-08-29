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

#include <new>
#include "ruby.h"
#include "mongodb_cxxrunner.hpp"

static MongodbCxxRunner* getMongodbCxxRunner(VALUE self){
    MongodbCxxRunner* p;
    Data_Get_Struct(self, MongodbCxxRunner, p);
    return p;
}

static void wrap_MongodbCxxRunner_free(MongodbCxxRunner* p){
    if(p->isLegal()){
	p->~MongodbCxxRunner();
    }
    ruby_xfree(p);
}

static VALUE wrap_MongodbCxxRunner_alloc(VALUE klass){
    return Data_Wrap_Struct(klass, NULL, wrap_MongodbCxxRunner_free, ruby_xmalloc(sizeof(MongodbCxxRunner)));
}

/*********************
 * Initialize Method *
 *********************/
static VALUE wrap_MongodbCxxRunner_init(VALUE self){
    MongodbCxxRunner* p = getMongodbCxxRunner(self); 
    new (p) MongodbCxxRunner();
    return Qnil;
}

/*********************
 * Set Database Name *
 *********************/
static VALUE wrap_MongodbCxxRunner_setDatabaseName(VALUE self, VALUE _databaseName){
  std::string databaseName = StringValuePtr(_databaseName);
  getMongodbCxxRunner(self)->setDatabaseName(databaseName);
  return Qnil;
}
/***********************
 * Clear Database Name *
 ***********************/
static VALUE wrap_MongodbCxxRunner_clearDatabaseName(VALUE self){
    getMongodbCxxRunner(self)->clearDatabaseName();
    return Qnil;
}

/***********************
 * Set Collection Name *
 **********************/
static VALUE wrap_MongodbCxxRunner_setCollectionName(VALUE self, VALUE _collectionName){
  std::string collectionName = StringValuePtr(_collectionName);
  getMongodbCxxRunner(self)->setCollectionName(collectionName);
  return Qnil;
}
/*************************
 * Clear Collection Name *
 *************************/
static VALUE wrap_MongodbCxxRunner_clearCollectionName(VALUE self){
    getMongodbCxxRunner(self)->clearCollectionName();
    return Qnil;
}

/***********
 * connect *
 ***********/
static VALUE wrap_MongodbCxxRunner_connect(VALUE self, VALUE _uri) {
    const char* uri = StringValuePtr(_uri);
    getMongodbCxxRunner(self)->connect(uri);
    return true;
}

/*********
 * close *
 *********/
static VALUE wrap_MongodbCxxRunner_close(VALUE self){
    getMongodbCxxRunner(self)->close();
    return true;
}

/****************
 * syncExecuter *
 ****************/
static VALUE wrap_MongodbCxxRunner_syncExecuter(VALUE self, VALUE _query, VALUE _docs){
    const char* query = StringValuePtr(_query);
    const char* docs  = StringValuePtr(_docs);
    if(getMongodbCxxRunner(self)->syncExecuter(query,docs)){
	return true;
    }else{
	return false;
    }
}

/************
 * Duration *
 ************/
static VALUE wrap_MongodbCxxRunner_getDuration(VALUE self){
    return DBL2NUM(getMongodbCxxRunner(self)->getDuration());
}

/*********
 * Reply *
 *********/
static VALUE wrap_MongodbCxxRunner_getReply(VALUE self){
    return rb_str_new2(getMongodbCxxRunner(self)->getReply());
}

/*************
 * insertOne *
 *************/
static VALUE wrap_MongodbCxxRunner_insertOne(VALUE self, VALUE doc_){
  std::string doc = StringValuePtr(doc_);
  if(getMongodbCxxRunner(self)->insertOne(doc)){
    return true;
  }else{
    return false;
  }
}

/**************
 * insertMany *
 **************/
static VALUE wrap_MongodbCxxRunner_insertMany(VALUE self){
    if(getMongodbCxxRunner(self)->insertMany()){
	return true;
    }else{
	return false;
    }
}

/******************
 * commitDocument *
 ******************/
static VALUE wrap_MongodbCxxRunner_commitDocument(VALUE self, VALUE doc_){
    std::string doc = StringValuePtr(doc_);
    if(getMongodbCxxRunner(self)->commitDocument(doc)){
	return true;
    }else{
	return false;
    }
}

/******************
 * clearDocuments *
 ******************/
static VALUE wrap_MongodbCxxRunner_clearDocuments(VALUE self){
    if(getMongodbCxxRunner(self)->clearDocuments()){
	return true;
    }else{
	return false;
    }
}


/********
 * find *
 ********/
static VALUE wrap_MongodbCxxRunner_find(VALUE self, VALUE doc_){
    const std::string doc = StringValuePtr(doc_);
    if(getMongodbCxxRunner(self)->find(doc)){
	return true;
    }else{
	return false;
    }
}

/*********
 * count *
 *********/
static VALUE wrap_MongodbCxxRunner_count(VALUE self, VALUE doc_){
    const std::string doc = StringValuePtr(doc_);
    return INT2NUM(getMongodbCxxRunner(self)->count(doc));
}

/**********
 * update *
 **********/
static VALUE wrap_MongodbCxxRunner_update(VALUE self, VALUE filter_, VALUE doc_, VALUE multiFlag_){
    const std::string filter = StringValuePtr(filter_);
    const std::string doc    = StringValuePtr(doc_);
    bool multiFlag;
    if(TYPE(multiFlag_) == T_TRUE){
	multiFlag = true;
    }else{
	multiFlag = false;
    }
    if(getMongodbCxxRunner(self)->update(filter,doc,multiFlag)){
	return true;
    }else{
	return false;
    }
}

/******************
 * deleteExecuter *
 ******************/
static VALUE wrap_MongodbCxxRunner_deleteExecuter(VALUE self, VALUE doc_, VALUE multiFlag_){
    const std::string doc = StringValuePtr(doc_);
    bool multiFlag;
    if(TYPE(multiFlag_) == T_TRUE){
	multiFlag = true;
    }else{
	multiFlag = false;
    }
    if(getMongodbCxxRunner(self)->deleteExecuter(doc,multiFlag)){
	return true;
    }else{
	return false;
    }
}

/********
 * drop *
 ********/
static VALUE wrap_MongodbCxxRunner_drop(VALUE self){
    if(getMongodbCxxRunner(self)->drop()){
	return true;
    }else{
	return false;
    }
}


/*************
 * Aggregate *
 *************/
static VALUE wrap_MongodbCxxRunner_setAggregateCommand(VALUE self, 
						       VALUE type_,
						       VALUE command_){
    const std::string type    = StringValuePtr(type_);
    const std::string command = StringValuePtr(command_);
    if(getMongodbCxxRunner(self)->setAggregateCommand(type,command)){
	return true;
    }else{
	return false;
    }
}

static VALUE wrap_MongodbCxxRunner_aggregate(VALUE self){
    if(getMongodbCxxRunner(self)->aggregate()){
	return true;
    }else{
	return false;
    }
}


// For Require 
extern "C" void Init_mongodbCxxRunner(){
    VALUE c = rb_define_class("MongodbCxxRunner", rb_cObject);
    rb_define_alloc_func(c, wrap_MongodbCxxRunner_alloc); // alloc memory
    rb_define_private_method(c, "initialize", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_init), 0);
    rb_define_method(c, "setDatabaseName",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_setDatabaseName), 1);
    rb_define_method(c, "clearDatabaseName",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_clearDatabaseName), 0);
    rb_define_method(c, "setCollectionName",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_setCollectionName), 1);
    rb_define_method(c, "clearCollectionName",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_clearCollectionName), 0);
    rb_define_method(c, "syncExecuter", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_syncExecuter), 2);
    rb_define_method(c, "connect", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_connect), 1);
    rb_define_method(c, "close", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_close), 0);
    rb_define_method(c, "getDuration", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_getDuration), 0);
    rb_define_method(c, "getReply", RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_getReply), 0);

    rb_define_method(c, "insertOne",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_insertOne), 1);
    rb_define_method(c, "insertMany",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_insertMany), 0);
    rb_define_method(c, "commitDocument",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_commitDocument), 1);
    rb_define_method(c, "clearDocuments",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_clearDocuments), 0);
    rb_define_method(c, "find",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_find), 1);
    rb_define_method(c, "count",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_count), 1);
    rb_define_method(c, "update",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_update), 3);
    rb_define_method(c, "deleteExecuter",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_deleteExecuter), 2);
    rb_define_method(c, "drop",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_drop), 0);
    rb_define_method(c, "aggregate",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_aggregate), 0);
    rb_define_method(c, "setAggregateCommand",
		     RUBY_METHOD_FUNC(wrap_MongodbCxxRunner_setAggregateCommand), 2);
}
