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
#include "cassandra_cxxrunner.hpp"

static CassandraCxxRunner* getCassandraCxxRunner(VALUE self){
    CassandraCxxRunner* p;
    Data_Get_Struct(self, CassandraCxxRunner, p);
    return p;
}

static void wrap_CassandraCxxRunner_free(CassandraCxxRunner* p){
    if(p->isLegal()){
	p->~CassandraCxxRunner();
    }
    ruby_xfree(p);
}

static VALUE wrap_CassandraCxxRunner_alloc(VALUE klass){
    return Data_Wrap_Struct(klass, NULL, wrap_CassandraCxxRunner_free, ruby_xmalloc(sizeof(CassandraCxxRunner)));
}

/*********************
 * Initialize Method *
 *********************/
static VALUE wrap_CassandraCxxRunner_init(VALUE self){
    CassandraCxxRunner* p = getCassandraCxxRunner(self); 
    new (p) CassandraCxxRunner();
    return Qnil;
}

/****************
 * syncExecuter *
 ****************/
static VALUE wrap_CassandraCxxRunner_syncExecuter(VALUE self, VALUE _command){
    const char* command = StringValuePtr(_command);
    if(getCassandraCxxRunner(self)->syncExecuter(command)){
	return true;
    }else{
	return false;
    }
}

/****************
 * asyncExecuter *
 ****************/
static VALUE wrap_CassandraCxxRunner_asyncExecuter(VALUE self){
    if(getCassandraCxxRunner(self)->asyncExecuter()){
	return true;
    }else{
	return false;
    }
}

/***************
 * commitQuery *
 ***************/
static VALUE wrap_CassandraCxxRunner_commitQuery(VALUE self, VALUE _command){
    std::string command = StringValuePtr(_command);
    getCassandraCxxRunner(self)->commitQuery(command);
    return true;
}

/**************
 * resetQuery *
 **************/
static VALUE wrap_CassandraCxxRunner_resetQuery(VALUE self){
    getCassandraCxxRunner(self)->resetQuery();
    return true;
}

/*****************
 * resetDatabase *
 *****************/
static VALUE wrap_CassandraCxxRunner_resetDatabase(VALUE self){
    getCassandraCxxRunner(self)->resetDatabase();
    return true;
}


/***********
 * connect *
 ***********/
static VALUE wrap_CassandraCxxRunner_connect(VALUE self, VALUE _ip) {
    const char* ip = StringValuePtr(_ip);
    getCassandraCxxRunner(self)->connect(ip);
    return true;
}

/*********
 * close *
 *********/
static VALUE wrap_CassandraCxxRunner_close(VALUE self){
    getCassandraCxxRunner(self)->close();
    return true;
}

/************
 * Duration *
 ************/
static VALUE wrap_CassandraCxxRunner_getDuration(VALUE self){
    return DBL2NUM(getCassandraCxxRunner(self)->getDuration());
}

/*********
 * Reply *
 *********/
static VALUE wrap_CassandraCxxRunner_getReply(VALUE self, VALUE number_){
    const int number =  NUM2INT(number_);
    std::string result = getCassandraCxxRunner(self)->getReply(number);
    return rb_str_new2(result.c_str());
}


// For Require 
extern "C" void Init_cassandraCxxRunner(){
    VALUE c = rb_define_class("CassandraCxxRunner", rb_cObject);
    rb_define_alloc_func(c, wrap_CassandraCxxRunner_alloc); // alloc memory
    rb_define_private_method(c, "initialize", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_init), 0);
    rb_define_method(c, "syncExecuter", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_syncExecuter), 1);
    rb_define_method(c, "connect", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_connect), 1);
    rb_define_method(c, "close", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_close), 0);
    rb_define_method(c, "getDuration", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_getDuration), 0);
    rb_define_method(c, "getReply", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_getReply), 1);

    rb_define_method(c, "asyncExecuter", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_asyncExecuter), 0);
    rb_define_method(c, "commitQuery", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_commitQuery), 1);
    rb_define_method(c, "resetQuery", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_resetQuery), 0);
    rb_define_method(c, "resetDatabase", RUBY_METHOD_FUNC(wrap_CassandraCxxRunner_resetDatabase), 0);
}
