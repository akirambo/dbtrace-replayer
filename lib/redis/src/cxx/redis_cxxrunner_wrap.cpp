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
#include "redis_cxxrunner.hpp"

static RedisCxxRunner* getRedisCxxRunner(VALUE self){
    RedisCxxRunner* p;
    Data_Get_Struct(self, RedisCxxRunner, p);
    return p;
}

static void wrap_RedisCxxRunner_free(RedisCxxRunner* p){
    if(p->isLegal()){
	p->~RedisCxxRunner();
    }
    ruby_xfree(p);
}

static VALUE wrap_RedisCxxRunner_alloc(VALUE klass){
    return Data_Wrap_Struct(klass, NULL, wrap_RedisCxxRunner_free, ruby_xmalloc(sizeof(RedisCxxRunner)));
}

/*********************
 * Initialize Method *
 *********************/
static VALUE wrap_RedisCxxRunner_init(VALUE self){
    RedisCxxRunner* p = getRedisCxxRunner(self); 
    new (p) RedisCxxRunner();
    return Qnil;
}

/****************
 * syncExecuter *
 ****************/
static VALUE wrap_RedisCxxRunner_syncExecuter(VALUE self, VALUE _command){
    const char* command = StringValuePtr(_command);
    if(getRedisCxxRunner(self)->syncExecuter(command)){
	return true;
    }else{
	return false;
    }
}

/***********
 * connect *
 ***********/
static VALUE wrap_RedisCxxRunner_syncConnect(VALUE self, VALUE _ip, VALUE _port) {
    const char* ip   = StringValuePtr(_ip);
    int   port = NUM2INT(_port);
    getRedisCxxRunner(self)->syncConnect(ip, port);
    return true;
}

/*********
 * close *
 *********/
static VALUE wrap_RedisCxxRunner_syncClose(VALUE self){
    getRedisCxxRunner(self)->syncClose();
    return true;
}

/****************
 * Get Duration *
 ****************/
static VALUE wrap_RedisCxxRunner_getDuration(VALUE self){
    return DBL2NUM(getRedisCxxRunner(self)->getDuration());
}

/******************
 * Reset Duration *
 ******************/
static VALUE wrap_RedisCxxRunner_resetDuration(VALUE self){
    getRedisCxxRunner(self)->resetDuration();
    return true;
}

/*********
 * Reply *
 *********/
static VALUE wrap_RedisCxxRunner_getReply(VALUE self){
    return rb_str_new2(getRedisCxxRunner(self)->getReply());
}

/**************
 * AsyncReply *
 **************/
static VALUE wrap_RedisCxxRunner_getAsyncReply(VALUE self){
    return rb_str_new2(getRedisCxxRunner(self)->getAsyncReply());
}

/*****************
 * Async Connect *
 *****************/
static VALUE wrap_RedisCxxRunner_asyncConnect(VALUE self, VALUE _ip, VALUE _port){
    char* ip   = StringValuePtr(_ip);
    int   port = NUM2INT(_port);
    
    getRedisCxxRunner(self)->asyncConnect(ip,port);
    return true;
}

/***************
 * Async Close *
 ***************/
static VALUE wrap_RedisCxxRunner_asyncClose(VALUE self){
    getRedisCxxRunner(self)->asyncClose();
    return true;
}

/******************
 * Async Executer *
 ******************/
static VALUE wrap_RedisCxxRunner_asyncExecuter(VALUE self){
    getRedisCxxRunner(self)->asyncExecuter();
    return true;
}

static VALUE wrap_RedisCxxRunner_commitQuery(VALUE self, VALUE _query){
    char* query = StringValuePtr(_query); 
    getRedisCxxRunner(self)->commitQuery(query);
    return true;
}

static VALUE wrap_RedisCxxRunner_pooledQuerySize(VALUE self){
    return INT2NUM(getRedisCxxRunner(self)->pooledQuerySize());
}


// For Require 
extern "C" void Init_redisCxxRunner(){
    VALUE c = rb_define_class("RedisCxxRunner", rb_cObject);
    rb_define_alloc_func(c, wrap_RedisCxxRunner_alloc); // alloc memory
    rb_define_private_method(c, "initialize", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_init), 0);
    rb_define_method(c, "syncExecuter", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_syncExecuter), 1);
    rb_define_method(c, "syncConnect", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_syncConnect), 2);
    rb_define_method(c, "syncClose", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_syncClose), 0);
    rb_define_method(c, "getDuration", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_getDuration), 0);
    rb_define_method(c, "getReply", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_getReply), 0);
    rb_define_method(c, "getAsyncReply", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_getAsyncReply), 0);

    rb_define_method(c, "resetDuration", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_resetDuration), 0);
    rb_define_method(c, "asyncConnect", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_asyncConnect), 2);
    rb_define_method(c, "asyncExecuter", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_asyncExecuter), 0);
    rb_define_method(c, "asyncClose", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_asyncClose), 0);
    rb_define_method(c, "commitQuery", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_commitQuery), 1);
    rb_define_method(c, "pooledQuerySize", RUBY_METHOD_FUNC(wrap_RedisCxxRunner_pooledQuerySize), 0);
}
