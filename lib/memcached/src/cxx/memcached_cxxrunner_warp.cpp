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
#include <iostream>
#include "memcached_cxxrunner.hpp"

static MemcachedCxxRunner* getMemcachedCxxRunner(VALUE self){
    MemcachedCxxRunner* p;
    Data_Get_Struct(self, MemcachedCxxRunner, p);
    return p;
}

static void wrap_MemcachedCxxRunner_free(MemcachedCxxRunner* p){
    if(p->isLegal()){
	p->~MemcachedCxxRunner();
    }
    ruby_xfree(p);
}

static VALUE wrap_MemcachedCxxRunner_alloc(VALUE klass){
    return Data_Wrap_Struct(klass, NULL, wrap_MemcachedCxxRunner_free, ruby_xmalloc(sizeof(MemcachedCxxRunner)));
}

/*********************
 * Initialize Method *
 *********************/
static VALUE wrap_MemcachedCxxRunner_init(VALUE self){
    MemcachedCxxRunner* p = getMemcachedCxxRunner(self); 
    new (p) MemcachedCxxRunner();
    return Qnil;
}

/***********
 * Connect *
 ***********/
static VALUE wrap_MemcachedCxxRunner_connect(VALUE self, VALUE _url, VALUE binaryProtocol) {
    const char* url   = StringValuePtr(_url);
    //getMemcachedCxxRunner(self)->connect(url,binaryProtocol);
    getMemcachedCxxRunner(self)->connect(url,true);
    return true;
}

/*********
 * close *
 *********/
static VALUE wrap_MemcachedCxxRunner_close(VALUE self){
    getMemcachedCxxRunner(self)->close();
    return true;
}

/********
 * keys *
 ********/
static VALUE wrap_MemcachedCxxRunner_keys(VALUE self){
    std::string keys = getMemcachedCxxRunner(self)->keys();
    //std::cout << keys << std::endl;
    return rb_str_new2(keys.c_str());    
}

/****************
 * syncExecuter *
 ****************/
static VALUE wrap_MemcachedCxxRunner_syncExecuter(VALUE self, VALUE _query, 
						  VALUE _key, VALUE _value,
						  VALUE _expire){
    const char* query  = StringValuePtr(_query);
    const char* key    = StringValuePtr(_key);
    const char* value  = StringValuePtr(_value);
    const int   expire = NUM2INT(_expire);
    if(getMemcachedCxxRunner(self)->syncExecuter(query,key,value,expire)){
	return true;
    }else{
	return false;
    }
}

/************
 * Duration *
 ************/
static VALUE wrap_MemcachedCxxRunner_getDuration(VALUE self){
    return DBL2NUM(getMemcachedCxxRunner(self)->getDuration());
}

/*********
 * Reply *
 *********/
static VALUE wrap_MemcachedCxxRunner_getReply(VALUE self){
    return rb_str_new2(getMemcachedCxxRunner(self)->getReply());
}

/****************
 * commitGetKey *
 ****************/
static VALUE wrap_MemcachedCxxRunner_commitGetKey(VALUE self, VALUE _key){
    const char* key = StringValuePtr(_key);
    if(getMemcachedCxxRunner(self)->commitGetKey(key)){
	return T_TRUE;
    }else{
	return T_FALSE;
    }
}

/****************
 * resetGetKeys *
 ****************/
static VALUE wrap_MemcachedCxxRunner_resetGetKeys(VALUE self){
    if(getMemcachedCxxRunner(self)->resetGetKeys()){
	return T_TRUE;
    }else{
	return T_FALSE;
    }
}

/********
 * mget *
 ********/
static VALUE wrap_MemcachedCxxRunner_mget(VALUE self){
    if(getMemcachedCxxRunner(self)->mget()){
	return T_TRUE;
    }else{
	return T_FALSE;
    }
}

/*************
 * mgetReply *
 *************/
static VALUE wrap_MemcachedCxxRunner_mgetReply(VALUE self, VALUE _key){
    const char* key   = StringValuePtr(_key);
    return rb_str_new2(getMemcachedCxxRunner(self)->mgetReply(key));
}

// For Require 
extern "C" void Init_memcachedCxxRunner(){
    VALUE c = rb_define_class("MemcachedCxxRunner", rb_cObject);
    rb_define_alloc_func(c, wrap_MemcachedCxxRunner_alloc); // alloc memory
    rb_define_private_method(c, "initialize", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_init), 0);
    rb_define_method(c, "syncExecuter", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_syncExecuter), 4);
    rb_define_method(c, "connect", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_connect), 2);
    rb_define_method(c, "close", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_close), 0);
    rb_define_method(c, "getDuration", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_getDuration), 0);
    rb_define_method(c, "getReply", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_getReply), 0);

    rb_define_method(c, "commitGetKey", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_commitGetKey), 1);
    rb_define_method(c, "resetGetKeys", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_resetGetKeys), 0);
    rb_define_method(c, "keys", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_keys), 0);
    rb_define_method(c, "mget", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_mget), 0);
    rb_define_method(c, "mgetReply", RUBY_METHOD_FUNC(wrap_MemcachedCxxRunner_mgetReply), 1);
    
}
