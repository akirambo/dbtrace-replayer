# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandra2MemcachedOperation"
require_relative "./mock"

module Cassandra2MemcachedOperationUnitTest
  class Mock
    attr_reader :command, :args
    attr_accessor :queryReturn, :getValue
    include Cassandra2MemcachedOperation
    def initialize
      @parser = MemcachedUnitTest::ParserMock.new
      @logger = DummyLogger.new
      @queryReturn = false
      @getValue = nil
      @args = nil
      @option = {:datamodel => "KEYVALUE"}
    end
    ## Mock
    def monitor(a,b)
    end
    def datamodel(model)
      @option[:datamodel] = model
    end
    def setDocs(docs)
      @utils.docs = docs
    end
    def setCond(cond)
      @query_parser.cond = cond
    end
    def setQueryReturnValue(bool)
      @query_processor.returnValue = bool
    end
    def set(a)
      return execQuery("#{__method__}",a)
    end
    def get(a)
      execQuery("#{__method__}",a)
      if(@getValue.class == Hash)then
        return @getValue[a[0]]
      end
      return @getValue
    end
    def delete(a)
      return execQuery("#{__method__}",a)
    end
    def KEYLIST
      return ["t.a","t.t","a.t"]
    end
    def flush(a)
      return execQuery("#{__method__}",a)
    end
    def parse_json(d)
      return {"pkey"=>"p0","f0"=>"v0","f1"=>"v1"}
    end
    def convert_json(d)
      return "{\"_id\":\"001\",\"v\":\"00\"}"
    end
    def execQuery(operand,args)
      @args = args
      @command = operand
      return @queryReturn 
    end
  end

  RSpec.describe 'Cassandra TO Memcached Unit Test' do
    before do
      @tester = Cassandra2MemcachedOperationUnitTest::Mock.new
    end
    context 'Operation' do
      it "cassandra_insert (schema_fields == 2)" do
        @tester.queryReturn = true
        args = {
          "table"         => "t1",
          "schema_fields" => 2,
          "primaryKey"    => "pkey",
          "args"          => {"pkey" => "p0", "f0" => "v0"}
        }
        expect(@tester.send(:cassandra_insert,args)).to be true
      end
      it "cassandra_insert (schema_fields > 2)" do
        @tester.queryReturn = true
        args = {
          "table"         => "t1",
          "schema_fields" => 3,
          "primaryKey"    => "pkey",
          "args"          => {"pkey" => "p0", "f0" => "v0", "f1" => "v1"}
        }
        expect(@tester.send(:cassandra_insert,args)).to be true
      end
      it "cassandra_select (schema_fields == 2)" do
        @tester.getValue = "v0"
        args = {
          "table"         => "t1",
          "schema_fields" => 2,
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey"
        }
        expect(@tester.send(:cassandra_select,args)).to eq ["v0"]
      end
      it "cassandra_select (schema_fields > 2)" do
        @tester.getValue = "dummy"
        args = {
          "table"         => "t1",
          "schema_fields" => 3,
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey",
          "fields"        => ["f0","f1"]
        }
        expect(@tester.send(:cassandra_select,args)).to match_array ["v0","v1"]
      end
      it "cassandra_update (schema_fields == 2)" do
        @tester.queryReturn = true
        args = {
          "table"         => "t1",
          "schema_fields" => 2,
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey",
          "set"          =>  {"f0" => "n0"}
        }
        expect(@tester.send(:cassandra_update,args)).to be true
      end
      it "cassandar_update (schema_fields > 2)" do
        @tester.queryReturn = true
        @tester.getValue = "dummy"
        args = {
          "table"         => "t1",
          "schema_fields" => 3,
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey",
          "set"          => {"f0" => "n0", "f1" => "n1","f2"=> "n2"}
        }
        expect(@tester.send(:cassandra_update,args)).to be true
      end
      it "cassandra_update (schema_fields > 2)" do
        @tester.queryReturn = true
        @tester.getValue = "dummy"
        args = {
          "table"         => "t1",
          "schema_fields" => 3,
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey",
          "set"          => {"f0" => "n0", "f1" => "n1"}
        }
        expect(@tester.send(:cassandra_update,args)).to be true
      end
      it "cassandra_delete" do
        @tester.queryReturn = true
        args = {
          "cond_keys"     => ["pkey","f0"],
          "cond_values"   => ["p0","v0"],
          "primaryKey"    => "pkey"
        }
        expect(@tester.send(:cassandra_delete,args)).to be true
      end
      it "cassandra_drop" do
        @tester.queryReturn = true
        args = {"type" => "table","key" => "t"}
        expect(@tester.send(:cassandra_drop,args)).to be true
        args = {"type" => "keyspace", "key"=>"t"}
        expect(@tester.send(:cassandra_drop,args)).to be true
      end
    end
    context "Private Method" do
      it "prepare_cassandra" do
        ans = {"operand" => "cassandra_test", "args" => "OK"}
        expect(@tester.send(:prepare_cassandra,"test","OK")).to include ans
      end
      it "CASSANDRA_JUDGE" do
        doc  = {"f0"=>"v0","f1"=>"v1"}
        args = {"where" => ["f0=v0","f1=v1"]}
        expect(@tester.send(:cassandra_judge,doc,args)).to be true
        doc  = {"f0"=>"v1","f1"=>"v0"}
        args = {"where" => ["f0=v0","f1=v1"]}
        expect(@tester.send(:cassandra_judge,doc,args)).to be false
      end
      it "selectField * " do
        hash  = {"f0"=>"v0","f1"=>"v1"}
        args = {"fields" => ["*"]}
        ans  = {"f0"=>"v0","f1"=>"v1"}
        expect(@tester.send(:selectField,hash,args)).to include ans
      end
      it "selectField specific" do
        hash  = {"f0"=>"v0","f1"=>"v1"}
        args = {"fields" => ["f0,f1"]}
        ans  = {"f0"=>"v0","f1"=>"v1"}
        expect(@tester.send(:selectField,hash,args)).to include ans
      end
      it "cassandraSerialize" do
        arg = ["a","b"]
        ans = "a__A__b"
        expect(@tester.send(:cassandraSerialize,arg)).to eq ans
      end
      it "cassandraDeseriealize" do
        arg = "a__A__b"
        ans = ["a","b"]
        expect(@tester.send(:cassandraDeserialize,arg)).to match_array ans
      end
    end
  end
end


