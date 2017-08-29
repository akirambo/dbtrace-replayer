# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandra2RedisOperation"
require_relative "../../common/utils"

module CassandraTest
  class ParserMock
    def exec(a,b)
      return "PARSED"
    end
  end
  class Mock
    attr_reader :command, :key, :value
    include Cassandra2RedisOperation
    def initialize(logger)
      @logger = logger
      @command = ""
      @parser = ParserMock.new()
      @getValue = nil
      @key = nil
      @value = nil
      @setValue = ""
    end
    def SET(args)
      @command = "#{__method__}"
      @key   = args[0]
      @value = args[1]
      return "OK"
    end 
    def HMSET(args)
      args["key"] 
      @command = "#{__method__}"
      @key = args["key"]
      @value = args["args"]
      return "OK"
    end
    def GET(args)
      @command = "#{__method__}"
      @key = args[0]
      return "data"
    end
    def HMGET(args,flag)
      @command = "#{__method__}"
      @key   = args["key"]
      @value = args["fields"]
      return "data"
    end
    def DEL(args)
      @command = "#{__method__}"
      @key = args[0]
      return "OK"
    end
    def HDEL(args)
      @command = "#{__method__}"
      @key = args[0]
      @value = args[1]
      return "OK"
    end
    def KEYS(a,b)
      return ["TARGET_KEYS"]
    end
    def parseJSON(doc)
      @utils.parseJSON(doc)
    end
    def convJSON(doc)
      @utils.convJSON(doc)
    end
  end
  
  RSpec.describe 'Cassandra TO Redis Test' do
    before do
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::FATAL
      @tester = Mock.new(@logger)
    end
    context 'Operation' do
      it "CASSANDRA_INSERT (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "schema_fields" => 2,
          "args"=>{"pkey"=>"p1","c1"=>"v1"}}
        expect(@tester.send(:CASSANDRA_INSERT,args)).to eq "OK"
        expect(@tester.command).to eq "SET"
        expect(@tester.key).to eq "t--p1"
        expect(@tester.value).to eq "v1"
      end
      it "CASSANDRA_INSERT (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "schema_fields" => 3,
          "args"=>{"pkey"=>"p1","c1"=>"v1","c2"=>"v2"}}
        expect(@tester.send(:CASSANDRA_INSERT,args)).to eq "OK"
        expect(@tester.command).to eq "HMSET"
        expect(@tester.key).to eq "t--p1"
        ans = {"c1"=>"v1","c2"=>"v2"}
        expect(@tester.value).to include ans
      end
      it "CASSANDRA_SELECT (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 2}
        expect(@tester.send(:CASSANDRA_SELECT,args)).to eq ["data"]
        expect(@tester.command).to eq "GET"
        expect(@tester.key).to eq "t--p1"
      end
      it "CASSANDRA_SELECT (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 3}
        expect(@tester.send(:CASSANDRA_SELECT,args)).to eq "data"
        expect(@tester.command).to eq "HMGET"
        expect(@tester.key).to eq "t--p1"

      end
      it "CASSANDRA_UPDATE (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 2,
          "set"=>{"pkey"=>"p1","c1"=>"v1"}}
        expect(@tester.send(:CASSANDRA_UPDATE,args)).to eq "OK"
        expect(@tester.command).to eq "SET"
        expect(@tester.key).to eq "t--p1"
      end
      it "CASSANDRA_UPDATE (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 3,
          "set"=>{"pkey"=>"p1","c1"=>"v1","c2"=>"v2"}}
        expect(@tester.send(:CASSANDRA_UPDATE,args)).to eq "OK"
        expect(@tester.command).to eq "HMSET"
        expect(@tester.key).to eq "t--p1"
        ans = {"c1"=>"v1","c2"=>"v2"}
        expect(@tester.value).to include ans
      end
      it "CASSANDRA_DELETE (key value)" do
        args = {"table"=>"t",
          "fields"=>"*",
          "schema_fields" => 2}
        expect(@tester.send(:CASSANDRA_DELETE,args)).to eq "OK"
        expect(@tester.command).to eq "DEL"
        expect(@tester.key).to eq "t"
      end
      it "CASSANDRA_DELETE (table)" do
        args = {"table"=>"t",
          "fields"=>"*",
          "schema_fields" => 3}
        expect(@tester.send(:CASSANDRA_DELETE,args)).to eq "OK"
        expect(@tester.command).to eq "HDEL"
        expect(@tester.key).to eq "t"
      end
      it "CASSANDRA_DROP" do
        args = {"key"=>"t","type"=>"a"}
        expect(@tester.send(:CASSANDRA_DROP,args)).to eq "OK"
        expect(@tester.command).to eq "DEL"
        expect(@tester.key).to eq "TARGET_KEYS"
      end
    end
    context "Private" do
      it "prepare_CASSANDRA" do
        ans = {"operand"=>"CASSANDRA_TEST", "args"=>"PARSED"}
        expect(@tester.send(:prepare_CASSANDRA,"test",{})).to eq ans
      end
      it "cassandraQuery (true)" do
        result = {"c1"=>"v1", "c2"=>"v2"}
        args  = {"where" =>["c1=v1"]}
        expect(@tester.send(:cassandraQuery,result,args)).to be true
      end
      it "cassandraQuery (false)" do
        result = {"c1"=>"v1", "c2"=>"v2"}
        args  = {"where" =>["c1=mm"]}
        expect(@tester.send(:cassandraQuery,result,args)).to be false
      end
      it "selectField (*)" do
        hash = {"c1"=>"v1", "c2"=>"v2"}
        args = {"fields" =>["*"]}
        ans  = {"c1"=>"v1", "c2"=>"v2"}
        expect(@tester.send(:selectField,hash,args)).to include ans
      end
      it "selectField (c1)" do
        hash = {"c1"=>"v1", "c2"=>"v2"}
        args = {"fields" =>["c1"]}
        ans  = {"c1"=>"v1"}
        expect(@tester.send(:selectField,hash,args)).to include ans
      end
    end
    context "Java API" do
      it "CASSANDRA_BATCH_MUTATE(counterColumn)" do
        skip("Unimplemented Test")
      end
      it "CASSANDRA_BATCH_MUTATE(NO counterColumn)" do
        skip("Unimplemented Test")
      end
      it "CASSANDRA_GET_SLICE" do
        skip("Unimplemented Test")
      end
      it "CASSANDRA_GET_RANGE_SLICES" do
        skip("Unimplemented Test")
      end
      it "CASSANDRA_MULTIGET_SLICE" do
        skip("Unimplemented Test")
      end
      it "cassandraSerialize" do
        skip("Unimplemented Test")
      end
      it "cassandraDeserialze" do
        skip("Unimplemented Test")
      end
    end
  end
end
