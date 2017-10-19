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
    def set(args)
      @command = "#{__method__}"
      @key   = args[0]
      @value = args[1]
      return "OK"
    end 
    def hmset(args)
      args["key"] 
      @command = "#{__method__}"
      @key = args["key"]
      @value = args["args"]
      return "OK"
    end
    def get(args)
      @command = "#{__method__}"
      @key = args[0]
      return "data"
    end
    def hmget(args,flag)
      @command = "#{__method__}"
      @key   = args["key"]
      @value = args["fields"]
      return "data"
    end
    def del(args)
      @command = "#{__method__}"
      @key = args[0]
      return "OK"
    end
    def hdel(args)
      @command = "#{__method__}"
      @key = args[0]
      @value = args[1]
      return "OK"
    end
    def keys(a,b)
      return ["target_keys"]
    end
    def parse_json(doc)
      @utils.parse_json(doc)
    end
    def convert_json(doc)
      @utils.convert_json(doc)
    end
  end
  
  RSpec.describe 'Cassandra TO Redis Test' do
    before do
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::FATAL
      @tester = Mock.new(@logger)
    end
    context 'Operation' do
      it "cassandra_insert (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "schema_fields" => 2,
          "args"=>{"pkey"=>"p1","c1"=>"v1"}}
        expect(@tester.send(:cassandra_insert,args)).to eq "OK"
        expect(@tester.command).to eq "set"
        expect(@tester.key).to eq "t--p1"
        expect(@tester.value).to eq "v1"
      end
      it "cassandra_insert (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "schema_fields" => 3,
          "args"=>{"pkey"=>"p1","c1"=>"v1","c2"=>"v2"}}
        expect(@tester.send(:cassandra_insert,args)).to eq "OK"
        expect(@tester.command).to eq "hmset"
        expect(@tester.key).to eq "t"
        ans = ["pkey","p1","c1","v1","c2","v2"]
        expect(@tester.value).to match ans
      end
      it "cassandra_select (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 2}
        expect(@tester.send(:cassandra_select,args)).to eq ["data"]
        expect(@tester.command).to eq "get"
        expect(@tester.key).to eq "t--p1"
      end
      it "cassandra_select (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 3,
          "fields" => ["a,b"]
        }
        expect(@tester.send(:cassandra_select,args)).to eq "data"
        expect(@tester.command).to eq "hmget"
        expect(@tester.key).to eq "t--p1"
      end
      it "cassandra_select (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 3,
          "fields" => ["a,b"]
        }
        expect(@tester.send(:cassandra_select,args)).to eq "data"
        expect(@tester.command).to eq "hmget"
        expect(@tester.key).to eq "t"
      end
      it "cassandra_update (key value)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 2,
          "set"=>{"pkey"=>"p1","c1"=>"v1"}}
        expect(@tester.send(:cassandra_update,args)).to eq "OK"
        expect(@tester.command).to eq "set"
        expect(@tester.key).to eq "t--p1"
      end
      it "cassandra_update (table)" do
        args = {"table"=>"t",
          "primaryKey"=>"pkey",
          "cond_keys"=>["c1","pkey"],
          "cond_values"=>["v1","p1"],
          "schema_fields" => 3,
          "set"=>{"pkey"=>"p1","c1"=>"v1","c2"=>"v2"}}
        expect(@tester.send(:cassandra_update,args)).to eq "OK"
        expect(@tester.command).to eq "hmset"
        expect(@tester.key).to eq "t"
        ans = ["pkey","p1","c1","v1","c2","v2"]
        expect(@tester.value).to match ans
      end
      it "cassandra_delete (key value)" do
        args = {"table"=>"t",
          "fields"=>"*",
          "schema_fields" => 2}
        expect(@tester.send(:cassandra_delete,args)).to eq "OK"
        expect(@tester.command).to eq "del"
        expect(@tester.key).to eq "t"
      end
      it "cassandra_delete (table)" do
        args = {"table"=>"t",
          "fields"=>"*",
          "schema_fields" => 3}
        expect(@tester.send(:cassandra_delete,args)).to eq "OK"
        expect(@tester.command).to eq "hdel"
        expect(@tester.key).to eq "t"
      end
      it "cassandra_drop" do
        args = {"key"=>"t","type"=>"a"}
        expect(@tester.send(:cassandra_drop,args)).to eq "OK"
        expect(@tester.command).to eq "del"
        expect(@tester.key).to eq "target_keys"
      end
      it "prepare_cassandra" do
        args = {"key"=>"t","type"=>"a"}
        ans =  {"operand"=>"cassandra_drop", "args"=>"PARSED"}
        expect(@tester.send("prepare_cassandra", "drop", args)).to eq ans
      end
    end
  end
end
