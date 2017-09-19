# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandra2MongodbOperation"

module Cassandra2MongodbTester
  class ParseMock
    def initialize()
    end
    def exec(op,args)
      return args
    end
  end
  class Mock
    attr_reader :command, :value
    include Cassandra2MongodbOperation
    def initialize
      @logger = DummyLogger.new
      @parser = ParseMock.new
      @command = nil
      @value   = nil
      @queryReturn = nil
      @findReturn = nil
    end
    def setQueryValue(v)
      @queryReturn = v
    end
    def setFindValue(v)
      @findReturn = v
    end
    def insert(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def update(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def delete(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def drop(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def find(a)
      @value = a
      @command = "#{__method__}"
      return @findReturn
    end
    def convert_json(a)
      return  "{f1:\"v1\",f2:\"v2\"}"
    end
    def parse_json(row)
      return {"f1"=>"v1","f2"=>"v2"}
    end
  end

  RSpec.describe 'Cassandra2Mongodb  Unit TEST' do
    before do
      @tester = Mock.new
    end
    context "Cassandra To Mongodb Operation" do
      it "CASSANDRA_INSERT" do
        @tester.setQueryValue(true)
        arg = {"table"=>"t1", "args"=>{"pkey"=>"aaa"}, "primaryKey"=>"pkey"}
        expect(@tester.send(:cassandra_insert,arg)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "CASSANDRA_SELECT" do
        @tester.setQueryValue(true)
        @tester.setFindValue([{"_id"=>"id00","f1"=>"v1","f2"=>"v2"}])
        arg = {"table"=>"t1",
          "args"=>{"pkey"=>"aaa","fields"=>"*"},
          "fields" => ["f1","f2"],
          "primaryKey"=>"pkey",
          "cond_keys"=>["pkey"],
          "cond_values"=>["v"]}
        expect(@tester.send(:cassandra_select,arg)).to match_array ["v1","v2"]
        expect(@tester.command).to eq "find"
      end
      it "CASSANDRA_UPDATE" do
        @tester.setQueryValue(true)
        arg = {"table"=>"t1",
          "args"=>{"pkey"=>"aaa","fields"=>"*"},
          "fields" => ["f1","f2"],
          "primaryKey"=>"pkey",
          "cond_keys"=>["pkey"],
          "cond_values"=>["v"]}
        expect(@tester.send(:cassandra_update,arg)).to eq true
        expect(@tester.command).to eq "update"
      end
      it "CASSANDRA_DELETE" do
        @tester.setQueryValue(true)
        arg = {"table"=>"t1",
          "args"=>{"pkey"=>"aaa","fields"=>"*"},
          "fields" => ["f1","f2"],
          "primaryKey"=>"pkey",
          "cond_keys"=>["pkey"],
          "cond_values"=>["v"]}
        expect(@tester.send(:cassandra_delete,arg)).to eq true
        expect(@tester.command).to eq "update"
      end
      it "CASSANDRA_DROP" do
        @tester.setQueryValue(true)
        expect(@tester.send(:cassandra_drop,[])).to eq true
        expect(@tester.command).to eq "drop"
      end
    end
    context "Private Method" do
      it "prepare_cassandra" do
        ans = {"operand"=>"cassandra_test","args"=>"test_args"}
        expect(@tester.send(:prepare_cassandra,"test","test_args")).to include ans
      end
      it "CASSANDRA_JUDGE" do
        result = {"f1"=>"v1"} 
        arg    = {"where"=>["f1=v1"]}
        expect(@tester.send(:cassandra_judge,result,arg)).to be true
        result = {"f1"=>"v2"} 
        arg    = {"where"=>["f1=v1"]}
        expect(@tester.send(:cassandra_judge,result,arg)).to be false
      end
      it "selected_field" do
        ans  = {"f1"=>"v1","f2"=>"v2"}
        args = {"fields"=>["*"]}
        expect(@tester.send(:select_field,ans,args)).to include ans
        ans  = {"f1"=>"v1","f2"=>"v2"}
        args = {"fields"=>["f1,f2"]}
        expect(@tester.send(:select_field,ans,args)).to include ans
      end
      it "cassandraSerialize" do
        dummy = {}
        expect(@tester.send(:cassandra_serialize,dummy)).to eq "{f1:\"v1\",f2:\"v2\"}"
      end
      it "cassandraDeserialize" do
        dummy = ["d1"]
        expect(@tester.send(:cassandra_deserialize,dummy)).to eq [{"f1"=>"v1","f2"=>"v2"}]
      end
    end
  end
end
