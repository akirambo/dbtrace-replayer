# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodb2MemcachedOperation"
require_relative "./mock"

module Mongodb2MemcachedOperationUnitTest 
  class Mock
    attr_reader :command, :args
    attr_accessor :queryReturn, :getValue
    include MongoDB2MemcachedOperation
    def initialize
      @logger = DummyLogger.new
      @parser = MemcachedUnitTest::ParserMock.new
      @utils  = MemcachedUnitTest::UtilsMock.new
      @query_parser = MemcachedUnitTest::QueryParserMock.new
      @query_processor = MemcachedUnitTest::QueryProcessorMock.new
      @queryReturn = false
      @getValue = nil
      @args = nil
      @option = {:datamodel => "KEYVALUE"}
    end
    ## Mock
    def add_count(a)
    end
    def add_duration(a,b,c)
    end
    def monitor(a,b)
    end
    def datamodel(model)
      @option[:datamodel] = model
    end
    def setDocs(docs)
      @utils.docs = docs
    end
    def set_nogroupkey
      @query_parser.set_nogroupkey
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
    def get(a,flag=false)
      execQuery("#{__method__}",a)
      if(@getValue.class == Hash)then
        return @getValue[a[0]]
      end
      return @getValue
    end
    def add(a)
      return execQuery("#{__method__}",a)
    end
    def incr(a)
      return execQuery("#{__method__}",a)
    end
    def decr(a)
      return execQuery("#{__method__}",a)
    end
    def append(a)
      return execQuery("#{__method__}",a)
    end
    def replace(a)
      return execQuery("#{__method__}",a)
    end
    def delete(a)
      return execQuery("#{__method__}",a)
    end
    def flush(a)
      return execQuery("#{__method__}",a)
    end
    def parse_json(d)
      return {"_id"=>"001","v"=>"00"}
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
  RSpec.describe 'Mongodb2MemcachedOperation Unit TEST' do
    before do
      @tester = Mongodb2MemcachedOperationUnitTest::Mock.new
    end
    context "Insert Operation" do
      it "MONGODB_INSERT(key value when SET is passed)" do
        @tester.queryReturn = true
        @tester.datamodel("KEYVALUE")
        args = [["k0",{"_id"=>"v0","f0"=>"v1"}]]
        expect(@tester.send(:mongodb_insert, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "MONGODB_INSERT(key value when SET is failed)" do
        @tester.queryReturn = false
        @tester.datamodel("KEYVALUE")
        args = [["k0",{"_id"=>"v0","f0"=>"v1"}]]
        expect(@tester.send(:mongodb_insert, args)).to be false
        expect(@tester.command).to eq "set"
      end
      it "MONGODB_INSERT(key value when SET is passed)" do
        @tester.queryReturn = true
        @tester.datamodel("KEYVALUE")
        args = [["k0",[{"_id"=>"v0","f0"=>"v1"}]]]
        expect(@tester.send(:mongodb_insert, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "MONGODB_INSERT(key value when SET is failed)" do
        @tester.queryReturn = false
        @tester.datamodel("KEYVALUE")
        args = [["k0",[{"_id"=>"v0","f0"=>"v1"}]]]
        expect(@tester.send(:mongodb_insert, args)).to be false
        expect(@tester.command).to eq "set"
      end
      it "MONGODB_INSERT(document)" do
        @tester.datamodel("DOCUMENT")
        docs = [{:_id => "ObjectId('000')"}]
        @tester.setDocs(docs)
        @tester.getValue =  "{\"_id\":\"001\"}"
        ## true case
        @tester.queryReturn = true
        args = [["key",{"_id"=>"ObjectId('000')"}]]
        expect(@tester.send(:mongodb_insert, args)).to be true
        expect(@tester.command).to eq "set"
        ans =  ["[{\"_id\":\"000\"},{\"_id\":\"001\"}]", "key"]
        expect(@tester.args).to match_array ans
        
        ## false case
        @tester.queryReturn = false
        expect(@tester.send(:mongodb_insert, args)).to be false
      end
      it "MONGODB_INSERT(error datamodel)" do
        @tester.queryReturn = false
        @tester.datamodel("datamodel")
        args = []
        expect(@tester.send(:mongodb_insert, args)).to be false
      end
      it "MONGODB_UPSERT(key value when SET is passed)" do
        @tester.queryReturn = true
        @tester.datamodel("KEYVALUE")
        args = [["k0",{"_id"=>"v0","f0"=>"v1"}]]
        expect(@tester.send(:mongodb_upsert, args)).to be true
        expect(@tester.command).to eq "set"
      end
    end
    context "Update Operation" do
      it "MONGODB_UPDATE (key value)" do
        @tester.datamodel("KEYVALUE")
        @tester.queryReturn = true
        args = {
          "key"    => "k00",
          "query"  => {"_id"  => "id00"},
          "update" => {"$set" => {"v0"=>"v","v1"=>"v"}}
        }
        expect(@tester.send(:mongodb_update, args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "MONGODB_UPDATE (document [multi:true])" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue =  "[{\"_id\":\"001\"},{\"_id\":\"002\"}]"
        @tester.queryReturn = true
        args = {
          "key"    => "k00",
          "query"  => {"_id"  => "id00"},
          "update" => {"$set" => {"v0"=>"v","v1"=>"v"}},
          "multi"  => true
        }
        expect(@tester.send(:mongodb_update, args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "MONGODB_UPDATE (document [multi:false])" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue =  "[{\"_id\":\"001\"},{\"_id\":\"002\"}]"
        @tester.queryReturn = true
        args = {
          "key"    => "k00",
          "query"  => {"_id"  => "id00"},
          "update" => {"$set" => {"v0"=>"v","v1"=>"v"}},
          "multi"  => false
        }
        expect(@tester.send(:mongodb_update, args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "MONGODB_UPDATE (document [multi:false])" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue =  "{\"_id\":\"001\"}"
        @tester.queryReturn = true
        args = {
          "key"    => "k00",
          "query"  => {"_id"  => "id00"},
          "update" => {},
          "multi"  => false
        }
        expect(@tester.send(:mongodb_update, args)).to be false
      end
      it "MONGODB_UPDATE (error datamodel)" do
        @tester.queryReturn = false
        @tester.datamodel("datamodel")
        @tester.getValue = "[{\"_id\":\"001\"}]"
        expect(@tester.send(:mongodb_update, {})).to be false
      end
    end
    context "Find Operation" do
      it "MONGODB_FIND (key value)" do
        @tester.datamodel("KEYVALUE")
        args = {"key" => "k00", "filter" => {"_id" => "001"}}
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        ans = {:_id => "001",:v => 2}
        expect(@tester.send(:mongodb_find, args)).to include ans
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_FIND (document with filter)" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id => "001", :v => 2}])
        args = {"key" => "k00", "filter" => {"_id" => "001"}}
        ans  = [{:_id => "001",:v => 2}]
        expect(@tester.send(:mongodb_find, args)).to include ans
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_FIND (document with filter)" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue = "[[{\"_id\":\"001\",\"v\":2}]]"
        @tester.setDocs([{:_id => "001", :v => 2}])
        args = {"key" => "k00", "filter" => {"_id" => "001"}}
        ans  = [{:_id => "001",:v => 2}]
        expect(@tester.send(:mongodb_find, args)).to include ans
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_FIND (document w/o filter)" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id => "001", :v => 2}])
        args = {"key" => "k00", "filter" => {}}
        ans  = [{:_id => "001",:v => 2}]
        expect(@tester.send(:mongodb_find, args)).to include ans
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_FIND (error datamodel))" do
        @tester.datamodel("error")
        expect(@tester.send(:mongodb_find,{})).to eq []
      end
    end
    context "Delete Operation" do
      it "MONGODB_DELETE (key value) " do
        @tester.datamodel("KEYVALUE")
        @tester.queryReturn = true
        args = {"key"=>"k","filter"=>{"_id"=>"001"}}
        expect(@tester.send(:mongodb_delete,args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "MONGODB_DELETE (document w/o filter) " do
        @tester.datamodel("DOCUMENT")
        @tester.queryReturn = true
        args = {"filter" => []}
        expect(@tester.send(:mongodb_delete,args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "MONGODB_DELETE (document w/ filter) " do
        @tester.datamodel("DOCUMENT")
        @tester.queryReturn = true
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id => "001", :v => 2}])
        args = {"key" => "k", "filter" => {"_id"=>"001"}}
        expect(@tester.send(:mongodb_delete,args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "MONGODB_DELETE (document w/ filter) " do
        @tester.datamodel("DOCUMENT")
        @tester.queryReturn = true
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id => "001", :v => 2}])
        args = {"key" => "k", "filter" => {"_id"=>"002"}}
        expect(@tester.send(:mongodb_delete,args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "MONGODB_DELETE (error datamodel)" do
        @tester.datamodel("error")
        expect(@tester.send(:mongodb_delete,{})).to be false
      end
    end
    context "Count Operation" do
      it "MONGODB_COUNT (keyvalue)" do
        @tester.datamodel("KEYVALUE")
        @tester.getValue = "a"
        args = {"key" => "k", "query" => {"_id"=>"002"}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_COUNT (document)" do
        @tester.datamodel("DOCUMENT")
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2},{\"_id\":\"002\",\"v\":1}]"
        args = {"key" => "k", "query" => {"v"=>2}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        expect(@tester.command).to eq "get"
      end
      it "MONGODB_COUNT (error)" do
        @tester.datamodel("error")
        expect(@tester.send(:mongodb_count,{})).to eq 0
      end
    end
    context "Aggregate Operation" do 
      it "MONGODB_AGGREGATE (cond.class == Hash)" do
        args = {"key"=>"k"}
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id =>"001",:v =>2 ,:x => 3},{:_id => "002", :v => 1, :x => 4}])
        @tester.setCond({"cond"=>{"v" => "sum"}})
        ans = {"groupKey"=>{"v"=>10}}
        expect(@tester.send(:mongodb_aggregate,args)).to include ans 
      end
      it "MONGODB_AGGREGATE (keys == "")" do
        args = {"key"=>"k"}
        @tester.set_nogroupkey
        @tester.getValue = "[{\"_id\":\"001\",\"v\":2}]"
        @tester.setDocs([{:_id =>"001",:v =>2 ,:x => 3},{:_id => "002", :v => 1, :x => 4}])
        @tester.setCond({"cond"=>{"v" => "sum"}})
        ans = {"groupKey"=>{"v"=>10}}
        expect(@tester.send(:mongodb_aggregate,args)).to include ans 
      end
    end
    context "Replace Operation" do
      it "MONGODB_REPLACE #1 " do
        args = {"update" => {"$set" => {"v" =>"aa"}}}
        ans  = "{\"_id\":\"001\",\"v\":\"00\"}"
        expect(@tester.send(:mongodb_replace,"dummy",args)).to include ans 
      end
      it "MONGODB_REPLACE #2 " do
        args = {"update" => {"v" => "aa"}}
        ans  = "{\"_id\":\"001\",\"v\":\"00\"}"
        expect(@tester.send(:mongodb_replace,"dummy",args)).to include ans 
      end
    end
    context "Unsupported Operation" do 
      it "MONGODB_GROUP" do
        expect(@tester.send(:mongodb_group,"dummy")).to eq true
      end
      it "MONGODB_MAPREDUCE" do
        expect(@tester.send(:mongodb_mapreduce,"dummy")).to eq true
      end
    end
    context "Private Method" do
      it "prepare_mongodb" do
        ans = {"operand" => "mongodb_test", "args" => "OK"}
        expect(@tester.send(:prepare_mongodb,"TEST","dummy")).to include ans
      end
      it "mongodb_process_keyvalue (error)" do
        expect(@tester.send(:mongodb_process_keyvalue,nil,"notype")).to be false
      end
      it "mongodb_process_document (error)" do
        expect(@tester.send(:mongodb_process_document,nil,"notype")).to be false
      end
      it "mongodb_replace_doc (error)" do
        @tester.getValue = '[{"a":"n"}]'
        args = {"query" => nil}
        newvals = {:a => "v", :b => "v"}
        expect(@tester.send(:mongodb_replace_doc,args,newvals)).to include newvals
      end
      it "mongodbQuery (return TRUE)" do
        doc = {:_id => "001" , :value => "test"}
        query = {"value"=>"test"}
        expect(@tester.send(:mongodb_query,doc,query,"test")).to be true
      end
      it "mongodb_query (return FALSE #1)" do
        doc = {:_id => "001" , :value => "test1"}
        query = {"value"=>"test"}
        expect(@tester.send(:mongodb_query,doc,query,"test")).to be false
      end
      it "mongodb_query (return FALSE #2)" do
        doc = {:_id => "001" , :value => "test"}
        query = {"value"=>{"test"=>"b"}}
        @tester.setQueryReturnValue(false)
        expect(@tester.send(:mongodb_query,doc,query,"test")).to be false
      end
      it "mongodb_query (return FALSE #3)" do
        doc = {:_id => "001" , :value => "test"}
        query = {"test"=>"test"}
        expect(@tester.send(:mongodb_query,doc,query,"test")).to be false
      end
      it "documentSymbolize" do
        docs = [{"_id" => "001", "value" => "test"},{"_id" => "002" , "value" => "test"}]
        ans = [{:_id => "001", :value => "test"},{:_id => "002", :value => "test"}]
        expect([@tester.send(:document_symbolize,docs)]).to include ans
        end
    end
  end
end

