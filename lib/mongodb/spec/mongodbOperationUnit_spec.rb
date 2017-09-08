# -*- coding: utf-8 -*-


require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbOperation"

module MongodbOperationTester
  class ClientMock
    def initialize()
      @queryResult = true
      @querySuccess = true
    end
    def setQuerySuccess(flag)
      @querySuccess = flag
    end
    def setResult(result)
      @queryResult = result
    end
    def setDatabaseName(a)
    end
    def setCollectionName(a)
    end
    def syncExecuter(a,b)
      return @queryResult
    end
    def update(a,b,c)
      return @queryResult
    end
    def find(a)
      return @querySuccess
    end
    def deleteExecuter(a,b)
      return @queryResult
    end
    def count(a)
      return @queryResult
    end
    def setAggregateCommand(a,b)
    end
    def drop
      return true
    end
    def clearCollectionName()
    end
    def aggregate
      return @querySuccess
    end
    def getReply
      return @queryResult
    end
    def commitDocument(a)
      return @queryResult
    end
    def getDuration()
      return 0.1
    end
  end
  class UtilsMock
    def add_doublequotation(hash)
      str = hash.to_json
      str.gsub!(/\"/,"")
      str.gsub!("{","{\"")
      str.gsub!(":","\":\"")
      str.gsub!("http\":\"","http:")
      str.gsub!("https\":\"","https:")
      str.gsub!(",","\",\"")
      str.gsub!("}","\"}")
      str.gsub!(/\"(\d+)\"/,'\1')
      str.gsub!("\"{","{")
      str.gsub!("}\"","}")
      str.gsub!(":\"[",":[\"")
      str.gsub!("]\"","\"]")
      str.gsub!("[\"{","\[{")
      str.gsub!("}\"]","}]")
      return str
    end
  end
  class ParserMock
    def exec(a,b,c)
      return "ARGS"
    end
  end
  class Mock
    include MongodbOperation
    def initialize(option)
      @option = option
      @logger = DummyLogger.new
      @client = ClientMock.new
      @parser = ParserMock.new
      @utils  = UtilsMock.new
      @metrics = true
    end
    def setResult(result)
      @client.setResult(result)
    end
    def setQuerySuccess(flag)
      @client.setQuerySuccess(flag)
    end
    def connect
    end
    def close
    end
    def addDuration(a,b,c)
    end
    def addCount(a)
    end
    def async
      @option[:async] = true
    end
    def sync
      @option[:async] = false
    end
  end
  RSpec.describe 'MongodbOperation Unit TEST' do
    before do
      option = {
        :async => false
      }
      @tester = Mock.new(option)
    end
    context "Mongodb Operation" do
      it "INSERT (empty) sync" do
        @tester.sync
        @tester.setResult(true)
        expect(@tester.send("INSERT", [])).to eq true
      end
      it "INSERT (empty) async" do
        @tester.async
        @tester.setResult(true)
        expect(@tester.send("INSERT", [])).to eq true
      end
      it "INSERT (single) sync" do
        doc = {"val" => 'a', "_id" => 'key00'}
        @tester.sync
        @tester.setResult(true)
        expect(@tester.send("INSERT", [["test",doc]])).to eq true
      end
      it "INSERT (single) async" do
        doc = {"val" => 'a', "_id" => 'key00'}
        @tester.async
        @tester.setResult(true)
        expect(@tester.send("INSERT", [["test",doc]])).to eq true
      end
      it "INSERT (multi) sync" do
        doc00 = {:val => 'a', :_id => 'key00'}
        doc01 = {:val => 'a', :_id => 'key01'}
        @tester.sync
        @tester.setResult(true)
        expect(@tester.send("INSERT", [["test",doc00],["test",doc01]])).to eq true
      end
      it "INSERT (multi) async" do
        doc00 = {:val => 'a', :_id => 'key00'}
        doc01 = {:val => 'a', :_id => 'key01'}
        @tester.async
        @tester.setResult(true)
        expect(@tester.send("INSERT", [["test",doc00],["test",doc01]])).to eq true
      end
      it "INSERT (single) false" do
        @tester.async
        @tester.setResult(false)
        doc = {"val" => 'a', "_id" => 'key00'}
        expect(@tester.send("INSERT", [["test",doc]])).to eq false
      end
      it "INSERT (multi) false" do
        doc00 = {:val => 'a', :_id => 'key00'}
        doc01 = {:val => 'a', :_id => 'key01'}
        @tester.async
        @tester.setResult(false)
        expect(@tester.send("INSERT", [["test",doc00],["test",doc01]])).to eq false
      end
      it "UPDATE (single/multi)" do
        @tester.setResult(true)
        ## Exec
        cond = {
          "key"    => "test",
          "multi"  => false,
          "query"  => {"val" =>  'a0'},
          "update" => {'$set' => {"val" => 'b'}}
        }
        expect(@tester.send("UPDATE", cond)).to eq true
      end
      it "FIND" do
        # Setup
        @tester.setQuerySuccess(true)
        result = []
        result.push("{\"_id\":\"key01\",\"val\":\"a0\"}")
        @tester.setResult(result.join("\n"))
        
        cond = {
          "key"  => "test",
          "sort" => nil,
          "projection" => nil,
          "filter" => {"_id" => "key01"}
        }
        expect(@tester.send("FIND", cond)).to eq [{"_id"=>"key01", "val"=>"a0"}]
      end
      it "DELETE" do
        ## Setup
        @tester.setResult(true)
        cond = {
          "key"  => "test",
          "filter" => {"val" => 'a0'}
        }
        expect(@tester.send("DELETE", cond)).to eq true
      end
      it "COUNT" do
        ## Setup
        @tester.setResult(1)
        cond = {
          "key"  => "a.b",
          "query" => {"val" => 'a0'}
        }
        expect(@tester.send("COUNT", cond)).to eq 1
      end
      it "AGGREGATE" do
        ## Setup
        @tester.setQuerySuccess(true)
        @tester.setResult("test")
        cond = {
          "key"  => "a.b",
          "match"=> {"val" => 'a0'}
        }
        expect(@tester.send("AGGREGATE", cond)).to eq "test"
        @tester.setQuerySuccess(false)
        expect(@tester.send("AGGREGATE", cond)).to eq ""
      end
      it "DROP" do
        cond = ["a"]
        expect(@tester.send("DROP", cond)).to be true
        cond = ["a.b"]
        expect(@tester.send("DROP", cond)).to be true
        cond = ["a.b","c"]
        expect(@tester.send("DROP", cond)).to be false
      end
      it "prepare_MONGODB" do
        ans = { "operand" => "test", "args"=>"ARGS"}
        expect(@tester.send("prepare_MONGODB", "test","")).to include ans 
      end
      it "MAPREDUCE" do
        @tester.send("MAPREDUCE", {})
      end
    
    end
  end
end

