

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodb2RedisOperation"
require_relative "../../common/utils"

module MongodbTest
  class ParserMock
    def initialize
    end
    def exec(a,b)
      return "PARSED"
    end
  end
  class QueryProcessorMock
    def initialize
      @flag = true
    end
    def setQueryFlag(bool)
      @flag = bool
    end
    def aggregation(a,b,c,d)
      return "AGGREGATED"
    end
    def query(a,b)
      return @flag
    end
  end
  
  class QueryParserMock
    def getParameter(a)
      return {"cond" => {"e" => {"$sum" => 1}},"match"=>{"b"=>"v"}}
    end
    def createKey2RealKey(a,b)
      return "realKey"
    end
    def createGroupKey(a,b)
      return "key"
    end
  end
  
  class Mock
    attr_reader :command, :setValue
    include MongoDB2RedisOperation
    def initialize(logger,options)
      @logger = logger
      @command = ""
      @parser = ParserMock.new()
      @options = options
      @getValue = nil
      @setValue = ""
      @utils = Utils.new()
      @queryParser = QueryParserMock.new()
      @queryProcessor = QueryProcessorMock.new()
    end
    def datamodel(d)
      @options[:datamodel] = d
    end
    def setGetValueHash
      @getValue  = '{"a":1,"b":"v","c":{"h":1},"d":true,"e":1.0}'
      @getValue += ',{"a":2,"b":"v","c":{"h":1},"d":false,"e":2.0}'
    end
    def resetGetValue
      @getValue = "reply"
    end
    def setQueryFlag(bool)
      @queryProcessor.setQueryFlag(bool)
    end
    private
    def parse_json(doc)
      @utils.parse_json(doc)
    end
    def convert_json(doc)
      @utils.convert_json(doc)
    end
    def monitor(a,b)
      # Do nothing 
    end
    def SET(args)
      @command = "#{__method__}"
      @setKey   = args[0]
      @setValue = args[1]
      return "OK"
    end
    def GET(args)
      @command = "#{__method__}"
      if(args[0] == nil)then
        return nil
      end
      return @getValue
    end
    def SMEMBERS(a,b)
      @command = "#{__method__}"
      if(a == nil)then
        return nil
      end

      return @getValue
    end
    def SADD(args)
      @command = "#{__method__}"
      return "OK"
    end
    def SREM(args)
      @command = "#{__method__}"
      return "OK"
  end
    def DEL(args)
      @command = "#{__method__}"
    return "OK"
    end
  end
  
  RSpec.describe 'Memcached TO Redis Unit Test' do
    before do
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::FATAL
      @tester = Mock.new(@logger,{:datamodel=>"DOCUMENT"})
    end
    context 'Operation' do
      it "MONGODB_INSERT" do
        @tester.datamodel("DOCUMENT")
        args = [[["key"],[{"a"=>1,"b"=>"v"}]]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq "OK"
        expect(@tester.command).to eq "SADD"
      end
      it "MONGODB_INSERT(error)" do
        args = [[["key"],[{"a"=>1,"b"=>"v"}]]]
        @tester.datamodel("KEYVALUE") ## unsupported.
        expect(@tester.send(:MONGODB_INSERT,args)).to eq "NG"
      end
      it "MONGODB_UPDATE(simple docs)" do
        ## setup
        @tester.setGetValueHash
        @tester.datamodel("DOCUMENT")
        
        args = {
          "key"    => "test_update",
          "update" => {"$set" => {"b" => "newVal"}},
          "query"  => {}
        }
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq "OK"
        expect(@tester.command).to eq "SET"
        ans  = "[{\"a\":1,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":true,\"e\":1.0}"
        ans += ",{\"a\":2,\"b\":\"v\",\"c\":{\"h\":1},\"d\":false,\"e\":2.0}]"
        expect(@tester.setValue).to eq ans
        @tester.resetGetValue
      end
      it "MONGODB_UPDATE(docs+multi)" do
        ## setup
        @tester.setGetValueHash
        @tester.datamodel("DOCUMENT")
        
        args = {
          "key"    => "test_update",
          "update" => {"$set" => {"b" => "newVal"}},
          "query"  => {},
          "multi"  => true
        }
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq "OK"
        expect(@tester.command).to eq "SET"
        ans  = "[{\"a\":1,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":true,\"e\":1.0}"
        ans += ",{\"a\":2,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":false,\"e\":2.0}]"
        expect(@tester.setValue).to eq ans
        @tester.resetGetValue
      end
      it "MONGODB_UPDATE(data model Error)" do
      args = {}
        @tester.datamodel("KEYVALUE") ## unsupported
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq "NG"
      end
      it "MONGODB_UPDATE(query Error)" do
        args = {
          "key"    => "test_update",
          "query"  => {},
        }
        @tester.datamodel("DOCUMENT")
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq "NG"
      end
      it "MONGODB_FIND" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1", "filter"=>{}}
        @tester.setGetValueHash
        exp = [{:a=>1, :b=>"v"}, {:a=>2, :b=>"v"}]
        ans = @tester.send(:MONGODB_FIND,args)
      @tester.resetGetValue
        expect(ans[0]).to include exp[0]
        expect(ans[1]).to include exp[1]
      end
      it "MONGODB_FIND (filter)" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1", "filter"=>{"a"=>1}}
        @tester.setGetValueHash
        exp = [{:a=>1, :b=>"v"}]
        ans = @tester.send(:MONGODB_FIND,args)
        @tester.resetGetValue
        expect(ans[0]).to include exp[0]
      end
      it "MONGODB_FIND(data model Error)" do
        @tester.datamodel("KEYVALUE") ## unsupported.
        args = {}
        expect(@tester.send(:MONGODB_FIND,args)).to eq "NG"
      end
      it "MONGODB_DELETE" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1","filter" => {}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq "OK"
      end
      it "MONGODB_DELETE (filter)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","filter" => {"a"=>1}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq "OK"
        @tester.resetGetValue
      end
      it "MONGODB_DELETE (filter : no return)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","filter" => {"b"=>"v"}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq "OK"
        @tester.resetGetValue
      end
      
      it "MONGODB_DELETE(data model Error)" do
        args = {}
        @tester.datamodel("KEYVALUE") ## unsupported.
        expect(@tester.send(:MONGODB_DELETE,args)).to eq "NG"
      end
      it "MONGODB_FINDANDMODIFY (Not Implemented)" do
        expect(@tester.send(:MONGODB_FINDANDMODIFY,{})).to eq "NG"
      end
      it "MONGODB_MAPREDUCE (Not Implemented)" do
        expect(@tester.send(:MONGODB_MAPREDUCE,{})).to eq "NG"
      end
      
      it "MONGODB_COUNT (string query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"b"=>"v"}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 2
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (fixnum query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"a"=>1}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 1
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (trueFlag query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"d"=>true}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 1
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (falseFlag query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"d"=>false}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 1
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (hash query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"c"=>{"h"=>1}}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 2
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (float query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"e"=>1.0}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 1
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (hash query & float query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"c"=>{"h"=>1}, "e"=>1.0}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 1
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (not found case)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"e"=>5.0}}
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 0
        @tester.resetGetValue
      end
      it "MONGODB_COUNT (data model Error)" do
        @tester.datamodel("KEYVALUE")
        expect(@tester.send(:MONGODB_COUNT,{})).to eq 0
      end
      it "MONGODB_AGGREGATE" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1",
          "query" =>{
            "$group"=>"{\"count\":\"$sum\":1}"},
          "match"=>{"b"=>"v"}
      }
        ans = {"key"=>{"e"=>"AGGREGATED"}}
        expect(@tester.send(:MONGODB_AGGREGATE,args)).to include ans
        @tester.resetGetValue
      end
    end
    context "Private Method" do 
      it "mongodbQuery Pattern (numeric query :true)" do
      doc = {:a=>1}
        query = {"a"=>1}
        expect(@tester.send(:mongodbQuery,doc,query)).to be true
        doc = {:a=>1.0}
        query = {"a"=>1.0}
        expect(@tester.send(:mongodbQuery,doc,query)).to be true
      end
      it "mongodbQuery Pattern (numeric query :false)" do
        doc = {:a=>1}
        query = {"a"=>2}
        expect(@tester.send(:mongodbQuery,doc,query)).to be false
        doc = {:a=>1.0}
        query = {"a"=>2.2}
        expect(@tester.send(:mongodbQuery,doc,query)).to be false
      end
      it "mongodbQuery Pattern (string query :true)" do
        doc = {:a=>"v"}
        query = {"a"=>"v"}
        expect(@tester.send(:mongodbQuery,doc,query)).to be true
      end
      it "mongodbQuery Pattern (string query :false)" do
        doc = {:a=>"v"}
        query = {"a"=>"mm"}
        expect(@tester.send(:mongodbQuery,doc,query)).to be false
      end
      it "mongodbQuery Pattern (Hash query :true)" do
        doc = {:a=>{:b=>"v"}}
        query = {"a"=>{:b=>"v"}}
        expect(@tester.send(:mongodbQuery,doc,query)).to be true
    end
      it "mongodbQuery Pattern (Hash query :false)" do
        @tester.setQueryFlag(false)
        doc = {:a=>{:b=>"v"}}
        query = {"a"=>{:b=>"mm"}}
      expect(@tester.send(:mongodbQuery,doc,query)).to be false
        @tester.setQueryFlag(true)
      end
      it "mongodbQuery Pattern (no query target)" do
        doc = {:a=>1}
        query = {"b"=>1}
        expect(@tester.send(:mongodbQuery,doc,query)).to be false
      end
      it "mongodbQuery Pattern (no query)" do
        doc = {:a=>1}
        query = nil
        expect(@tester.send(:mongodbQuery,doc,query)).to be false
      end
      it "prepare_mongodb" do
        ans = {"operand" => "MONGODB_test", "args" => "PARSED"}
        expect(@tester.send(:prepare_mongodb,"test",{})).to include ans
      end
    end
  end
end
