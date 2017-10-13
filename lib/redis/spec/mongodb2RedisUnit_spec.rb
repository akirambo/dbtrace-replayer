

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
    def get_parameter(a)
      return {"cond" => {"e" => {"$sum" => 1}},"match"=>{"b"=>"v"}}
    end
    def createkey2realkey(a,b)
      return "realKey"
    end
    def create_groupkey(a,b)
      return "key"
    end
  end
  
  class Mock
    attr_reader :command, :setValue
    include MongoDB2RedisOperation
    def initialize(logger,option)
      @logger = logger
      @command = ""
      @parser = ParserMock.new()
      @option = option
      @getValue = nil
      @setValue = ""
      @utils = Utils.new()
      @query_parser = QueryParserMock.new()
      @query_processor = QueryProcessorMock.new()
    end
    def datamodel(d)
      @option[:datamodel] = d
    end
    def setGetValueHash
      @getValue  = '{"a":1,"b":"v","c":{"h":1},"d":true,"e":1.0}'
      @getValue += ',{"a":2,"b":"v","c":{"h":1},"d":false,"e":2.0}'
    end
    def resetGetValue
      @getValue = "reply"
    end
    def setQueryFlag(bool)
      @query_processor.setQueryFlag(bool)
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
    def set(args)
      @command = "#{__method__}"
      @setKey   = args[0]
      @setValue = args[1]
      return "OK"
    end
    def get(args)
      @command = "#{__method__}"
      if(args[0] == nil)then
        return nil
      end
      return @getValue
    end
    def smembers(a,b)
      @command = "#{__method__}"
      if(a == nil)then
        return nil
      end
      return @getValue
    end
    def sadd(args)
      @command = "#{__method__}"
      return "OK"
    end
    def srem(args)
      @command = "#{__method__}"
      return "OK"
    end
    def del(args)
      @command = "#{__method__}"
    return "OK"
    end
  end
  
  RSpec.describe 'Mongodb TO Redis Unit Test' do
    before do
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::FATAL
      @tester = Mock.new(@logger,{:datamodel=>"DOCUMENT", :key_of_keyvalue => "_id"})
    end
    context 'Operation' do
      it "mongodb_insert" do
        @tester.datamodel("DOCUMENT")
        args = [[["key"],[{"a"=>1,"b"=>"v"}]]]
        expect(@tester.send(:mongodb_insert,args)).to eq "OK"
        expect(@tester.command).to eq "sadd"
      end
      it "mongodb_insert(error)" do
        args = [[["key"],[{"a"=>1,"b"=>"v"}]]]
        @tester.datamodel("KEYVALUE") ## unsupported.
        expect(@tester.send(:mongodb_insert,args)).to eq "OK"
      end
      it "mongodb_update(simple docs)" do
        ## setup
        @tester.setGetValueHash
        @tester.datamodel("DOCUMENT")        
        args = {
          "key"    => "test_update",
          "update" => {"$set" => {"b" => "newVal"}},
          "query"  => {}
        }
        expect(@tester.send(:mongodb_update,args)).to eq "OK"
        expect(@tester.command).to eq "set"
        ans  = "[{\"a\":1,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":true,\"e\":1.0}"
        ans += ",{\"a\":2,\"b\":\"v\",\"c\":{\"h\":1},\"d\":false,\"e\":2.0}]"
        expect(@tester.setValue).to eq ans
        @tester.resetGetValue
      end
      it "mongodb_update(docs+multi)" do
        ## setup
        @tester.setGetValueHash
        @tester.datamodel("DOCUMENT")
        args = {
          "key"    => "test_update",
          "update" => {"$set" => {"b" => "newVal"}},
          "query"  => {},
          "multi"  => true
        }
        expect(@tester.send(:mongodb_update,args)).to eq "OK"
        expect(@tester.command).to eq "set"
        ans  = "[{\"a\":1,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":true,\"e\":1.0}"
        ans += ",{\"a\":2,\"b\":\"newVal\",\"c\":{\"h\":1},\"d\":false,\"e\":2.0}]"
        expect(@tester.setValue).to eq ans
        @tester.resetGetValue
      end
      it "mongodb_update(keyvalue)" do
      args = [[nil,[{"_id" => "a", "v" => "b"}]]]
        @tester.datamodel("KEYVALUE")
        expect(@tester.send(:mongodb_update,args)).to eq "OK"
      end
      it "mongodb_update(query Error)" do
        args = {
          "key"    => "test_update",
          "query"  => {},
        }
        @tester.datamodel("DOCUMENT")
        expect(@tester.send(:mongodb_update,args)).to eq "NG"
      end
      it "mongodb_find" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1", "filter"=>{}}
        @tester.setGetValueHash
        exp = [{:a=>1, :b=>"v"}, {:a=>2, :b=>"v"}]
        ans = @tester.send(:mongodb_find,args)
      @tester.resetGetValue
        expect(ans[0]).to include exp[0]
        expect(ans[1]).to include exp[1]
      end
      it "mongodb_find (filter)" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1", "filter"=>{"a"=>1}}
        @tester.setGetValueHash
        exp = [{:a=>1, :b=>"v"}]
        ans = @tester.send(:mongodb_find,args)
        @tester.resetGetValue
        expect(ans[0]).to include exp[0]
      end
      it "mongodb_find(keyvalue)" do
        @tester.datamodel("KEYVALUE")
        @tester.resetGetValue
        args = {"filter" => {"_id" => "a"} }
        expect(@tester.send(:mongodb_find,args)).to eq "reply"
      end
      it "mongodb_delete" do
        @tester.datamodel("DOCUMENT")
        args = {"key"=>"k1","filter" => {}}
        expect(@tester.send(:mongodb_delete,args)).to eq "OK"
      end
      it "mongodb_delete (filter)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","filter" => {"a"=>1}}
        expect(@tester.send(:mongodb_delete,args)).to eq "OK"
        @tester.resetGetValue
      end
      it "mongodb_delete (filter : no return)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","filter" => {"b"=>"v"}}
        expect(@tester.send(:mongodb_delete,args)).to eq "OK"
        @tester.resetGetValue
      end
      it "mongodb_delete(data model Error)" do
        args = {}
        @tester.datamodel("KEYVALUE") ## unsupported.
        expect(@tester.send(:mongodb_delete,args)).to eq "NG"
      end
      it "mongodb_findandmodify (Not Implemented)" do
        expect(@tester.send(:mongodb_findandmodify,{})).to eq "NG"
      end
      it "mongodb_upsert (Not Implemented)" do
        expect(@tester.send(:mongodb_upsert,{})).to eq "NG"
      end
      it "mongodb_group (Not Implemented)" do
        expect(@tester.send(:mongodb_group,{})).to eq "NG"
      end
      it "mongodb_mapreduce (Not Implemented)" do
        expect(@tester.send(:mongodb_mapreduce,{})).to eq "NG"
      end
      it "mongodb_count (string query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"b"=>"v"}}
        expect(@tester.send(:mongodb_count,args)).to eq 2
        @tester.resetGetValue
      end
      it "mongodb_count (fixnum query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"a"=>1}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        @tester.resetGetValue
      end
      it "mongodb_count (trueFlag query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"d"=>true}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        @tester.resetGetValue
      end
      it "mongodb_count (falseFlag query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"d"=>false}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        @tester.resetGetValue
      end
      it "mongodb_count (hash query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"c"=>{"h"=>1}}}
        expect(@tester.send(:mongodb_count,args)).to eq 2
        @tester.resetGetValue
      end
      it "mongodb_count (float query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"e"=>1.0}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        @tester.resetGetValue
      end
      it "mongodb_count (hash query & float query)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"c"=>{"h"=>1}, "e"=>1.0}}
        expect(@tester.send(:mongodb_count,args)).to eq 1
        @tester.resetGetValue
      end
      it "mongodb_count (not found case)" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1","query" => {"e"=>5.0}}
        expect(@tester.send(:mongodb_count,args)).to eq 0
        @tester.resetGetValue
      end
      it "mongodb_count (data model Error)" do
        @tester.datamodel("KEYVALUE")
        expect(@tester.send(:mongodb_count,{})).to eq 0
      end
      it "mongodb_aggregate" do
        @tester.datamodel("DOCUMENT")
        @tester.setGetValueHash
        args = {"key"=>"k1",
          "query" =>{
            "$group"=>"{\"count\":\"$sum\":1}"},
          "match"=>{"b"=>"v"}
      }
        ans = {"key"=>{"e"=>"AGGREGATED"}}
        expect(@tester.send(:mongodb_aggregate,args)).to include ans
        @tester.resetGetValue
      end
    end
    context "Private Method" do 
      it "mongodb_query Pattern (numeric query :true)" do
      doc = {:a=>1}
        query = {"a"=>1}
        expect(@tester.send(:mongodb_query,doc,query)).to be true
        doc = {:a=>1.0}
        query = {"a"=>1.0}
        expect(@tester.send(:mongodb_query,doc,query)).to be true
      end
      it "mongodb_query Pattern (numeric query :false)" do
        doc = {:a=>1}
        query = {"a"=>2}
        expect(@tester.send(:mongodb_query,doc,query)).to be false
        doc = {:a=>1.0}
        query = {"a"=>2.2}
        expect(@tester.send(:mongodb_query,doc,query)).to be false
      end
      it "mongodb_query Pattern (string query :true)" do
        doc = {:a=>"v"}
        query = {"a"=>"v"}
        expect(@tester.send(:mongodb_query,doc,query)).to be true
      end
      it "mongodb_query Pattern (string query :false)" do
        doc = {:a=>"v"}
        query = {"a"=>"mm"}
        expect(@tester.send(:mongodb_query,doc,query)).to be false
      end
      it "mongodb_query Pattern (Hash query :true)" do
        doc = {:a=>{:b=>"v"}}
        query = {"a"=>{:b=>"v"}}
        expect(@tester.send(:mongodb_query,doc,query)).to be true
    end
      it "mongodb_query Pattern (Hash query :false)" do
        @tester.setQueryFlag(false)
        doc = {:a=>{:b=>"v"}}
        query = {"a"=>{:b=>"mm"}}
      expect(@tester.send(:mongodb_query,doc,query)).to be false
        @tester.setQueryFlag(true)
      end
      it "mongodb_query Pattern (no query target)" do
        doc = {:a=>1}
        query = {"b"=>1}
        expect(@tester.send(:mongodb_query,doc,query)).to be false
      end
      it "mongodb_query Pattern (no query)" do
        doc = {:a=>1}
        query = nil
        expect(@tester.send(:mongodb_query,doc,query)).to be false
      end
      it "prepare_mongodb" do
        ans = {"operand" => "mongodb_test", "args" => "PARSED"}
        expect(@tester.send(:prepare_mongodb,"test",{})).to include ans
      end
    end
  end
end
