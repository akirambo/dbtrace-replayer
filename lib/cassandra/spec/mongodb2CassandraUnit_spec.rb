
# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodb2CassandraOperation"

module Mongodb2CassandraOperationTester
  class ParserMock
    def exec(a,b)
      return "OK"
    end
  end
  class CassandraSchemaMock
    attr_accessor :fields, :checkValue, :stringTypeValue
    def createQuery
      return "dummy query"
    end
    def check(a,b)
      return @checkValue
    end
    def extractKeyValue(kv)
      return {"key"=>"'_id','f0'","value"=>"'k0','v0'"}
    end
    def stringType(v)
      return @stringTypeValue
    end
    def primaryKeys
      return ["f0"]
    end
  end
  class QueryParserMock
    attr_accessor :targetKeysValue
    def targetKeys(a)
      return @targetKeysValue
    end
    def csv2docs(a,b)
      return ["dummy","dummy"]
    end
    def getParameter(a)
      return {"cond" => {"f1" => "$sum"}}
    end
    def createGroupKey(a,b)
      return "f2"
    end
  end
  class QueryProcessorMock
    def aggregation(a,b,c)
      return "dummy"
    end
  end
  class Mock
    attr_accessor :value, :raiseError, 
    :command, :schemas, :returnParseJSON, :raiseParseJSONError
    include Mongodb2CassandraOperation
    def initialize
      @parser = ParserMock.new
      @schemas = {}
      @raiseError = false
      @logger = DummyLogger.new
      @queryParser = QueryParserMock.new
      @queryProcessor = QueryProcessorMock.new
      @options = {
        :keyspace => "k",
        :columnfamily => "f"
      }
    end
    def DIRECT_EXECUTER(a,b=false)
      @command = a
      if(@raiseError)then
        raise ArgumentError, "Error"
      end
      return @value
    end
    def DIRECT_SELECT(a)
      @command = a
      if(@raiseError)then
        raise ArgumentError, "Error"
      end
      return @value
    end
    def change_numeric_when_numeric(str)
      return str
    end
    def setTargetKeysValue(a)
      @queryParser.targetKeysValue = a
    end
    def parse_json(a)
      if(@raiseParseJSONError)then
        raise ArgumentError, "Error"
      end
      return @returnParseJSON
    end
    def monitor(a,b)
    end
  end

  RSpec.describe 'Mongodb To CassandraOperation Unit TEST' do
    before (:each) do
      @tester = Mock.new
    end
    context "INSERT Operation" do
      it "MONGODB_INSERT(simple case :: string)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = false
        @tester.returnParseJSON = {"_id"=>"k0", "f0"=>"v0"}
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        @tester.schemas["k.f"].fields = ["_id","f0"]
        ## args[2] means bulk import or not
        args = [["k.f", "dummy",false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq true
        command = "INSERT INTO k.f ('_id','f0') VALUES ('k0','v0');"
        expect(@tester.command).to eq command
      end
      it "MONGODB_INSERT(simple case :: array)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = false
        @tester.returnParseJSON = {"_id"=>"k0", "f0"=>"v0"}
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        @tester.schemas["k.f"].fields = ["_id","f0"]

        ## args[2] means bulk import or not
        args = [["k.f", ["dummy"],false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq true
        command = "INSERT INTO k.f ('_id','f0') VALUES ('k0','v0');"
        expect(@tester.command).to eq command
      end
      it "MONGODB_INSERT(simple case :: hash)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        @tester.schemas["k.f"].fields = ["_id","f0"]
        ## args[2] means bulk import or not
        args = [["k.f", {"_id"=>"k0","value"=>"v0"},false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq true
        command = "INSERT INTO k.f ('_id','f0') VALUES ('k0','v0');"
        expect(@tester.command).to eq command
      end
      it "MONGODB_INSERT(error case :: no arg[1])" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        args = [["k.f"]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
      it "MONGODB_INSERT(error case :: parse_json Error)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        ## args[2] means bulk import or not
        args = [["k.f", "dummy",false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
      it "MONGODB_INSERT(error case :: no keyValue)" do
        @tester.raiseError = false
        @tester.returnParseJSON = {}
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        ## args[2] means bulk import or not
        args = [["k.f", "dummy",false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
      it "MONGODB_INSERT(error casae :: query error)" do
        @tester.raiseError = true
        @tester.raiseParseJSONError = false
        @tester.returnParseJSON = {"_id"=>"k0", "f0"=>"v0"}
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = true
        @tester.schemas["k.f"].fields = ["_id","f0"]
        args = [["k.f", ["dummy"],false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
      it "MONGODB_INSERT(error case :: not match schema)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = false
        @tester.returnParseJSON = {"_id"=>"k0", "f0"=>"v0"}
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].checkValue = false
        @tester.schemas["k.f"].fields = ["_id","f0"]
        args = [["k.f", ["dummy"],false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
      it "MONGODB_INSERT(error casae :: not match schema)" do
        @tester.raiseError = false
        @tester.raiseParseJSONError = false
        @tester.returnParseJSON = {"_id"=>"k0", "f0"=>"v0"}
        @tester.schemas = {"k.f0" => CassandraSchemaMock.new}
        args = [["k.f", ["dummy"],false]]
        expect(@tester.send(:MONGODB_INSERT,args)).to eq false
      end
    end
    context "UPDATE Operation" do
      it "MONGODB_UPDATE(simple case :: w/ update(string) & query)" do
        @tester.raiseError = false      
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].stringTypeValue = true
        args = { "key" => "k.f",
          "update" => {"$set" => {"f0" =>"v0","f1"=>"v1"}},
          "query"  => {"f2" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq true
        ans = "UPDATE k.f SET f0='v0', f1='v1' WHERE f2 > 10;" 
        expect(@tester.command).to eq ans
      end

      it "MONGODB_UPDATE(error case)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.schemas["k.f"].stringTypeValue = true
        args = { "key" => "k.f",
          "update" => {"$set" => {"f0" =>"v0","f1"=>"v1"}},
          "query"  => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_UPDATE,args)).to eq false
      end
    end
    context "FIND Operation" do
      it "MONGODB_FIND(simple case)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.value   = "v0,v1\nv2,v3"
        args = { "key" => "k.f",
          "filter" => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_FIND,args)).to eq "v0,v1\nv2,v3"
        ans = "SELECT * FROM k.f WHERE f0 > 10 ALLOW FILTERING;" 
        expect(@tester.command).to eq ans
      end
      it "MONGODB_FIND(error case)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.value   = "v0,v1\nv2,v3"
        args = { "key" => "k.f",
          "filter" => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_FIND,args)).to eq ""
      end     
    end
    context "COUNT Operation" do
      it "MONGODB_COUNT(simple case)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.value   = "10"
        args = { "key" => "k.f",
          "filter" => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 10
        ans = "SELECT count(*) FROM k.f WHERE f0 > 10;" 
        expect(@tester.command).to eq ans
      end
      it "MONGODB_COUNT(error case :: Error)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.value   = {"a"=>"b"}
        args = { "key" => "k.f",
          "filter" => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 0
      end
      it "MONGODB_COUNT(error case ::Return Hash)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.value   = {"a"=>"b"}
        args = { "key" => "k.f",
          "filter" => {"f0" => {"$gt" => 10}}
        }
        expect(@tester.send(:MONGODB_COUNT,args)).to eq 0
      end
    end
    context "DELETE Operation" do
      it "MONGODB_DELETE(simple case :: w/o Filter)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        args = { "key" => "k.f", "filter" => {}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq true
        expect(@tester.command).to eq "TRUNCATE k.f;"
      end
      it "MONGODB_DELETE(erorr case :: w/o Filter)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        args = { "key" => "k.f", "filter" => {}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq false
      end
      it "MONGODB_DELETE(simple case :: w/ Filter)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        args = { "key" => "k.f", "filter" => {"f0" => {"$gt" => 10}}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq true
        command = "DELETE FROM k.f WHERE f0 > 10;"
        expect(@tester.command).to eq command
      end
      it "MONGODB_DELETE(error case :: w/ Filter)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        args = { "key" => "k.f", "filter" => {"f0" => {"$gt" => 10}}}
        expect(@tester.send(:MONGODB_DELETE,args)).to eq false
        command = "DELETE FROM k.f WHERE f0 > 10;"
        expect(@tester.command).to eq command
      end
    end
    context "AGGREGATE Operation" do
      it "MONGODB_AGGREGATE(simple case)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.setTargetKeysValue(["f0","f1"])
        args = { "key" => "k.f", "match" => {"f0" => "v0"}}
        @tester.send(:MONGODB_AGGREGATE,args)
        command = "SELECT f0,f1 FROM k.f WHERE f0 = 'v0';"
        expect(@tester.command).to eq command
      end
      it "MONGODB_AGGREGATE(simple case)" do
        @tester.raiseError = false
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.setTargetKeysValue([])
        args = { "key" => "k.f", "match" => {"f0" => "v0"}}
        @tester.send(:MONGODB_AGGREGATE,args)
        command = "SELECT * FROM k.f WHERE f0 = 'v0';"
        expect(@tester.command).to eq command
      end
      it "MONGODB_AGGREGATE(simple case)" do
        @tester.raiseError = true
        @tester.schemas = {"k.f" => CassandraSchemaMock.new}
        @tester.setTargetKeysValue([])
        args = { "key" => "k.f", "match" => {"f0" => "v0"}}
        @tester.send(:MONGODB_AGGREGATE,args)
      end      
    end
    context "Private Method" do
      it "prepare_MONGODB" do
        ans = {"operand" => "MONGODB_test", "args" => "OK"}
        expect(@tester.send(:prepare_MONGODB,"test","test")).to eq ans
      end
      it "mongodbParserQuery($gt)" do
        hash = {"c0" => {"$gt" => 10}}
        ans = "c0 > 10"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery($gte)" do
        hash = {"c0" => {"$gte" => 10}}
        ans = "c0 >= 10"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery($lt)" do
        hash = {"c0" => {"$lt" => 10}}
        ans = "c0 < 10"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery($lte)" do
        hash = {"c0" => {"$lte" => 10}}
        ans = "c0 <= 10"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery(error)" do
        hash = {"c0" => {"$max" => 10}}
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ""
      end
      it "mongodbParserQuery(equel primary key)" do
        hash = {"_id" => "aaa-01"}
        ans = "mongoid = 'aaa01'"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery(equal)" do
        hash = {"c0" => "5"}
        ans = "c0 = '5'"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery(equal)" do
        hash = {"c0" => "5"}
        ans = "c0 = '5'"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
      it "mongodbParserQuery(equal + $gt)" do
        hash = {"c0" => "5", "p0" => {"$gt" => 10}, "_id" => "m0"}
        ans = "c0 = '5' AND p0 > 10"
        expect(@tester.send(:mongodbParseQuery,hash)).to eq ans
      end
    end
  end
end
