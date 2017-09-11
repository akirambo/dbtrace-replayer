# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraOperation"

module CassandraOperationTester
  class MetricsMock
    def start_monitor(a,b)
    end
    def end_monitor(a,b)
    end
  end
  class ClientMock
    attr_accessor :value, :queryReturn
    def syncExecuter(q)
      return @queryReturn
    end
    def getReply(a)
      return @value
    end
    def getDuration()
      return 0.1
    end
    def commitQuery(a)
      return @queryReturn
    end
    def asyncExecuter()
    end
    def resetQuery()
    end
  end
  class ParserMock 
    attr_accessor :value
    def parseBatchMutateParameter(a)
      return @value
    end
    def parseGetRangeSlicesParameter(args)
      return @value
    end
    def parseGetSliceParameter(args)
      return @value
    end
    def parseGETINDEXEDSLICESParameter(args)
      return @value
    end
    def parseMULTIGETSLICEParameter(args)
      return @value
    end
  end
  class CassandraMock
    include CassandraOperation
    def initialize
      @logger = DummyLogger.new
      @client = ClientMock.new
      @metrics = MetricsMock.new
      @parser = ParserMock.new
      @option = {
        :async => false,
        :poolRequestMaxSize => -1
      }
      @pool_request_size = 0
      @pool_byte_size = 0
    end
    ## For Mock
    def setAsync
      @option[:async] = true
    end
    def setSync
      @option[:async] = false
    end 
    def setPoolRequestMaxSize(i)
      @option[:poolRequestMaxSize] = i
    end
    def setValue(v)
      @client.value = v
    end
    def setFlag(bool)
      @client.queryReturn = bool
    end
    def setParserReturnValue(hash)
      @parser.value = hash
    end
    def add_duration(a,b,c)
    end
    def add_count(a)
    end
    def add_total_duration(a,b)
    end
    def connect
    end
    def close
    end
  end

  RSpec.describe 'CassandraOperation Unit TEST' do
    before do
      @tester = CassandraMock.new
    end
    context "Cassandra Operation" do
      it "DIRECT_SELECT" do
        query = "SELECT * FROM test;"
        ans   = "[{\"f0\":\"v0\",\"f1\":\"v1\"},{\"f0\":\"a0\",\"f1\":\"a1\"}]"
        @tester.setFlag(true)
        @tester.setValue(ans)
        expect(@tester.send(:DIRECT_SELECT,query)).to eq ans
      end
      it "DIRECT_EXECUTER (ONE QUERY && SYNC) [SELECT]" do
        @tester.setSync
        query = "SELECT * FROM test;"
        ans   = "[{\"f0\":\"v0\",\"f1\":\"v1\"},{\"f0\":\"a0\",\"f1\":\"a1\"}]"
        @tester.setFlag(true)
        @tester.setValue(ans)
        expect(@tester.send(:DIRECT_EXECUTER,query)).to eq ans
      end
      it "DIRECT_EXECUTER (ONE QUERY && SYNC) [INSERT]" do
        @tester.setSync
        query = "INSERT XXXX;"
        ans   = {}
        @tester.setFlag(true)
        @tester.setValue(ans)
        expect(@tester.send(:DIRECT_EXECUTER,query)).to eq ans
      end
      it "DIRECT_EXECUTER (ONE QUERY && ASYNC+ontime) [SELECT]" do
        @tester.setAsync
        query = "SELECT * FROM test;"
        ans   = "[{\"f0\":\"v0\",\"f1\":\"v1\"},{\"f0\":\"a0\",\"f1\":\"a1\"}]"
        @tester.setFlag(true)
        @tester.setValue(ans)
        expect(@tester.send(:DIRECT_EXECUTER,query)).to eq ans
      end
      it "DIRECT_EXECUTER (ONE QUERY && ASYNC + !ontime) [SELECT]" do
        query = "SELECT * FROM test;"
        ans   = "[{\"f0\":\"v0\",\"f1\":\"v1\"},{\"f0\":\"a0\",\"f1\":\"a1\"}]"
        @tester.setAsync
        @tester.setPoolRequestMaxSize(0)
        @tester.setFlag(true)
        @tester.setValue(ans)
        expect(@tester.send(:DIRECT_EXECUTER,query,false)).to include {}
      end
      it "DIRECT_EXECUTER (ONE QUERY && ASYNC + !ontime) [SELECT]" do
        query = "SELECT * FROM test;"
        @tester.setAsync
        @tester.setPoolRequestMaxSize(-1)
        @tester.setFlag(true)
        expect(@tester.send(:DIRECT_EXECUTER,query,false)).to be true
      end
      it "DIRECT_EXECUTER (MULTI QUERIES) [SELECT]" do
        query = ["SELECT * FROM test;","SELECT * FROM test;"]
        @tester.setAsync
        @tester.setPoolRequestMaxSize(-1)
        @tester.setValue(true)
        expect(@tester.send(:DIRECT_EXECUTER,query,false)).to be true
      end
      it "DIRECT_EXECUTER (MULTI QUERIES) [SELECT,SELECT] (false case)" do
        query = ["SELECT * FROM test;","SELECT * FROM test;"]
        @tester.setAsync
        @tester.setPoolRequestMaxSize(-1)
        @tester.setValue(false)
        expect(@tester.send(:DIRECT_EXECUTER,query,false)).to be false
      end
    end
    context "Private Method" do
      it "prepare_cassandra (CQL)" do
        ope = "DIRECT_EXECUTER"
        args = ["a","b","c"]
        ans = {
          "operand" => "DIRECT_EXECUTER",
          "args" => "a b c"
        }
        expect(@tester.send(:prepare_cassandra,ope,args)).to include ans
      end
      it "prepare_cassandra (BATCH_MUTATE)" do
        ope = "BATCH_MUTATE"
        ans = {
          "operand" => "DIRECT_EXECUTER",
          "args" => "INSERT INTO t1 (rkey,f0,f1) VALUES(r0,v0,v1)"
        }
        hash = {
          "counterColumn" => nil,
          "keyValue" => {"f0"=>"v0","f1"=>"v1"},
          "rowKey"   => "rkey",
          "rowValue" => "r0",
          "table"    => "t1"    
        }
        @tester.setParserReturnValue(hash)
        expect(@tester.send(:prepare_cassandra,ope,"")).to include ans
      end
      it "prepare_BATCH_MUTATE (error case)" do
        hash = {
          "counterColumn" => nil,
          "keyValue" => {},
          "rowKey"   => "rkey",
          "rowValue" => "r0",
          "table"    => "t1"    
        }
        @tester.setParserReturnValue(hash)
        expect(@tester.send(:prepare_BATCH_MUTATE,"")).to eq ""
      end
      it "prepare_BATCH_MUTATE (counter case)" do
        hash = {
          "counterColumn" => ["c0"],
          "counterKeyValue" => [{"f0"=>10,"f1"=>20},{"f1"=>10,"f2"=>20}],
          "keyValue" => [{"f0"=>10,"f1"=>20},{"f0"=>15,"f1"=>25}],
          "rowKey"   => "rkey",
          "rowValue" => "r0",
          "table"    => "t1"    
        }
        @tester.setParserReturnValue(hash)
        ans = []
        ans.push("UPDATE t1 SET f0 = f0 + 10  WHERE rkey = r0 AND f0 = '10' AND f1 = '20'")
        ans.push("UPDATE t1 SET f1 = f1 + 20  WHERE rkey = r0 AND f0 = '10' AND f1 = '20'")
        ans.push("UPDATE t1 SET f1 = f1 + 10  WHERE rkey = r0 AND f0 = '15' AND f1 = '25'")
        ans.push("UPDATE t1 SET f2 = f2 + 20  WHERE rkey = r0 AND f0 = '15' AND f1 = '25'")
        expect(@tester.send(:prepare_BATCH_MUTATE,"")).to match_array ans
      end
      it "prepare_GET_RANGE_SLICES (with start_key & end_key)" do
        hash = {"table" => "t1", "start_key" => 0, "end_key" => 10,
          "primaryKey" => "pkey", "count" => 100}
        @tester.setParserReturnValue(hash)
        ans = "SELECT * FROM t1 WHERE pkey >= 0 AND pkey <= 10 limit 100;"
        expect(@tester.send(:prepare_GET_RANGE_SLICES,"")).to eq ans
      end
      it "prepare_GET_RANGE_SLICES (with start_key & end_key)" do
        hash = {"table" => "t1", "end_key" => 10,
          "primaryKey" => "pkey", "count" => 100}
        @tester.setParserReturnValue(hash)
        ans = "SELECT * FROM t1 WHERE pkey <= 10 limit 100;"
        expect(@tester.send(:prepare_GET_RANGE_SLICES,"")).to eq ans
      end
      it "prepare_GET_RANGE_SLICES (with end_key)" do
        hash = {"table" => "t1", "start_key" => 0, 
          "primaryKey" => "pkey", "count" => 100}
        @tester.setParserReturnValue(hash)
        ans = "SELECT * FROM t1 WHERE pkey >= 0 limit 100;"
        expect(@tester.send(:prepare_GET_RANGE_SLICES,"")).to eq ans
      end
      it "prepare_GET_SLICE" do
        hash = {"table" => "t1", "targetKey" => "f0",
          "primaryKey" => "pkey", "count" => 100}
        @tester.setParserReturnValue(hash)
        ans = "SELECT * FROM t1 WHERE pkey = f0 limit 100;"
        expect(@tester.send(:prepare_GET_SLICE,"")).to eq ans
      end
      it "prepare_GET_INDEXED_SLICES" do
        hash = "DUMMY"
        @tester.setParserReturnValue(hash)
        expect(@tester.send(:prepare_GET_INDEXED_SLICES,"")).to eq "DUMMY"
      end
      it "prepare_MULTIGET_SLICE" do
        hash = {"table" => "t1", "primaryKey" => "pkey", "keys" => ["A","B"]}
        @tester.setParserReturnValue(hash)
        ans = "SELECT * FROM t1 WHERE pkey IN (A,B);"
        expect(@tester.send(:prepare_MULTIGET_SLICE,"")).to eq ans
      end
      it "normalizeCassandraQuery" do
        query = "\"a--b__DOUBLEQ__\""
        ans   = "'ab\"';"
        expect(@tester.send(:normalizeCassandraQuery,query)).to eq ans
      end
      it "exec_buffered_queries" do
        @tester.setValue("")
        expect(@tester.send(:exec_buffered_queries)).to eq ""
      end
    end
  end
end

