# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraParser"

module CassandraParserTester
  RSpec.describe 'Cassandra Parser Unit TEST' do
    before(:each) do
      filename = ""
      logger = DummyLogger.new
      option = {}
      @tester = CassandraParser.new(filename,option,logger)
    end
    context "CQL3" do
      it "Parse" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{'query': 'select field0,field1 from testdb.testcf;', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        ans = {"select"=>["select", "field0,field1", "from", "testdb.testcf"]}
        expect(@tester.parse(line)).to eq ans
      end
      it "Parse with WHERE" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{ 'query': 'SELECT * FROM testdb.testcf WHERE field0 = B', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        ans = {"select" => ["SELECT", "*", "FROM", "testdb.testcf", "WHERE", "field0", "=", "'B'"]}
        expect(@tester.parse(line)).to eq ans
      end
      it "Parse for unsupported command" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{'query': 'bug field0,field1 from testdb.testcf;', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        expect(@tester.parse(line)).to eq nil
      end
      it "Parse for SKIP case" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{'query': 'USE;', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        expect(@tester.parse(line)).to eq nil
      end
      it "Parse for Command SUPPORTED & UNIMPLEMENTED)" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        @tester.instance_variable_set(:@supportedCommand,"UNIMPLE")
        line = "{'query': 'UNIMPLE;', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        expect(@tester.parse(line)).to eq nil
      end
      it "ParseINSERT_CQL3" do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{'query': 'insert into testdb.testcf (field0,field1,field2,field3) values (''A'',''G1'',1,10);', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        result = {"insert"=>["insert", "into", "testdb.testcf",
                             "(field0,field1,field2,field3)",
                             "values", "('\"A\"','\"G1\"','1','10')"]}
        ans = {"insert"=>["insert", "into", "testdb.testcf", "(field0,field1,field2,field3)", "values", "(''A'',''G1'','1','10')"]}
        expect(@tester.parse_insert_cql3(result)).to eq ans
      end
      it "ParseSELECT_CQL3 " do
        result = {"select"=>["select", "field0,field1", "from", "testdb.testcf"]}
        ans = {"select"=>["select", "field0,field1", "from", "testdb.testcf"]}
        expect(@tester.parse_select_cql3(result)).to eq ans
      end
      it "ParseUPDATE_CQL3" do
        line = "{'query': 'update testdb.testcf SET field3=45 WHERE field0=''C'';', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        ans = {"update"=>["update", "testdb.testcf", "SET", "field3=45", "WHERE", "field0=\"C\""]}
        expect(@tester.parse(line)).to eq ans
      end
      it "ParseDELETE_CQL3"  do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{ 'query': 'delete from testdb.testcf WHERE field0=''C'';', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        ans = {"delete"=>["delete", "from", "testdb.testcf", "WHERE", "field0=\"C\""]}
        expect(@tester.parse(line)).to eq ans
      end
      it "ParseDROP_CQL3"  do
        @tester.instance_variable_set(:@option,{:inputFormat => "cql3"})
        line = "{ 'query': 'drop keyspace testdb;', 'serial_consistency_level': 'SERIAL'} | Execute CQL3 query "
        ans = {"drop"=>["drop", "keyspace", "testdb"]}
        expect(@tester.parse(line)).to eq ans
      end
      it "Unsupoorted API" do
        @tester.instance_variable_set(:@option,{:inputFormat => "java"})
        expect(@tester.parse("")).to eq nil
      end
    end
  end
end

