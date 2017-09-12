# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraRunner"
require_relative "../../common/utils"

RSpec.describe 'CassandraOperation Unit TEST (C++ API) [Each Connection]' do
  before do
    @logger = DummyLogger.new
    @option = {
      :keyspace => "test",
      :api => "cxx",
      :keepalive => false
    }
    @option[:sourceDB] = "cassandra"
    @runner = CassandraRunner.new("cassandra",@logger,@option)
  end
  context " > Cassandra Operation" do
    before (:each) do
      @runner.send("DIRECT_EXECUTER","DROP KEYSPACE IF EXISTS test;")
      @runner.send("DIRECT_EXECUTER","CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
      @runner.send("DIRECT_EXECUTER","CREATE TABLE test.test (id int, value text, PRIMARY KEY(id));")
    end
    it "DIRECT_EXECUTER (insert)" do
      query = "INSERT INTO test.test (id,value) VALUES (1,'text001');"
      @runner.send("DIRECT_EXECUTER",query)
      query = "SELECT id,value FROM test.test;"
      ans = @runner.send("DIRECT_EXECUTER",query).split(",")
      ## id
      expect(ans[0]).to eq "1"
      ## value
      expect(ans[1]).to eq "text001"
    end
  end
end



RSpec.describe 'CassandraOperation Unit TEST (C++ API) [Reuse Connection]' do
  before do
    @logger = DummyLogger.new
    @option = {
      :keyspace => "test",
      :api => "cxx",
      :keepalive => true
    }
    @option[:sourceDB] = "cassandra"
    @runner = CassandraRunner.new("cassandra",@logger,@option)
  end
  context " > Cassandra Operation" do
    before (:each) do
      @runner.send("DIRECT_EXECUTER","DROP KEYSPACE IF EXISTS test;")
      @runner.send("DIRECT_EXECUTER","CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
      @runner.send("DIRECT_EXECUTER","CREATE TABLE test.test (id int, value text, PRIMARY KEY(id));")
    end
    it "DIRECT_EXECUTER (insert)" do
      query = "INSERT INTO test.test (id,value) VALUES (1,'text001');"
      @runner.send("DIRECT_EXECUTER",query)
      query = "SELECT id,value FROM test.test;"
      ans = @runner.send("DIRECT_EXECUTER",query).split(",")
      ## id
      expect(ans[0]).to eq "1"
      ## value
      expect(ans[1]).to eq "text001"
    end
  end
end

