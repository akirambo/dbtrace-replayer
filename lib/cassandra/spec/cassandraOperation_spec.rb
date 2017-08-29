# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraRunner"
require_relative "../../common/utils"

RSpec.describe 'CassandraOperation Unit TEST (Ruby API)' do
  before do
    @logger = DummyLogger.new
    @options = {}
    @options[:sourceDB] = "cassandra"
    @runner = CassandraRunner.new("cassandra",@logger,@options)
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
      query = "SELECT * FROM test.test;"
      ans = @runner.send("DIRECT_EXECUTER",query)
      expect(ans["id"]).to eq [1]
      expect(ans["value"]).to eq ["text001"]
    end
    it "DIRECT_SELECT" do
      query = "INSERT INTO test.test (id,value) VALUES (1,'text001');"
      @runner.send("DIRECT_EXECUTER",query)
      query = "SELECT * FROM test.test;"
      ans = @runner.send("DIRECT_SELECT",query,false)
      expect(ans["id"]).to eq [1]
      expect(ans["value"]).to eq ["text001"]
    end
  end
end

