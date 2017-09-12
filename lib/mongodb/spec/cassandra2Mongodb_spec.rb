# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbRunner"

RSpec.describe 'Cassandra TO Mongodb Unit Test' do
  before do
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::FATAL
    @option = {
      :sourceDB => "cassandra",
      :keyspace => "testdb",
      :columnfamliry => "string"
    }
    @runner = MongodbRunner.new("cassandra", @logger,@option)
  end
  context ' Table Test' do
    before do
      @keyspace = "test"
      @columnfamily = "hash"
      @insertArg = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "args"  => {
          "key"    => "key00",
          "field00" => "val00",
          "field01" => "val01"
        }
      }
      @selectArg = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["field00","field01"],
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 3
      }
    end
    before(:each) do
      @runner.send("DROP",[])
    end
    it "CASSANDRA_INSERT/CASSANDRA_SELECT" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq true
      expect(@runner.send("CASSANDRA_SELECT",@selectArg)).to eq ["val00","val01"]
    end
    it "CASSANDRA_UPDATE" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq true
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "set"  => {"field00" => "valXX"},
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_UPDATE",args)).to eq true
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["field00"],
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_SELECT",@selectArg)).to eq ["valXX","val01"]
    end
    it "CASSANDRA_DELETE" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq true
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "fields"  => ["field00"],
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_DELETE",args)).to eq true
    end
  end      
end


