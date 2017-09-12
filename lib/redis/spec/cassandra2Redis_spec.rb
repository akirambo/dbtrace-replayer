# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redisRunner"

RSpec.describe 'Cassandra TO Redis Test' do
  before do
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::FATAL
    @option = {
      :sourceDB => "cassandra",
    }
    @runner = RedisRunner.new("cassandra", @logger,@option)
    @runner.send("FLUSHALL",[])
  end
  context 'Field Size == 1 => String' do
    before do
      @keyspace = "test"
      @columnfamily = "string"
      @insertArg =  {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "args"  => {
          "key"    => "key00",
          "field00" => "val00" 
        },
        "schema_fields" => 2
      }
      @selectArg = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["field00"],
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 2
      }
      @dropTable = {
        "type" => "table",
        "key"  => @columnfamily
      }
      
      @dropKeyspace = {
        "type" => "keyspace",
        "key"  => @keyspace
      }
    end
    before(:each) do
      @runner.send("FLUSHALL",[])
    end
    it "CASSANDRA_INSERT/CASSANDRA_SELECT" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      expect(@runner.send("CASSANDRA_SELECT",@selectArg)).to eq ["val00"]
    end
    it "CASSANDRA_UPDATE" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "set"  => {"field00" => "valXX"},
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 2
      }
      expect(@runner.send("CASSANDRA_UPDATE",args)).to eq "OK"
      expect(@runner.send("CASSANDRA_SELECT",@selectArg)).to eq ["valXX"]
    end
    it "CASSANDRA_DELETE" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["*"],
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 2
      }
      expect(@runner.send("CASSANDRA_DELETE",args)).to eq 0
    end
    it "CASSANDRA_DROP" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      expect(@runner.send("CASSANDRA_DROP",@dropTable)).to eq 1
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      expect(@runner.send("CASSANDRA_DROP",@dropKeyspace)).to eq 1
    end
  end      
  context 'Field Size > 1 => Hash' do
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
      @runner.send("FLUSHALL",[])
    end
    it "CASSANDRA_INSERT/CASSANDRA_SELECT" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      expect(@runner.send("CASSANDRA_SELECT",@selectArg)).to eq ["val00","val01"]
    end
    it "CASSANDRA_UPDATE" do
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "set"  => {"field00" => "valXX"},
        "cond_keys"   => ["key"],
        "cond_values" => ["key00"],
        "limit"   => nil,
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_UPDATE",args)).to eq "OK"
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
      expect(@runner.send("CASSANDRA_INSERT",@insertArg)).to eq "OK"
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["field00"],
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_DELETE",args)).to eq 0
      args = {
        "table" => "#{@keyspace}.#{@columnfamily}",
        "primaryKey" => "key",
        "fields"  => ["*"],
        "schema_fields" => 3
      }
      expect(@runner.send("CASSANDRA_DELETE",args)).to eq 0
    end
  end      
end
