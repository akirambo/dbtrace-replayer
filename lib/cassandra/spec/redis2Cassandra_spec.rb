# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraRunner"
require_relative "../../common/utils"

RSpec.describe 'Redis To CassandraOperation Unit TEST' do
  before (:all) do
    @logger = DummyLogger.new
    @options = {
      :keyspace     => "testdb",
      :columnfamily => "string",
      :schemaFile => "#{File.dirname(__FILE__)}/input/testSchema.schema",
      :sourceDB => "redis"
    }

    @options[:sourceDB] = "cassandra"
    @runner = CassandraRunner.new("cassandra",@logger,@options)
  end
  context " > Redis To Cassandra Operation" do
    before (:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")
    end
    it "REDIS_SET" do
      args = ["key00","val00"]
      expect(@runner.send("REDIS_SET", args)).to eq true
    end
    it "REDIS_GET" do
      args = ["key00","val00"]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_GET", args,false)).to eq "val00"
    end
    it "REDIS_SETNX" do
      args = ["key00","val00"]
      expect(@runner.send("REDIS_SETNX", args)).to eq true
      args = ["key00","val01"]
      expect(@runner.send("REDIS_SETNX", args)).to eq false
    end
    it "REDIS_SETEX" do
      args = ["key00","val00", 1]
      expect(@runner.send("REDIS_SETEX", args)).to eq true
    end
    it "REDIS_PSETEX" do
      args = ["key00","val00", 1000]
      expect(@runner.send("REDIS_PSETEX", args)).to eq true
    end
    it "REDIS_MSET/REDIS_MGET" do
      args = {}
      10.times{|i|
        args["key#{i}"] = "val#{i}"
      }
      expect(@runner.send("REDIS_MSET", args)).to eq true
      expect(@runner.send("REDIS_MGET", args.keys())).to eq args.values()
    end
    it "REDIS_INCR" do
      args = ["key00", 100]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_INCR", args)).to eq true
      expect(@runner.send("REDIS_GET", args,false)).to eq "101"
    end
    it "REDIS_INCRBY" do
      args = ["key00", 100]
      expect(@runner.send("REDIS_SET", args)).to eq true
      expect(@runner.send("REDIS_INCRBY", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_GET", args,false)).to eq "200"
    end
    it "REDIS_DECR" do
      args = ["key00", 100]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_DECR", args)).to eq true
      expect(@runner.send("REDIS_GET", args,false)).to eq "99"
    end
    it "REDIS_DECRBY" do
      args = ["key00", 100]
      expect(@runner.send("REDIS_SET", args)).to eq true
      expect(@runner.send("REDIS_DECRBY", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_GET", args,false)).to eq "0"
    end
    it "REDIS_APPEND" do
      args = ["key00", "before"]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00", ">>after"]
      expect(@runner.send("REDIS_APPEND", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_GET", args,false)).to eq "before>>after"
    end
    it "REDIS_GETSET" do
      args = ["key00", "before"]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00", "after"]
      expect(@runner.send("REDIS_GETSET", args)).to eq "before"
      args = ["key00"]
      expect(@runner.send("REDIS_GET", args,false)).to eq "after"
    end
    it "REDIS_STRLEN" do
      args = ["key00", "text"]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_STRLEN", args)).to eq 4
    end
    it "REDIS_DEL" do 
      args = ["key00"]
      expect(@runner.send("REDIS_SET", args)).to eq true
      args = ["key00"]
      expect(@runner.send("REDIS_DEL", args)).to eq true
    end
  end
  context " > Redis (Array) Operation" do
  end
  context " > Redis (Sorted Array) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")
    end
  end
  context " > Redis (List) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")
    end
  end
  context " > Redis (Hash) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")
    end
  end
end

