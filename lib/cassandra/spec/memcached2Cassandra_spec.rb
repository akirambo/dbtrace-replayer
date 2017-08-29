# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraRunner"
require_relative "../../common/utils"

RSpec.describe 'Memcached To CassandraOperation Unit TEST' do
  before (:all) do
    @logger = DummyLogger.new
    @options = {
      :keyspace     => "testdb",
      :columnfamily => "string",
      :schemaFile => "#{File.dirname(__FILE__)}/input/testSchema.schema",
      :sourceDB => "memacached",
      :api => "ruby"
    }
    @runner = CassandraRunner.new("memacached",@logger,@options)
  end
  context " > Memcached To Cassandra Operation" do
    before (:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")
    end
    it "MEMCACHED_SET" do
      args = ["key00","val00"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
    end
    it "MEMCACHED_GET" do
      args = ["key00","val00"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "val00"
    end
    it "MEMCACHED_ADD" do
      args = ["key01","val00"]
      expect(@runner.send("MEMCACHED_ADD",args)).to eq true
      args = ["key01","val00"]
      expect(@runner.send("MEMCACHED_ADD",args)).to eq false
    end
    it "MEMCACHED_REPLACE" do
      args = ["key01","incorrect"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key01","correct"]
      @runner.send("MEMCACHED_REPLACE",args)
      args = ["key01"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "correct"
    end
    it "MEMCACHED_GETS" do
      skip "仮実装"
      args = ["key00","val00"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "val00"
    end
    it "MEMCACHED_APPEND" do
      args = ["key00","before"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00",">>after"]
      expect(@runner.send("MEMCACHED_APPEND",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "before>>after"
    end
    it "MEMCACHED_PREPEND" do
      args = ["key00","before"]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00","after<<"]
      expect(@runner.send("MEMCACHED_PREPEND",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "after<<before"
    end
    it "MEMCACHED_CAS" do
      skip "未実装"
    end
    it "MEMCACHED_INCR" do
      args = ["key00",100]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00",100]
      expect(@runner.send("MEMCACHED_INCR",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "200"
    end
    it "MEMCACHED_DECR" do
      args = ["key00",100]
      expect(@runner.send("MEMCACHED_SET",args)).to eq true
      args = ["key00",100]
      expect(@runner.send("MEMCACHED_DECR",args)).to eq true
      args = ["key00"]
      expect(@runner.send("MEMCACHED_GET",args,false)).to eq "0"
    end
    it "MEMCACHED_DELETE" do
      args = ["key00"]
      @runner.send("MEMCACHED_DELETE",args)
    end
    it "MEMCACHED_FLUSH" do
      @runner.send("MEMCACHED_FLUSH",[])
    end
  end
end

