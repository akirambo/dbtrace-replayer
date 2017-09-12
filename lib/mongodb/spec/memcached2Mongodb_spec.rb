# -*- coding: utf-8 -*-

require "mongo"

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbRunner"
require_relative "../../common/utils"

RSpec.describe 'MongodbOperation Unit TEST' do
  before do
    @logger = DummyLogger.new
    @option = {}
    @option[:sourceDB] = "mongodb"
    @runner = MongodbRunner.new("mongodb",@logger,@option)
  end
  context " > Memcached To Mongodb Operation" do
    before (:each) do
      @runner.send("MEMCACHED_FLUSH",[])
    end
    it "MEMCACHED_SET" do
      args = ["key0",5,0]
      expect(@runner.send("MEMCACHED_SET", args)).to eq true
      args = ["key1","XX",0]
      expect(@runner.send("MEMCACHED_SET", args)).to eq true
      args = ["key2",6,5]
      expect(@runner.send("MEMCACHED_SET", args)).to eq true
      args = ["key3","XX",5]
      expect(@runner.send("MEMCACHED_SET", args)).to eq true
    end
    it "MEMCACHED_GET" do
      args = ["key0",5,0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GET", args)).to eq 5
    end
    it "MEMCACHED_ADD" do 
      args = ["key0",5,0]
      expect(@runner.send("MEMCACHED_ADD", args)).to eq true
      args = ["key0",6,0]
      expect(@runner.send("MEMCACHED_ADD", args)).to eq false
    end
    it "MEMCACHED_REPLACE" do
      args = ["key0",5,0]
      expect(@runner.send("MEMCACHED_SET", args)).to eq true
      args = ["key0",6,0]
      expect(@runner.send("MEMCACHED_REPLACE", args)).to eq true
    end
    it "MEMCACHED_GETS" do
      skip "未実装"
      args = ["key0",5,0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GETS", args)).to eq 5
    end
    it "MEMCACHED_APPEND" do
      args = ["key0","before",0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0","-->after"]
      expect(@runner.send("MEMCACHED_APPEND", args)).to eq true
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GET", args)).to eq "before-->after"
    end
    it "MEMCACHED_PREPEND" do
      args = ["key0","before",0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0","after-->"]
      expect(@runner.send("MEMCACHED_PREPEND", args)).to eq true
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GET", args)).to eq "after-->before"
    end
    it "MEMCACHED_CAS" do
      skip "未実装"
    end
    it "MEMCACHED_INCR" do
      args = ["key0",100,0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0",100]
      expect(@runner.send("MEMCACHED_INCR", args)).to eq true
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GET", args)).to eq 200
    end
    it "MEMCACHED_DECR" do
      args = ["key0",200,0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0",100]
      expect(@runner.send("MEMCACHED_DECR", args)).to eq true
      args = ["key0"]
      expect(@runner.send("MEMCACHED_GET", args)).to eq 100
    end
    it "MEMCACHED_DELETE" do
      args = ["key0",200,0]
      @runner.send("MEMCACHED_SET", args)
      args = ["key0"]
      expect(@runner.send("MEMCACHED_DELETE", args)).to eq 1
      expect(@runner.send("MEMCACHED_GET", args)).to eq nil
    end
    it "MEMCACHED_FLUSH" do
      expect(@runner.send("MEMCACHED_FLUSH", [])).to eq false
    end
  end
end

