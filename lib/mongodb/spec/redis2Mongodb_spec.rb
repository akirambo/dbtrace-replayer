# -*- coding: utf-8 -*-

require "mongo"

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbRunner"
require_relative "../../common/utils"

RSpec.describe 'MongodbOperation Unit TEST' do
  before do
    @logger = DummyLogger.new
    @options = {
      :sourceDB => "mongodb",
      :api => "ruby"
    }
    @runner = MongodbRunner.new("mongodb",@logger,@options)
  end
  context " > Redis (Key-Value) To Mongodb Operation" do
    before (:each) do
      @runner.send("REDIS_DEL",["test"])
    end
    it "REDIS_SET/REDIS_GET" do
      @runner.send("REDIS_SET",["test","correct"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq "correct"
    end
    it "REDIS_SETNX" do
      @runner.send("REDIS_SETNX",["test","correct"])
      @runner.send("REDIS_SETNX",["test","incorrect"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq "correct"
    end
    it "REDIS_SETEX" do
      skip "対応不能"
      @runner.send("REDIS_SETEX",["test","correct"])
    end
    it "REDIS_PSETEX" do
      skip "対応不能"
      @runner.send("REDIS_PSETEX",["test","correct"])
    end
    it "REDIS_MSET/REDIS_MGET" do
      args = {}
      10.times{|i|
        args["test#{i}"] = "#{i}"
      }
      @runner.send("REDIS_MSET",args)
      expect(@runner.send("REDIS_MGET",args.keys())).to eq args.values()
    end
    it "REDIS_INCR" do
      @runner.send("REDIS_SET",["test",100])
      @runner.send("REDIS_INCR",["test"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq 101
    end
    it "REDIS_INCRBY" do
      @runner.send("REDIS_SET",["test",100])
      @runner.send("REDIS_INCRBY",["test",100])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq 200
    end
    it "REDIS_DECR" do
      @runner.send("REDIS_SET",["test",100])
      @runner.send("REDIS_DECR",["test"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq 99
    end
    it "REDIS_DECRBY" do
      @runner.send("REDIS_SET",["test",100])
      @runner.send("REDIS_DECRBY",["test",100])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq 0
    end
    it "REDIS_APPEND" do
      @runner.send("REDIS_SET",["test","before"])
      @runner.send("REDIS_APPEND",["test","-->after"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq "before-->after"
    end
    it "REDIS_GETSET" do
      @runner.send("REDIS_SET",["test","before"])
      expect(@runner.send("REDIS_GETSET",["test","after"])).to eq "before"
      expect(@runner.send("REDIS_GET",["test"],false)).to eq "after"
    end
    it "REDIS_STRLEN" do
      @runner.send("REDIS_SET",["test","before"])
      expect(@runner.send("REDIS_STRLEN",["test"])).to eq 6
    end
    it "REDIS_DEL" do 
      @runner.send("REDIS_SET",["test","before"])
      @runner.send("REDIS_DEL",["test"])
      expect(@runner.send("REDIS_GET",["test"],false)).to eq ""
    end
  end
  context " > Redis (Array) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("FLUSH",[])
    end
  end
  context " > Redis (Sorted Array) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("FLUSH",[])
    end
  end
  context " > Redis (List) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("FLUSH",[])
    end
  end
  context " > Redis (Hash) Operation" do
    skip "テスト未実装"
    before (:each) do
      @runner.send("FLUSH",[])
    end
  end

end

