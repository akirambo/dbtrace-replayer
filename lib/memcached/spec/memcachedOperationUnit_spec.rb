# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/memcachedOperation"
require_relative "./mock"

module MemcachedOperationUnitTest 
  class Tester
    attr_accessor :metrics
    include MemcachedOperation
    def initialize
      @logger = DummyLogger.new
      @client = MemcachedUnitTest::ClientMock.new
      @parser = MemcachedUnitTest::ParserMock.new
      @option = {:async => false}
      @metrics = false
      @host   = "127.0.0.1"
    end
    ## Mock
    def connect
    end
    def close
    end
    def add_count(a)
    end
    def add_duration(a,b,c)
    end
    ## For Test
    def async
      @option[:async] = true
    end
    def sync
      @option[:async] = false
    end
    def clientQueryReturn(val)
      @client.queryReturn = val
    end
    def replyValue(val)
      @client.replyValue = val
    end
  end
  
  RSpec.describe 'MemcachedOperation Unit TEST' do
    before do
      @tester = MemcachedOperationUnitTest::Tester.new()
    end
    context "Operation" do
      it "SET (argumentSize == 2)" do
        @tester.clientQueryReturn(true)
        args = ["test","correct"]
        expect(@tester.send(:set, args)).to be true
      end
      it "SET (argumentSize == 3)" do
        @tester.clientQueryReturn(true)
        args =["test","correct",5]
        expect(@tester.send(:set, args)).to be true
      end
      it "GET (not asyncable)" do
        @tester.clientQueryReturn(true)
        @tester.replyValue("OK")
        @tester.sync
        expect(@tester.send("get",["test"])).to eq "OK"
      end
      it "GET (asyncable)" do
        @tester.async
        expect(@tester.send("get",["test"],true)).to eq ""
      end
      it "ADD" do
        @tester.clientQueryReturn(true)
        expect(@tester.send("add",["test","v"])).to be true
      end
      it "REPLACE (argumentSize == 2)" do
        @tester.clientQueryReturn(true)
        args = ["test","correct"]
        expect(@tester.send(:replace, args)).to be true
      end
      it "REPLACE (argumentSize == 3)" do
        @tester.clientQueryReturn(true)
        args =["test","correct",5]
        expect(@tester.send(:replace, args)).to be true
      end
      it "APPEND" do
        @tester.clientQueryReturn(true)
        expect(@tester.send(:append,["test","v"])).to be true
      end
      it "PREPEND" do
        @tester.clientQueryReturn(true)
        expect(@tester.send(:prepend,["test","v"])).to be true
      end
      it "CAS" do
        expect(@tester.send(:cas,["test","v"])).to be false
      end
      it "INCR" do
        @tester.clientQueryReturn(true)
        expect(@tester.send(:incr,["test",11])).to be true
      end
      it "DECR" do
        @tester.clientQueryReturn(true)
        expect(@tester.send(:decr,["test",11])).to be true
      end
      it "DELETE" do
        @tester.clientQueryReturn(true)
        expect(@tester.send(:delete,["test"])).to be true
      end
      it "FLUSH" do
        @tester.clientQueryReturn(true)
        @tester.metrics = true
        expect(@tester.send(:flush,[])).to be true
      end
      it "KEYLIST" do
        expect(@tester.send(:keylist)).to match_array ["test00","test01","test02","test03"]
      end
    end
    context  "Private Method" do
      it "prepare_memcached" do
        ans = {"operand" => "test", "args"=> "OK"}
        expect(@tester.send(:prepare_memcached,"test","arg")).to include ans
      end
    end
  end
end

