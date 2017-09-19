
require_relative "../../../spec/spec_helper"
require_relative "../src/memcached2RedisOperation"

module MemcachedTest
  class ParserMock
    def initialize
    end
    def exec(a,b)
      return "PARSED"
    end
  end
  
  class Mock
    attr_reader :command
    include Memcached2RedisOperation
    def initialize(logger)
      @logger = logger
      @command = ""
      @parser = ParserMock.new()
    end
    private
    def set(argsOB)
      @command = "#{__method__}"
      return "OK"
    end
    def setex(args)
      @command = "#{__method__}"
      return "OK"
    end
    def get(args)
      @command = "#{__method__}"
      if(args[0] == nil)then
        return nil
      end
      return "reply"
    end
    def incrby(args)
    @command = "#{__method__}"
      return "OK"
    end
    def decrby(args)
      @command = "#{__method__}"
      return "OK"
    end
    def del(args)
      @command = "#{__method__}"
      return "OK"
    end
    def flushall(args)
      @command = "#{__method__}"
      return "OK"
    end
  end
  
  
  RSpec.describe 'Memcached TO Redis Unit Test' do
    before do
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::FATAL
      @tester = Mock.new(@logger)
    end
    context 'Operation' do
      it "memcached_set with expiretime" do
        expect(@tester.send(:memcached_set,["key",1,"value"])).to eq "OK"
        expect(@tester.command).to eq "setex"
      end
      it "memcached_set" do
        expect(@tester.send(:memcached_set,["key","value"])).to eq "OK"
        expect(@tester.command).to eq "set"
      end
      it "memcached_set (error)" do
        expect(@tester.send(:memcached_set,["key"])).to eq "NG"
      end
      it "memcached_get" do
        expect(@tester.send(:memcached_get,["key","value"])).to eq "reply"
        expect(@tester.command).to eq "get"
      end
      it "memcached_add" do
        expect(@tester.send(:memcached_add,["key","add"])).to eq "NG"
        expect(@tester.send(:memcached_add,[nil,"add"])).to eq "OK"
        expect(@tester.command).to eq "set"
      end
      it "memcached_gets" do
        expect(@tester.send(:memcached_gets,["key"])).to eq "reply"
        expect(@tester.command).to eq "get"
      end
      it "memcached_cas" do
        expect(@tester.send(:memcached_cas,["key","value",1])).to eq  "OK"
        expect(@tester.command).to eq "set"
        expect(@tester.send(:memcached_cas,["key",1,"value",1])).to eq  "OK"
        expect(@tester.command).to eq "setex"
      end
      it "memcached_replace" do
        expect(@tester.send(:memcached_replace,["key","correct"])).to eq "OK"
        expect(@tester.command).to eq "set"
        expect(@tester.send(:memcached_replace,[nil,"correct"])).to eq "NG"
      end
      it "memcached_append" do
        expect(@tester.send(:memcached_append,["key",10,"ect"])).to eq "OK"
        expect(@tester.command).to eq "setex"
        expect(@tester.send(:memcached_append,["key","ect"])).to eq "OK"
        expect(@tester.command).to eq "set"
      end
      it "memcached_prepend" do
        expect(@tester.send(:memcached_prepend,["key",10,"ect"])).to eq "OK"
        expect(@tester.command).to eq "setex"
        expect(@tester.send(:memcached_prepend,["key","ect"])).to eq "OK"
        expect(@tester.command).to eq "set"
      end
      it "memcached_incr" do
        expect(@tester.send(:memcached_incr,["key",100])).to eq "OK"
        expect(@tester.command).to eq "incrby"
      end
      it "memcached_decr" do
        expect(@tester.send(:memcached_decr,["key",100])).to eq "OK"
        expect(@tester.command).to eq "decrby"
      end
      it "memcached_delete" do
        expect(@tester.send(:memcached_delete,["key"])).to eq "OK"
        expect(@tester.command).to eq "del"
      end
      it "memcached_flush" do
        expect(@tester.send(:memcached_flush,[])).to eq "OK"
        expect(@tester.command).to eq "flushall"
      end
      it "prepare_memcached" do
        ans = {"operand" => "flushall"}
        expect(@tester.send(:prepare_memcached,"flushall",[""])).to include ans
        ans = {"operand" => "memcached_test", "args"=>"PARSED"}
        expect(@tester.send(:prepare_memcached,"test",[""])).to include ans
      end
    end
  end
end
