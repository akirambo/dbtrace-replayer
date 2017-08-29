
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
    def SET(args)
      @command = "#{__method__}"
      return "OK"
    end
    def SETEX(args)
      @command = "#{__method__}"
      return "OK"
    end
    def GET(args)
      @command = "#{__method__}"
      if(args[0] == nil)then
        return nil
      end
      return "reply"
    end
    def INCRBY(args)
    @command = "#{__method__}"
      return "OK"
    end
    def DECRBY(args)
      @command = "#{__method__}"
      return "OK"
    end
    def DEL(args)
      @command = "#{__method__}"
      return "OK"
    end
    def FLUSHALL(args)
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
      it "MEMCACHED_SET with expiretime" do
        expect(@tester.send(:MEMCACHED_SET,["key",1,"value"])).to eq "OK"
        expect(@tester.command).to eq "SETEX"
      end
      it "MEMCACHED_SET" do
        expect(@tester.send(:MEMCACHED_SET,["key","value"])).to eq "OK"
        expect(@tester.command).to eq "SET"
      end
      it "MEMCACHED_SET (ERROR)" do
        expect(@tester.send(:MEMCACHED_SET,["key"])).to eq "NG"
      end
      it "MEMCACHED_GET" do
        expect(@tester.send(:MEMCACHED_GET,["key","value"])).to eq "reply"
        expect(@tester.command).to eq "GET"
      end
      it "MEMCACHED_ADD" do
        expect(@tester.send(:MEMCACHED_ADD,["key","add"])).to eq "NG"
        expect(@tester.send(:MEMCACHED_ADD,[nil,"add"])).to eq "OK"
        expect(@tester.command).to eq "SET"
      end
      it "MEMCACHED_GETS" do
        expect(@tester.send(:MEMCACHED_GETS,["key"])).to eq "reply"
        expect(@tester.command).to eq "GET"
      end
      it "MEMCACHED_CAS" do
        expect(@tester.send(:MEMCACHED_CAS,["key","value",1])).to eq  "OK"
        expect(@tester.command).to eq "SET"
        expect(@tester.send(:MEMCACHED_CAS,["key",1,"value",1])).to eq  "OK"
        expect(@tester.command).to eq "SETEX"
      end
      it "MEMCACHED_REPLACE" do
        expect(@tester.send(:MEMCACHED_REPLACE,["key","correct"])).to eq "OK"
        expect(@tester.command).to eq "SET"
        expect(@tester.send(:MEMCACHED_REPLACE,[nil,"correct"])).to eq "NG"
      end
      it "MEMCACHED_APPEND" do
        expect(@tester.send(:MEMCACHED_APPEND,["key",10,"ect"])).to eq "OK"
        expect(@tester.command).to eq "SETEX"
        expect(@tester.send(:MEMCACHED_APPEND,["key","ect"])).to eq "OK"
        expect(@tester.command).to eq "SET"
      end
      it "MEMCACHED_PREPEND" do
        expect(@tester.send(:MEMCACHED_PREPEND,["key",10,"ect"])).to eq "OK"
        expect(@tester.command).to eq "SETEX"
        expect(@tester.send(:MEMCACHED_PREPEND,["key","ect"])).to eq "OK"
        expect(@tester.command).to eq "SET"
      end
      it "MEMCACHED_INCR" do
        expect(@tester.send(:MEMCACHED_INCR,["key",100])).to eq "OK"
        expect(@tester.command).to eq "INCRBY"
      end
      it "MEMCACHED_DECR" do
        expect(@tester.send(:MEMCACHED_DECR,["key",100])).to eq "OK"
        expect(@tester.command).to eq "DECRBY"
      end
      it "MEMCACHED_DELETE" do
        expect(@tester.send(:MEMCACHED_DELETE,["key"])).to eq "OK"
        expect(@tester.command).to eq "DEL"
      end
      it "MEMCACHED_FLUSH" do
        expect(@tester.send(:MEMCACHED_FLUSH,[])).to eq "OK"
        expect(@tester.command).to eq "FLUSHALL"
      end
      it "prepare_MEMCACHED" do
        ans = {"operand" => "FLUSHALL"}
        expect(@tester.send(:prepare_MEMCACHED,"FLUSHALL",[""])).to include ans
        ans = {"operand" => "MEMCACHED_TEST", "args"=>"PARSED"}
        expect(@tester.send(:prepare_MEMCACHED,"TEST",[""])).to include ans
      end
    end
  end
end
