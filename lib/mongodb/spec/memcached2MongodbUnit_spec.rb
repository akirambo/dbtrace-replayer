# -*- coding: utf-8 -*-


require_relative "../../../spec/spec_helper"
require_relative "../src/memcached2MongodbOperation"

module Memcahed2MongodbTester
  class ParseMock
    def initialize()
    end
    def exec(op,args)
      return args
    end
  end
  class Mock
    attr_reader :command, :value
    include Memcached2MongodbOperation
    def initialize
      @logger = DummyLogger.new
      @parser = ParseMock.new
      @command = nil
      @value   = nil
      @queryReturn = nil
      @findReturn = nil
    end
    def setQueryValue(v)
      @queryReturn = v
    end
    def setFindValue(v)
      @findReturn = v
    end
    def INSERT(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def UPDATE(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def DELETE(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def DROP(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def FIND(a)
      @value = a
      @command = "#{__method__}"
      return @findReturn
    end
    def change_numeric_when_numeric(input)
      if(/^[+-]?[0-9]*[\.]?[0-9]+$/ =~ input.to_s)then
        number = input.to_i
        if(number < 2147483648 and number > -2147483648)then
          return number
        end
      end
      return input
    end
  end

  RSpec.describe 'Memcached2Mongodb  Unit TEST' do
    before do
      @tester = Mock.new
    end
    context " > Memcached To Mongodb Operation" do
      it "MEMCACHED_SET(argument size :: 2)" do
        args = ["key",5]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_SET, args)).to eq true
        expect(@tester.command).to eq "INSERT"
      end
      it "MEMCACHED_SET(argument size :: 3)" do
        args = ["key0",5,0]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_SET, args)).to eq true
        expect(@tester.command).to eq "INSERT"
      end
      it "MEMCACHED_SET(argument size is not 2 and 3 )" do
        args = ["key0",5,0,4]
        expect(@tester.send(:MEMCACHED_SET, args)).to eq false
      end
      it "MEMCACHED_GET" do
        args = ["id0"]
        ans = [{"value"=>"a"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:MEMCACHED_GET, ans)).to eq "a"
        expect(@tester.command).to eq "FIND"
      end
      it "MEMCACHED_ADD" do 
        args = ["key0",5,0]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_ADD, args)).to eq true
        expect(@tester.command).to eq "INSERT"
        args = ["key0",6,0]
        @tester.setQueryValue(false)
        expect(@tester.send(:MEMCACHED_ADD, args)).to eq false
        expect(@tester.command).to eq "INSERT"
      end
      it "MEMCACHED_REPLACE" do
        args = ["key0",6,0]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_REPLACE, args)).to eq true
        expect(@tester.command).to eq "UPDATE"
      end 
     it "MEMCACHED_GETS" do
        args = ["id0"]
        ans = [{"value"=>"a"},{"value"=>"a"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:MEMCACHED_GETS, ans)).to eq "a,a"
        expect(@tester.command).to eq "FIND"
      end
      it "MEMCACHED_APPEND" do
        args = [{"_id" => "id00"},"v"]
        ans = [{"value"=>"a"}]
        @tester.setFindValue(ans)
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_APPEND, args)).to eq true
        expect(@tester.command).to eq "UPDATE"
      end
      it "MEMCACHED_PREPEND" do
        args = [{"_id" => "id00"},"f"]
        ans = [{"value"=>"a"}]
        @tester.setFindValue(ans)
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_PREPEND, args)).to eq true
        expect(@tester.command).to eq "UPDATE"
      end
      it "MEMCACHED_CAS" do
        args = ["casid","key0",5,0]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_CAS, args)).to eq true
        expect(@tester.command).to eq "INSERT"
      end
      it "MEMCACHED_INCR" do
        args = ["key0",100]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_INCR, args)).to eq true
        expect(@tester.command).to eq "UPDATE"
      end
      it "MEMCACHED_DECR" do
        args = ["key0",100]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_DECR, args)).to eq true
        expect(@tester.command).to eq "UPDATE"
      end
      it "MEMCACHED_DELETE" do
        args = ["key0"]
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_DELETE, args)).to eq true
        expect(@tester.command).to eq "DELETE"
      end
      it "MEMCACHED_FLUSH" do
        @tester.setQueryValue(true)
        expect(@tester.send(:MEMCACHED_FLUSH, [])).to eq true
        expect(@tester.command).to eq "DROP"
      end
    end
    context " Private Method" do
      it "prepare_MEMCACHED (FLUSHALL)" do
        ans = {"operand" => "FLUSHALL"}
        expect(@tester.send(:prepare_MEMCACHED,"flushall","test_args")).to include ans
      end

      it "prepare_MEMCACHED (others)" do
        ans = {"operand" => "MEMCACHED_TEST","args" => "test_args"}
        expect(@tester.send(:prepare_MEMCACHED,"test","test_args")).to include ans
      end
    end
  end
end
