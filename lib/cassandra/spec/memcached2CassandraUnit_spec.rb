
# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/memcached2CassandraOperation"

module Memcached2CassandraOperationTester
  class ParserMock
    def exec(a,b)
      return "OK"
    end
  end
  class CassandraSchemaMock
    attr_accessor :fields
    def createQuery
      return "dummy query"
    end
  end
  class Mock
    attr_accessor :value, :raiseError, :command, :schemas
    include Memcached2CassandraOperation
    def initialize
      @parser = ParserMock.new
      @schemas = {}
      @raiseError = false
      @logger = DummyLogger.new
      @options = {
        :keyspace => "k",
        :columnfamily => "f"
      }
    end
    def DIRECT_EXECUTER(a,b=false)
      @command = a
      if(@raiseError)then
        raise ArgumentError, "Error"
      end
      return @value
    end
    def DIRECT_SELECT(a)
      @command = a
      if(@raiseError)then
        raise ArgumentError, "Error"
      end
      return @value
    end
    def change_numeric_when_numeric(str)
      return str
    end
  end

  RSpec.describe 'Memcached To CassandraOperation Unit TEST' do
    before (:all) do
      @tester = Mock.new
    end
    context "Operation" do
      it "MEMCACHED_SET (success)" do
        @tester.raiseError = false
        args = ["key00","val00"]
        expect(@tester.send(:MEMCACHED_SET, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_SET (error)" do
        @tester.raiseError = true
        args = ["key00","val00"]
        expect(@tester.send(:MEMCACHED_SET, args)).to eq false
      end
      it "MEMCACHED_GET (success)" do
        @tester.raiseError = false
        @tester.value = "v0"
        args = ["key00"]
        expect(@tester.send(:MEMCACHED_GET, args)).to eq "v0"
        command = "SELECT value FROM k.f WHERE key = 'key00';"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_GET (error)" do
        @tester.raiseError = true
        args = ["key00"]
        expect(@tester.send(:MEMCACHED_GET, args)).to eq ""
      end
      it "MEMCACHED_ADD (success)" do
        @tester.raiseError = false
        @tester.value = ""
        args = ["key00","val00"]
        expect(@tester.send(:MEMCACHED_ADD, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_ADD (exist)" do
        @tester.raiseError = false
        @tester.value = "val00"
        args = ["key00","val00"]
        expect(@tester.send(:MEMCACHED_ADD, args)).to eq true
      end
      it "MEMCACHED_REPLACE" do
        @tester.raiseError = false
        args = ["key00","val00"]
        expect(@tester.send(:MEMCACHED_REPLACE, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_GETS" do
        @tester.raiseError = false
        @tester.value = "v0"
        args = ["key00"]
        expect(@tester.send(:MEMCACHED_GETS, args)).to eq "v0"
        command = "SELECT value FROM k.f WHERE key = 'key00';"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_APPEND" do
        @tester.raiseError = false
        @tester.value = "appe"
        args = ["key00","nd"]
        expect(@tester.send(:MEMCACHED_APPEND, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','append');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_PREPEND" do
        @tester.raiseError = false
        @tester.value = "pend"
        args = ["key00","pre"]
        expect(@tester.send(:MEMCACHED_PREPEND, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','prepend');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_CAS" do
        @tester.raiseError = false
        @tester.value = "v0"
        args = ["key00","v0",""]
        expect(@tester.send(:MEMCACHED_CAS, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','v0');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_INCR" do
        @tester.raiseError = false
        @tester.value = "10"
        args = ["key00","20"]
        expect(@tester.send(:MEMCACHED_INCR, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','30');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_DECR" do
        @tester.raiseError = false
        @tester.value = "50"
        args = ["key00","20"]
        expect(@tester.send(:MEMCACHED_DECR, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','30');"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_DELETE" do
        @tester.raiseError = false
        @tester.value = true
        args = ["key00"]
        expect(@tester.send(:MEMCACHED_DELETE, args)).to eq true
        command = "DELETE FROM k.f WHERE key = 'key00'"
        expect(@tester.command).to eq command
      end
      it "MEMCACHED_FLUSH" do
        @tester.raiseError = false
        @tester.value = true
        @tester.schemas = {"k" => CassandraSchemaMock.new}
        expect(@tester.send(:MEMCACHED_FLUSH)).to eq true
      end
      it "MEMCACHED_FLUSH(error)" do
        @tester.raiseError = true
        @tester.schemas = {"k" => CassandraSchemaMock.new}
        expect(@tester.send(:MEMCACHED_FLUSH)).to eq false
      end
    end
    context "Private Method" do
      it "prepare_memcached (FLUSHALL)" do
        ans = {"operand"=>"FLUSHALL"}
        expect(@tester.send(:prepare_memcached,"FLUSHALL","OK")).to include ans
      end
      it "prepare_memcached (OTHERS)" do
        ans = {"operand"=>"MEMCACHED_OTHERS","args" => "OK"}
        expect(@tester.send(:prepare_memcached,"others","OK")).to include ans
      end
    end
  end
end
