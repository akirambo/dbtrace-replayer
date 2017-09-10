# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redis2MemcachedOperation"
require_relative "./mock"

module MemcachedOperationUnitTest 
  class Mock
    attr_reader :command, :args
    attr_accessor :queryReturn, :getValue
    include Redis2MemcachedOperation
    def initialize
      @parser = MemcachedUnitTest::ParserMock.new
      @logger = DummyLogger.new
      @queryReturn = false
      @getValue = nil
      @args = nil
    end
    ## Mock
    def SET(a)
      return execQuery("#{__method__}",a)
    end
    def GET(a)
      execQuery("#{__method__}",a)
      if(@getValue.class == Hash)then
        return @getValue[a[0]]
      end
      return @getValue
    end
    def ADD(a)
      return execQuery("#{__method__}",a)
    end
    def INCR(a)
      return execQuery("#{__method__}",a)
    end
    def DECR(a)
      return execQuery("#{__method__}",a)
    end
    def APPEND(a)
      return execQuery("#{__method__}",a)
    end
    def REPLACE(a)
      return execQuery("#{__method__}",a)
    end
    def DELETE(a)
      return execQuery("#{__method__}",a)
    end
    def FLUSH(a)
      return execQuery("#{__method__}",a)
    end
    def execQuery(operand,args)
      @args = args
      @command = operand
      return @queryReturn 
    end
  end
  RSpec.describe 'MemcachedOperation Unit TEST' do
    before do
      @tester = Mock.new
    end
    context "String Operation" do
      it "REDIS_SET" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:REDIS_SET, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_GET" do
        @tester.getValue = "value"
        args = ["test"]
        expect(@tester.send(:REDIS_GET, args)).to eq "value"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SETNX" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:REDIS_SETNX, args)).to be true
        expect(@tester.command).to eq "ADD"
      end
      it "REDIS_SETEX" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:REDIS_SETEX, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_PSETEX" do
        @tester.queryReturn = true
        args = ["test","correct","10000"]
        expect(@tester.send(:REDIS_PSETEX, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args[2]).to eq 11
      end
      it "REDIS_MSET(pass case)" do
        args = [["test","correct"],["test1","correct"]]
        ## TRUE CASE
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_MSET, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_MSET(fail case)" do
        ## FALSE CASE
        args = [["test","correct"],["test1","correct"]]
        @tester.queryReturn = false
        expect(@tester.send(:REDIS_MSET, args)).to be false
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_MGET" do
        args = ["k1","k2","k3"]
        @tester.getValue = "v"
        @tester.queryReturn = "v"
        expect(@tester.send(:REDIS_MGET, args)).to match_array ["v","v","v"]
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_MSETNX(pass case)" do
        args = [["test","correct"],["test1","correct"]]
        ## TRUE CASE
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_MSETNX, args)).to be true
        expect(@tester.command).to eq "ADD"
      end
      it "REDIS_MSETNX(fail case)" do
        ## FALSE CASE
        args = [["test","correct"],["test1","correct"]]
        @tester.queryReturn = false
        expect(@tester.send(:REDIS_MSETNX, args)).to be false
        expect(@tester.command).to eq "ADD"
      end
      it "REDIS_INCR" do
        args = ["k:a"]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_INCR, args)).to be true
        expect(@tester.command).to eq "INCR"
      end
      it "REDIS_INCRBY" do
        args = ["k:a",2]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_INCRBY, args)).to be true
        expect(@tester.command).to eq "INCR"
      end
      it "REDIS_DECR" do
        args = ["k:a"]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_DECR, args)).to be true
        expect(@tester.command).to eq "DECR"
      end
      it "REDIS_DECRBY" do
        args = ["k:a",2]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_DECRBY, args)).to be true
        expect(@tester.command).to eq "DECR"
      end
      it "REDIS_APPEND" do
        args = ["k","a"]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_APPEND, args)).to be true
        expect(@tester.command).to eq "APPEND"
      end
      it "REDIS_GETSET" do
        args = ["k"]
        @tester.getValue = "value"
        expect(@tester.send(:REDIS_GETSET, args)).to eq "value"
        expect(@tester.command).to eq "REPLACE"
      end
      it "REDIS_STRLEN" do
        args = ["k"]
        @tester.getValue = "value"
        expect(@tester.send(:REDIS_STRLEN, args)).to eq 5
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_DEL" do
        args = ["k"]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_DEL, args)).to be true
        expect(@tester.command).to eq "DELETE"
      end
    end
    context  "LISTS Operation" do
      it "REDIS_LPUSH" do
        args = ["key","v2"]
        @tester.getValue = "v1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LPUSH, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args[1]).to eq "v2,v1"
      end
      it "REDIS_RPUSH" do
        args = ["key","v2"]
        @tester.getValue = "v1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_RPUSH, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args[1]).to eq "v1,v2"
      end
      it "REDIS_LPOP (value size == 1)" do
        args = ["key"]
        @tester.getValue = "v1"
        expect(@tester.send(:REDIS_LPOP, args)).to eq "v1"
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_LPOP (value size == 2)" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LPOP, args)).to eq "v1"
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_RPOP (value size == 1)" do
        args = ["key"]
        @tester.getValue = "v1"
        expect(@tester.send(:REDIS_RPOP, args)).to eq "v1"
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_RPOP (value size == 2)" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_RPOP, args)).to eq "v2"
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_LRANGE" do
        args = ["key",0,1]
        @tester.getValue = "v1,v2,v3"
        expect(@tester.send(:REDIS_LRANGE, args)).to eq ["v1","v2"]
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_LREM" do
        args = ["key",2,"v2"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LREM, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_LINDEX" do
        args = ["key",2]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LINDEX, args)).to eq "v3"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_RPOPLPUSH" do
        args = ["src","dst"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_RPOPLPUSH, args)).to eq "v3"
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_LSET" do
        args = ["key",2,"renew"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LSET, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args).to match_array ["key","v1,v2,renew"]
      end
      it "REDIS_LTRIM" do
        args = ["key",1,2]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_LTRIM, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args).to match_array ["key","v2,v3"]
      end
      it "REDIS_LLEN" do
        args = ["key"]
        @tester.getValue = "v1,v2,v3"
        expect(@tester.send(:REDIS_LLEN, args)).to eq 3
        expect(@tester.command).to eq "GET"
      end
    end
    context  "SET Operation" do
      it "REDIS_SRANDMEMBER" do
        args = ["key"]
        @tester.getValue = "v1,v1,v1"
        expect(@tester.send(:REDIS_SRANDMEMBER, args)).to eq "v1"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SMEMBERS" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        expect(@tester.send(:REDIS_SMEMBERS, args)).to match_array ["v1","v2"]
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SDIFF" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:REDIS_SDIFF, args)).to eq "v2,v4"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SDIFFSTORE" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SDIFFSTORE, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_SINTER" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:REDIS_SINTER, args)).to eq "v1,v3"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SINTERSTORE" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SINTERSTORE, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_SUNION" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:REDIS_SUNION, args)).to eq "v1,v2,v3,v4"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SUNIONSTORE" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SUNIONSTORE, args)).to be true
        expect(@tester.command).to eq "SET"
      end
      it "REDIS_SISMEMBER" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"v1,v2,v3,target"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SISMEMBER, args)).to be true
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SREM (value size == 1)" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"v1,v2,v3,target"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SREM, args)).to be true
        expect(@tester.command).to eq "REPLACE"
      end
      it "REDIS_SREM (value size == 2)" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"target"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SREM, args)).to be true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_SMOVE" do
        args = ["src","dst","target"]
        @tester.getValue = {"src"=>"v1,v2,target","dst"=>"v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SMOVE, args)).to be true
        expect(@tester.command).to eq "REPLACE"
      end
      it "REDIS_SCARD" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        expect(@tester.send(:REDIS_SCARD, args)).to eq 4
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_SADD (previous value size = 0)" do
        args = ["key","v5"]
        @tester.getValue = {"key"=>""}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SADD, args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args).to match_array ["key","v5"]
      end
      it "REDIS_SADD (previous value size > 0 )" do
        args = ["key","v5"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SADD, args)).to be true
        expect(@tester.command).to eq "REPLACE"
        expect(@tester.args).to match_array ["key","v1,v2,v3,v4,v5"]
      end
      it "REDIS_SPOP (previous value size == 0)" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1"}
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_SPOP, args)).to eq "v1"
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_SPOP (previous value size > 0)" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        @tester.queryReturn = true
        ans = @tester.send(:REDIS_SPOP, args)
        expect(["v1","v2","v3","v4"].include?(ans)).to be true
        expect(@tester.command).to eq "REPLACE"
      end
    end
    context "Sorted SET  Operation" do
      it "REDIS_ZADD" do
        args = ["key",100,"m0"]
        @tester.getValue = "10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZADD,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key", "10_m1,100_m0"]
        expect(@tester.args).to match_array ans
      end
      it "REDIS_ZREM (previous member size == 1)" do
        args = ["key","m0"]
        @tester.getValue = "100_m0"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZREM,args)).to be true
        expect(@tester.command).to eq "DELETE"
      end      
      it "REDIS_ZREM (previous member size > 1)" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZREM,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key","10_m1"]
        expect(@tester.args).to match_array ans
      end      
      it "REDIS_ZINCRBY" do
        args = ["key",200,"m0"]
        @tester.getValue = "100_m0,10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZINCRBY,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key","300_m0,10_m1"]
        expect(@tester.args).to match_array ans
      end      
      it "REDIS_ZRANK" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        expect(@tester.send(:REDIS_ZRANK,args)).to eq 0
      end      
      it "REDIS_ZREVRANK" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        expect(@tester.send(:REDIS_ZREVRANK,args)).to eq 1
      end       
      it "REDIS_ZRANGE" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m0","m1"]
        expect(@tester.send(:REDIS_ZRANGE,args)).to match_array ans
      end      
     it "REDIS_ZREVRANGE" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m2","m1"]
        expect(@tester.send(:REDIS_ZREVRANGE,args)).to match_array ans
      end
      it "REDIS_ZRANGEBYSCORE" do
        args = ["key",45,100]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m0","m1"]
        expect(@tester.send(:REDIS_ZRANGEBYSCORE,args)).to match_array ans
      end
      it "REDIS_ZCOUNT" do
        args = ["key",45,100]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:REDIS_ZCOUNT,args)).to eq 2
      end
      it "REDIS_ZCARD" do
        args = ["key"]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:REDIS_ZCARD,args)).to eq 3
      end
      it "REDIS_ZSCORE" do
        args = ["key","m1"]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:REDIS_ZSCORE,args)).to eq 50
      end
      it "REDIS_ZREMRANGEBYSCORE" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZREMRANGEBYSCORE,args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args).to match_array ["100_m0,50_m1,20_m2", "key"]
      end
      it "REDIS_ZREMRANGEBYRANK" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_ZREMRANGEBYRANK,args)).to be true
        expect(@tester.command).to eq "SET"
        expect(@tester.args).to match_array ["20_m2", "key"]
      end
      it "REDIS_ZUNIONSTORE" do 
        args = {
          "key"=>"dst", 
          "args"=>["src0","src1"],
          "options"=>{:weights => [1,2],:aggregate =>"SUM"}
        }
        @tester.queryReturn = true
        @tester.getValue = {
          "src0" =>"100_m0,50_m1,20_m2",
          "src1" =>"200_m0,150_m1,120_m5"
        }
        expect(@tester.send(:REDIS_ZUNIONSTORE,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["500.0_m0,350.0_m1,20_m2,120_m5", "dst"]
        expect(@tester.args).to match_array ans
      end

      it "REDIS_ZINTERSTORE" do 
        args = {
          "key"=>"dst", 
          "args"=>["src0","src1"],
          "options"=>{:weights => [1,2],:aggregate =>"SUM"}
        }
        @tester.queryReturn = true
        @tester.getValue = {
          "src0" =>"100_m0,50_m1,20_m2",
          "src1" =>"200_m0,150_m1,120_m5"
        }
        expect(@tester.send(:REDIS_ZINTERSTORE,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["100_m0,50_m1,20_m2", "dst"]
        expect(@tester.args).to match_array ans
      end
    end
    context "Hash Operation" do
      it "REDIS_HSET" do
        args = ["key","field","value"]
        @tester.queryReturn = true
        @tester.getValue = "200__H__m0,100__H__m1"
        expect(@tester.send(:REDIS_HSET,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["200__H__m0,100__H__m1,field__H__value", "key"]
        expect(@tester.args).to match_array ans
      end
      it "REDIS_HGET" do
        args = ["key","field","value"]
        @tester.getValue = "field__H__value,200__H__m0,100__H__m1"
        expect(@tester.send(:REDIS_HGET,args)).to eq "value"
        expect(@tester.command).to eq "GET"
      end
      it "REDIS_HMSET" do
        args = {"key"=>"key","args"=>{"f0"=>"m0","f1"=>"m1"}}
        @tester.queryReturn = true
        @tester.getValue = "f3__H__m3,f2__H__m2"
        expect(@tester.send(:REDIS_HMSET,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key", "f3__H__m3,f2__H__m2,f0__H__m0,f1__H__m1"]
        expect(@tester.args).to match_array ans        
      end
      it "REDIS_HMGET" do
        args = {"key"=>"key","args"=>["f0","f1"]}
        @tester.getValue = "f0__H__m0,f1__H__m1"
        ans = ["m0","m1"]
        expect(@tester.send(:REDIS_HMGET,args)).to match_array ans
      end
      it "REDIS_HINCRBY" do
        args = ["key","f0",20]
        @tester.queryReturn = true
        @tester.getValue = "f0__H__20"
        expect(@tester.send(:REDIS_HINCRBY,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key","f0__H__40"]
        expect(@tester.args).to match_array ans
      end
      it "REDIS_HEXISTS" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:REDIS_HEXISTS,args)).to be true
      end
      it "REDIS_HDEL(previous data size == 1)" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_HDEL,args)).to be true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_HDEL(previous data size > 1)" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_HDEL,args)).to be true
        expect(@tester.command).to eq "SET"
        ans = ["key","f1__H__m1"]
        expect(@tester.args).to match_array ans
      end
      it "REDIS_HLEN" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:REDIS_HLEN,args)).to eq 2
      end
      it "REDIS_HKEYS" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:REDIS_HKEYS,args)).to match_array ["f0","f1"]
      end
      it "REDIS_HVALS" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:REDIS_HVALS,args)).to match_array ["m0","m1"]
      end
      it "REDIS_HGETALL" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        ans = {"f0"=>"m0","f1"=>"m1"}
        expect(@tester.send(:REDIS_HGETALL,args)).to include ans
      end
      it "REDIS_FLUSHALL" do
        args = ["key"]
        @tester.queryReturn = true
        expect(@tester.send(:REDIS_FLUSHALL,args)).to be true
      end
    end
    context  "Private Method" do
      it "prepare_redis" do
        ans = {"operand"=>"REDIS_ZUNIONSTORE","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"ZUNIONSTORE","")).to include ans
        ans = {"operand"=>"REDIS_MSET","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"MSET","")).to include ans
        ans = {"operand"=>"REDIS_HMGET","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"HMGET","")).to include ans
        ans = {"operand"=>"REDIS_HMSET","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"HMSET","")).to include ans
        ans = {"operand"=>"REDIS_OTHER","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"OTHER","OK")).to include ans
      end
      it "sortedArrayGetRange" do
        args = ["v0","v1","v2","v3","v4"]
        expect(@tester.send(:sortedArrayGetRange,-1,-1,args)).to match_array args
        expect(@tester.send(:sortedArrayGetRange,1,2,args)).to match_array ["v1","v2"]
      end
      it "aggregateScore" do
        expect(@tester.send(:aggregateScore,"SUM",10,20,2)).to eq 50
        expect(@tester.send(:aggregateScore,"MAX",10,20,2)).to eq 40
        expect(@tester.send(:aggregateScore,"MIN",10,20,2)).to eq 10
        expect(@tester.send(:aggregateScore,"ERROR",10,20,2)).to eq 50
      end
    end
  end
end

