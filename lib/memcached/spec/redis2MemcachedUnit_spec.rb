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
    ## mock
    def set(a)
      return execQuery("#{__method__}",a)
    end
    def get(a)
      execQuery("#{__method__}",a)
      if(@getValue.class == Hash)then
        return @getValue[a[0]]
      end
      return @getValue
    end
    def add(a)
      return execQuery("#{__method__}",a)
    end
    def incr(a)
      return execQuery("#{__method__}",a)
    end
    def decr(a)
      return execQuery("#{__method__}",a)
    end
    def append(a)
      return execQuery("#{__method__}",a)
    end
    def replace(a)
      return execQuery("#{__method__}",a)
    end
    def delete(a)
      return execQuery("#{__method__}",a)
    end
    def flush(a)
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
      it "redis_set" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:redis_set, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_get" do
        @tester.getValue = "value"
        args = ["test"]
        expect(@tester.send(:redis_get, args)).to eq "value"
        expect(@tester.command).to eq "get"
      end
      it "redis_setnx" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:redis_setnx, args)).to be true
        expect(@tester.command).to eq "add"
      end
      it "redis_SETEX" do
        @tester.queryReturn = true
        args = ["test","correct"]
        expect(@tester.send(:redis_setex, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_psetex" do
        @tester.queryReturn = true
        args = ["test","correct","10000"]
        expect(@tester.send(:redis_psetex, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args[2]).to eq 11
      end
      it "redis_mset(pass case)" do
        args = [["test","correct"],["test1","correct"]]
        ## TRUE CASE
        @tester.queryReturn = true
        expect(@tester.send(:redis_mset, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_mset(fail case)" do
        ## FALSE CASE
        args = [["test","correct"],["test1","correct"]]
        @tester.queryReturn = false
        expect(@tester.send(:redis_mset, args)).to be false
        expect(@tester.command).to eq "set"
      end
      it "redis_mget" do
        args = ["k1","k2","k3"]
        @tester.getValue = "v"
        @tester.queryReturn = "v"
        expect(@tester.send(:redis_mget, args)).to match_array ["v","v","v"]
        expect(@tester.command).to eq "get"
      end
      it "redis_msetnx(pass case)" do
        args = [["test","correct"],["test1","correct"]]
        ## TRUE CASE
        @tester.queryReturn = true
        expect(@tester.send(:redis_msetnx, args)).to be true
        expect(@tester.command).to eq "add"
      end
      it "redis_msetnx(fail case)" do
        ## FALSE CASE
        args = [["test","correct"],["test1","correct"]]
        @tester.queryReturn = false
        expect(@tester.send(:redis_msetnx, args)).to be false
        expect(@tester.command).to eq "add"
      end
      it "redis_incr" do
        args = ["k:a"]
        @tester.queryReturn = true
        expect(@tester.send(:redis_incr, args)).to be true
        expect(@tester.command).to eq "incr"
      end
      it "redis_incrby" do
        args = ["k:a",2]
        @tester.queryReturn = true
        expect(@tester.send(:redis_incrby, args)).to be true
        expect(@tester.command).to eq "incr"
      end
      it "redis_decr" do
        args = ["k:a"]
        @tester.queryReturn = true
        expect(@tester.send(:redis_decr, args)).to be true
        expect(@tester.command).to eq "decr"
      end
      it "redis_decrby" do
        args = ["k:a",2]
        @tester.queryReturn = true
        expect(@tester.send(:redis_decrby, args)).to be true
        expect(@tester.command).to eq "decr"
      end
      it "redis_append" do
        args = ["k","a"]
        @tester.queryReturn = true
        expect(@tester.send(:redis_append, args)).to be true
        expect(@tester.command).to eq "append"
      end
      it "redis_getset" do
        args = ["k"]
        @tester.getValue = "value"
        expect(@tester.send(:redis_getset, args)).to eq "value"
        expect(@tester.command).to eq "replace"
      end
      it "redis_strlen" do
        args = ["k"]
        @tester.getValue = "value"
        expect(@tester.send(:redis_strlen, args)).to eq 5
        expect(@tester.command).to eq "get"
      end
      it "redis_del" do
        args = ["k"]
        @tester.queryReturn = true
        expect(@tester.send(:redis_del, args)).to be true
        expect(@tester.command).to eq "delete"
      end
    end
    context  "LISTS Operation" do
      it "redis_lpush" do
        args = ["key","v2"]
        @tester.getValue = "v1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_lpush, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args[1]).to eq "v2,v1"
      end
      it "redis_rpush" do
        args = ["key","v2"]
        @tester.getValue = "v1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_rpush, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args[1]).to eq "v1,v2"
      end
      it "redis_lpop (value size == 1)" do
        args = ["key"]
        @tester.getValue = "v1"
        expect(@tester.send(:redis_lpop, args)).to eq "v1"
        expect(@tester.command).to eq "delete"
      end
      it "redis_lpop (value size == 2)" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        @tester.queryReturn = true
        expect(@tester.send(:redis_lpop, args)).to eq "v1"
        expect(@tester.command).to eq "set"
      end
      it "redis_rpop (value size == 1)" do
        args = ["key"]
        @tester.getValue = "v1"
        expect(@tester.send(:redis_rpop, args)).to eq "v1"
        expect(@tester.command).to eq "delete"
      end
      it "redis_rpop (value size == 2)" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        @tester.queryReturn = true
        expect(@tester.send(:redis_rpop, args)).to eq "v2"
        expect(@tester.command).to eq "set"
      end
      it "redis_lrange" do
        args = ["key",0,1]
        @tester.getValue = "v1,v2,v3"
        expect(@tester.send(:redis_lrange, args)).to eq ["v1","v2"]
        expect(@tester.command).to eq "get"
      end
      it "redis_lrem" do
        args = ["key",2,"v2"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:redis_lrem, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_lindex" do
        args = ["key",2]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:redis_lindex, args)).to eq "v3"
        expect(@tester.command).to eq "get"
      end
      it "redis_rpoplpush" do
        args = ["src","dst"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:redis_rpoplpush, args)).to eq "v3"
        expect(@tester.command).to eq "set"
      end
      it "redis_lset" do
        args = ["key",2,"renew"]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:redis_lset, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args).to match_array ["key","v1,v2,renew"]
      end
      it "redis_ltrim" do
        args = ["key",1,2]
        @tester.getValue = "v1,v2,v3"
        @tester.queryReturn = true
        expect(@tester.send(:redis_ltrim, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args).to match_array ["key","v2,v3"]
      end
      it "redis_llen" do
        args = ["key"]
        @tester.getValue = "v1,v2,v3"
        expect(@tester.send(:redis_llen, args)).to eq 3
        expect(@tester.command).to eq "get"
      end
    end
    context  "SET Operation" do
      it "redis_srandmember" do
        args = ["key"]
        @tester.getValue = "v1,v1,v1"
        expect(@tester.send(:redis_srandmember, args)).to eq "v1"
        expect(@tester.command).to eq "get"
      end
      it "redis_smembers" do
        args = ["key"]
        @tester.getValue = "v1,v2"
        expect(@tester.send(:redis_smembers, args)).to match_array ["v1","v2"]
        expect(@tester.command).to eq "get"
      end
      it "redis_sdiff" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:redis_sdiff, args)).to eq "v2,v4"
        expect(@tester.command).to eq "get"
      end
      it "redis_sdiffstore" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sdiffstore, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_sinter" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:redis_sinter, args)).to eq "v1,v3"
        expect(@tester.command).to eq "get"
      end
      it "redis_sinterstore" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sinterstore, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_sunion" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        expect(@tester.send(:redis_sunion, args)).to eq "v1,v2,v3,v4"
        expect(@tester.command).to eq "get"
      end
      it "redis_sunionstore" do
        args = ["k1","k2"]
        @tester.getValue = {"k1"=>"v1,v2,v3", "k2"=>"v1,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sunionstore, args)).to be true
        expect(@tester.command).to eq "set"
      end
      it "redis_sismember" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"v1,v2,v3,target"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sismember, args)).to be true
        expect(@tester.command).to eq "get"
      end
      it "redis_srem (value size == 1)" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"v1,v2,v3,target"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_srem, args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "redis_srem (value size == 2)" do
        args = ["k1","target"]
        @tester.getValue = {"k1"=>"target"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_srem, args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "redis_smove" do
        args = ["src","dst","target"]
        @tester.getValue = {"src"=>"v1,v2,target","dst"=>"v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_smove, args)).to be true
        expect(@tester.command).to eq "replace"
      end
      it "redis_scard" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        expect(@tester.send(:redis_scard, args)).to eq 4
        expect(@tester.command).to eq "get"
      end
      it "redis_SADD (previous value size = 0)" do
        args = ["key","v5"]
        @tester.getValue = {"key"=>""}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sadd, args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args).to match_array ["key","v5"]
      end
      it "redis_sadd (previous value size > 0 )" do
        args = ["key","v5"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_sadd, args)).to be true
        expect(@tester.command).to eq "replace"
        expect(@tester.args).to match_array ["key","v1,v2,v3,v4,v5"]
      end
      it "redis_spop (previous value size == 0)" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1"}
        @tester.queryReturn = true
        expect(@tester.send(:redis_spop, args)).to eq "v1"
        expect(@tester.command).to eq "delete"
      end
      it "redis_spop (previous value size > 0)" do
        args = ["key"]
        @tester.getValue = {"key"=>"v1,v2,v3,v4"}
        @tester.queryReturn = true
        ans = @tester.send(:redis_spop, args)
        expect(["v1","v2","v3","v4"].include?(ans)).to be true
        expect(@tester.command).to eq "replace"
      end
    end
    context "Sorted SET  Operation" do
      it "redis_zadd" do
        args = ["key",100,"m0"]
        @tester.getValue = "10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zadd,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key", "10_m1,100_m0"]
        expect(@tester.args).to match_array ans
      end
      it "redis_zrem (previous member size == 1)" do
        args = ["key","m0"]
        @tester.getValue = "100_m0"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zrem,args)).to be true
        expect(@tester.command).to eq "delete"
      end 
      it "redis_zrem (previous member size > 1)" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zrem,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key","10_m1"]
        expect(@tester.args).to match_array ans
      end      
      it "redis_zincrby" do
        args = ["key",200,"m0"]
        @tester.getValue = "100_m0,10_m1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zincrby,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key","300_m0,10_m1"]
        expect(@tester.args).to match_array ans
      end      
      it "redis_zrank" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        expect(@tester.send(:redis_zrank,args)).to eq 0
      end      
      it "redis_zrevrank" do
        args = ["key","m0"]
        @tester.getValue = "100_m0,10_m1"
        expect(@tester.send(:redis_zrevrank,args)).to eq 1
      end       
      it "redis_zrange" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m0","m1"]
        expect(@tester.send(:redis_zrange,args)).to match_array ans
      end      
     it "redis_zrevrange" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m2","m1"]
        expect(@tester.send(:redis_zrevrange,args)).to match_array ans
      end
      it "redis_zrangebyscore" do
        args = ["key",45,100]
        @tester.getValue = "100_m0,50_m1,20_m2"
        ans = ["m0","m1"]
        expect(@tester.send(:redis_zrangebyscore,args)).to match_array ans
      end
      it "redis_zcount" do
        args = ["key",45,100]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:redis_zcount,args)).to eq 2
      end
      it "redis_zcard" do
        args = ["key"]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:redis_zcard,args)).to eq 3
      end
      it "redis_zscore" do
        args = ["key","m1"]
        @tester.getValue = "100_m0,50_m1,20_m2"
        expect(@tester.send(:redis_zscore,args)).to eq 50
      end
      it "redis_zremrangebyscore" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zremrangebyscore,args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args).to match_array ["100_m0,50_m1,20_m2", "key"]
      end
      it "redis_zremrangebyrank" do
        args = ["key",0,1]
        @tester.getValue = "100_m0,50_m1,20_m2"
        @tester.queryReturn = true
        expect(@tester.send(:redis_zremrangebyrank,args)).to be true
        expect(@tester.command).to eq "set"
        expect(@tester.args).to match_array ["20_m2", "key"]
      end
      it "redis_zunionstore" do 
        args = {
          "key"=>"dst", 
          "args"=>["src0","src1"],
          "option"=>{:weights => [1,2],:aggregate =>"SUM"}
        }
        @tester.queryReturn = true
        @tester.getValue = {
          "src0" =>"100_m0,50_m1,20_m2",
          "src1" =>"200_m0,150_m1,120_m5"
        }
        expect(@tester.send(:redis_zunionstore,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["500.0_m0,350.0_m1,20_m2,120_m5", "dst"]
        expect(@tester.args).to match_array ans
      end

      it "redis_zinterstore" do 
        args = {
          "key"=>"dst", 
          "args"=>["src0","src1"],
          "option"=>{:weights => [1,2],:aggregate =>"SUM"}
        }
        @tester.queryReturn = true
        @tester.getValue = {
          "src0" =>"100_m0,50_m1,20_m2",
          "src1" =>"200_m0,150_m1,120_m5"
        }
        expect(@tester.send(:redis_zinterstore,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["100_m0,50_m1,20_m2", "dst"]
        expect(@tester.args).to match_array ans
      end
    end
    context "Hash Operation" do
      it "redis_hset" do
        args = ["key","field","value"]
        @tester.queryReturn = true
        @tester.getValue = "200__H__m0,100__H__m1"
        expect(@tester.send(:redis_hset,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["200__H__m0,100__H__m1,field__H__value", "key"]
        expect(@tester.args).to match_array ans
      end
      it "redis_hget" do
        args = ["key","field","value"]
        @tester.getValue = "field__H__value,200__H__m0,100__H__m1"
        expect(@tester.send(:redis_hget,args)).to eq "value"
        expect(@tester.command).to eq "get"
      end
      it "redis_hmset" do
        args = {"key"=>"key","args"=>{"f0"=>"m0","f1"=>"m1"}}
        @tester.queryReturn = true
        @tester.getValue = "f3__H__m3,f2__H__m2"
        expect(@tester.send(:redis_hmset,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key", "f3__H__m3,f2__H__m2,f0__H__m0,f1__H__m1"]
        expect(@tester.args).to match_array ans        
      end
      it "redis_hmget" do
        args = {"key"=>"key","args"=>["f0","f1"]}
        @tester.getValue = "f0__H__m0,f1__H__m1"
        ans = ["m0","m1"]
        expect(@tester.send(:redis_hmget,args)).to match_array ans
      end
      it "redis_hincrby" do
        args = ["key","f0",20]
        @tester.queryReturn = true
        @tester.getValue = "f0__H__20"
        expect(@tester.send(:redis_hincrby,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key","f0__H__40"]
        expect(@tester.args).to match_array ans
      end
      it "redis_hexists" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:redis_hexists,args)).to be true
      end
      it "redis_hdel(previous data size == 1)" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0"
        @tester.queryReturn = true
        expect(@tester.send(:redis_hdel,args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "redis_hdel(previous data size > 1)" do
        args = ["key","f0"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        @tester.queryReturn = true
        expect(@tester.send(:redis_hdel,args)).to be true
        expect(@tester.command).to eq "set"
        ans = ["key","f1__H__m1"]
        expect(@tester.args).to match_array ans
      end
      it "redis_hlen" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:redis_hlen,args)).to eq 2
      end
      it "redis_hkeys" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:redis_hkeys,args)).to match_array ["f0","f1"]
      end
      it "redis_hvals" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        expect(@tester.send(:redis_hvals,args)).to match_array ["m0","m1"]
      end
      it "redis_hgetall" do
        args = ["key"]
        @tester.getValue = "f0__H__m0,f1__H__m1"
        ans = {"f0"=>"m0","f1"=>"m1"}
        expect(@tester.send(:redis_hgetall,args)).to include ans
      end
      it "redis_flushall" do
        args = ["key"]
        @tester.queryReturn = true
        expect(@tester.send(:redis_flushall,args)).to be true
      end
    end
    context  "Private Method" do
      it "prepare_redis" do
        ans = {"operand"=>"redis_zunionstore","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"zunionstore","")).to include ans
        ans = {"operand"=>"redis_mset","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"mset","")).to include ans
        ans = {"operand"=>"redis_hmget","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"hmget","")).to include ans
        ans = {"operand"=>"redis_hmset","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"hmset","")).to include ans
        ans = {"operand"=>"redis_other","args"=>"OK"}
        expect(@tester.send(:prepare_redis,"other","OK")).to include ans
      end
      it "sortedArrayGetRange" do
        args = ["v0","v1","v2","v3","v4"]
        expect(@tester.send(:sorted_array_get_range,-1,-1,args)).to match_array args
        expect(@tester.send(:sorted_array_get_range,1,2,args)).to match_array ["v1","v2"]
      end
      it "aggregateScore" do
        expect(@tester.send(:aggregate_score,"SUM",10,20,2)).to eq 50
        expect(@tester.send(:aggregate_score,"MAX",10,20,2)).to eq 40
        expect(@tester.send(:aggregate_score,"MIN",10,20,2)).to eq 10
        expect(@tester.send(:aggregate_score,"ERROR",10,20,2)).to eq 50
      end
    end
  end
end

