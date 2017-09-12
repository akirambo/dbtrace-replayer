# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redis2CassandraOperation"

module Redis2CassandraOperationTester
    class ParserMock
    def exec(a,b)
      return "OK"
    end
    def extractZ_X_STORE_ARGS(a)
      return "OK"
    end
    def args2hash(a)
      return "OK"
    end
    def args2key_args(a)
      return "OK"
    end
    def args2key_hash(a)
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
    include Redis2CassandraOperation
    def initialize
      @schemas = {}
      @raiseError = false
      @parser = ParserMock.new
      @logger = DummyLogger.new
      @option = {
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
  end

  RSpec.describe 'Redis To CassandraOperation Unit TEST' do
    before (:all) do
      @tester = Mock.new
    end
    context "String Operation" do
      it "REDIS_SET (success)" do
        @tester.raiseError = false
        args = ["key00","val00"]
        expect(@tester.send(:REDIS_SET, args,{"ttl"=>1})).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00') USING TTL 1;"
        expect(@tester.command).to eq command
      end
      it "REDIS_SET (error)" do
        @tester.raiseError = true
        args = ["key00","val00",12]
        expect(@tester.send(:REDIS_SET, args)).to eq false
      end
      it "REDIS_GET (success)" do
        @tester.value = {"value" => ["a"]}
        @tester.raiseError = false
        command = "SELECT value FROM k.f WHERE key = 'key' ;"
        expect(@tester.send(:REDIS_GET,["key"],false)).to eq "a"
        expect(@tester.command).to eq command                
      end
      it "REDIS_GET (error)" do
        @tester.value = {"value" => ["a"]}
        @tester.raiseError = true
        expect(@tester.send(:REDIS_GET,["key"],false)).to eq ""
      end
      it "REDIS_SETNX (there is one data)" do
        args = ["key00","val00"]
        @tester.value = {"value" => ["a"]}
        @tester.raiseError = false
        expect(@tester.send(:REDIS_SETNX, args)).to eq false
      end
      it "REDIS_SETNX (there is NOT one data)" do
        args = ["key00","val01"]
        @tester.value = {}
        @tester.raiseError = false
        expect(@tester.send(:REDIS_SETNX, args)).to eq true
      end
      it "REDIS_SETEX" do
        @tester.value = {}
        @tester.raiseError = false
        args = ["key00","val00", 1]
        expect(@tester.send(:REDIS_SETEX, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00') USING TTL 1;"
        expect(@tester.command).to eq command                
      end
      it "REDIS_PSETEX" do
        @tester.value = {}
        @tester.raiseError = false
        args = ["key00","val00", 1000]
        expect(@tester.send(:REDIS_PSETEX, args)).to eq true
        command = "INSERT INTO k.f (key,value) VALUES ('key00','val00') USING TTL 1;"
        expect(@tester.command).to eq command                
      end
      it "REDIS_MSET (success)" do 
        @tester.value = {}
        @tester.raiseError = false
        args = {}
        10.times{|i|
          args["key#{i}"] = "val#{i}"
        }
        expect(@tester.send(:REDIS_MSET, args)).to eq true
      end
      it "REDIS_MSET (error)" do 
        @tester.value = {}
        @tester.raiseError = true
        args = {}
        10.times{|i|
          args["key#{i}"] = "val#{i}"
        }
        expect(@tester.send(:REDIS_MSET, args)).to eq false
      end
      it "REDIS_MGET" do
        @tester.value = {"value" => ["v0"]}
        @tester.raiseError = false
        args = ["k0","k1"]
        expect(@tester.send(:REDIS_MGET, args)).to match_array ["v0","v0"]
      end
      it "REDIS_MSETNX (success)" do 
        @tester.value = {}
        @tester.raiseError = false
        args = {"k0"=>"v0","k1"=>"v1"}
        expect(@tester.send(:REDIS_MSETNX, args)).to eq true
      end
      it "REDIS_MSETNX (error)" do 
        @tester.value = {}
        @tester.raiseError = true
        args = {"k0"=>"v0","k1"=>"v1"}
        expect(@tester.send(:REDIS_MSETNX, args)).to eq false
      end
      it "REDIS_INCR" do
        args = ["key00"]
        @tester.raiseError = false
        @tester.value = {"value" =>[10]}
        command = "INSERT INTO k.f (key,value) VALUES ('key00','11');"
        expect(@tester.send(:REDIS_INCR, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_INCRBY" do
        args = ["key00", 100]
        @tester.value = {"value" =>[10]}
        @tester.raiseError = false
        command = "INSERT INTO k.f (key,value) VALUES ('key00','110');"
        expect(@tester.send(:REDIS_INCRBY, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_DECR" do
        args = ["key00"]
        @tester.raiseError = false
        @tester.value = {"value" =>[12]}
        command = "INSERT INTO k.f (key,value) VALUES ('key00','11');"
        expect(@tester.send(:REDIS_DECR, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_DECRBY" do
        args = ["key00", 100]
        @tester.raiseError = false
        @tester.value = {"value" =>[100]}
        command = "INSERT INTO k.f (key,value) VALUES ('key00','0');"
        expect(@tester.send(:REDIS_DECRBY, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_APPEND" do
        @tester.raiseError = false
        @tester.value = {"value" =>["before"]}
        args = ["key00", ">>after"]
        command = "INSERT INTO k.f (key,value) VALUES ('key00','before>>after');"
        expect(@tester.send(:REDIS_APPEND, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_GETSET" do
        @tester.raiseError = false
        args = ["key00", "after"]
        @tester.value = {"value" => ["before"]}
        expect(@tester.send(:REDIS_GETSET, args)).to eq "before"
      end
      it "REDIS_STRLEN" do
        @tester.raiseError = false
        args = ["key00"]
        @tester.value = {"value" => ["test"]}
        expect(@tester.send(:REDIS_STRLEN, args)).to eq 4
      end
      it "REDIS_DEL (success)" do 
        @tester.raiseError = false
        args = ["key00"]
        command = "DELETE FROM k.f WHERE key = 'key00';"
        expect(@tester.send(:REDIS_DEL, args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_DEL (error)" do 
        @tester.raiseError = true
        args = ["key00"]
        expect(@tester.send(:REDIS_DEL, args)).to eq false
      end
    end
    context " > Redis (Set) Operation" do
      it "REDIS_SADD(success)" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        command = "INSERT INTO k.array (key,value) VALUES ('test0',{'a','b','c','d'})"
        expect(@tester.send(:REDIS_SADD,["test0","d"])).to be true
        expect(@tester.command).to eq command
      end
      it "REDIS_SADD (error)" do
        @tester.raiseError = true
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        command = "INSERT INTO k.array (key,value) VALUES ('test0',{'a','b','c','d'})"
        expect(@tester.send(:REDIS_SADD,["test0"])).to be false
      end
      it "REDIS_SREM (success)" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        command = "INSERT INTO k.array (key,value) VALUES ('test0',{'a','b'})"
        expect(@tester.send(:REDIS_SREM,["test0","c"])).to be true
        expect(@tester.command).to eq command
      end
      it "REDIS_SREM (error)" do
        @tester.raiseError = true
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        command = "INSERT INTO k.array (key,value) VALUES ('test0',{'a','b'})"
        expect(@tester.send(:REDIS_SREM,["test0","c"])).to be false
      end
      it "REDIS_SMEMBERS (success)" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        ans = ["a","b","c"]        
        expect(@tester.send(:REDIS_SMEMBERS,["test0"])).to match_array ans
      end
      it "REDIS_SMEMBERS (error)" do
        @tester.raiseError = true
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SMEMBERS,["test0"])).to eq []
      end
      it "REDIS_SISMEMBER" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SISMEMBER,["test0","a"])).to be true
        expect(@tester.send(:REDIS_SISMEMBER,["test0","d"])).to be false
      end
      it "REDIS_SRANDMEMBER" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        ans = @tester.send(:REDIS_SRANDMEMBER,["test0"])
        expect(["a","b","c"].include?(ans)).to be true
      end
      it "REDIS_SPOP (success)" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SPOP,["test0"])).to be true
        command = "INSERT INTO k.array (key,value) VALUES ('test0',{'a','b'})"
        expect(@tester.command).to eq command
      end
      it "REDIS_SPOP (error)" do
        @tester.raiseError = true
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SPOP,["test0"])).to be false
      end
      it "REDIS_SMOVE" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SMOVE,["test0","dst","c"])).to be true
        command = "INSERT INTO k.array (key,value) VALUES ('dst',{'c'})"
        expect(@tester.command).to eq command
      end
      it "REDIS_SCARD" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SCARD,["test0"])).to be 3
      end
      it "REDIS_SDIFF" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c','d'}\ntest1,{'d','e','f'}"
        ans = ["a","b","c","e","f"]
        expect(@tester.send(:REDIS_SDIFF,["test0","test1"])).to match_array ans
      end
      it "REDIS_SDIFFSTORE" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c','d'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SDIFFSTORE,["dst","test0","test1"])).to be true
        command = "INSERT INTO k.array (key,value) VALUES ('dst',{'a','b','c','e','f'})"
        expect(@tester.command).to eq command
      end
      it "REDIS_SINTER" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c','d'}\ntest1,{'d','e','f'}"
        ans = ["d"]
        expect(@tester.send(:REDIS_SINTER,["test0","test1"])).to match_array ans
      end
      it "REDIS_SINTERSTORE" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c','d'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SINTERSTORE,["dst","test0","test1"])).to be true
        command = "INSERT INTO k.array (key,value) VALUES ('dst',{'d'})"
        expect(@tester.command).to eq command
      end
      it "REDIS_SUNION" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        ans = ["a","b","c","d","e","f"]
        expect(@tester.send(:REDIS_SUNION,["test0","test1"])).to match_array ans
      end
      it "REDIS_SUNIONSTORE" do
        @tester.raiseError = false
        @tester.value = "test0,{'a','b','c'}\ntest1,{'d','e','f'}"
        expect(@tester.send(:REDIS_SUNIONSTORE,["dst","test0","test1"])).to be true
        command = "INSERT INTO k.array (key,value) VALUES ('dst',{'a','b','c','d','e','f'})"
        expect(@tester.command).to eq command
      end
    end

    context " > Redis (Sorted Set) Operation" do
      it "REDIS_ZADD (success)" do
        @tester.raiseError = false
        args = ["k0",1.2,"v"]
        command = "UPDATE k.sarray SET value = value + {'v':1.2} WHERE key = 'k0';"
        expect(@tester.send(:REDIS_ZADD,args)).to be true
        expect(@tester.command).to eq command
      end
      it "REDIS_ZADD (error)" do
        @tester.raiseError = true
        args = ["k0",1.2,"v"]
        expect(@tester.send(:REDIS_ZADD,args)).to be false
      end
      it "REDIS_ZREM (success)" do
        @tester.raiseError = false
        args = ["k0","v"]
        command = "DELETE value['v'] FROM k.sarray WHERE key = 'k0';"
        expect(@tester.send(:REDIS_ZREM,args)).to be true
        expect(@tester.command).to eq command
      end
      it "REDIS_ZREM (error)" do
        @tester.raiseError = true
        args = ["k0","v"]
        expect(@tester.send(:REDIS_ZREM,args)).to be false
      end
      it "REDIS_ZINCRBY" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",1.2,"a"]
        expect(@tester.send(:REDIS_ZINCRBY,args)).to be true
        command = "UPDATE k.sarray SET value = value + {'a':3.2} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_ZRANK" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0","b"]
        expect(@tester.send(:REDIS_ZRANK,args)).to eq 3
      end
      it "REDIS_ZREVRANK" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0","b"]
        expect(@tester.send(:REDIS_ZREVRANK,args)).to eq 1
      end
      it "REDIS_ZRANGE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",1,2]
        expect(@tester.send(:REDIS_ZRANGE,args)).to match_array ["a","c"]
        args = ["k0",1,-1]
        expect(@tester.send(:REDIS_ZRANGE,args)).to match_array ["a","c","b"]
      end
      it "REDIS_ZREVRANGE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",1,2]
        expect(@tester.send(:REDIS_ZREVRANGE,args)).to match_array ["b","c"]
        args = ["k0",1,-1]
        expect(@tester.send(:REDIS_ZREVRANGE,args)).to match_array ["b","c","a"]
      end
      it "REDIS_ZRANGEBYSCORE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",4,5]
        expect(@tester.send(:REDIS_ZRANGEBYSCORE,args)).to match_array ["b","c"]
      end
      it "REDIS_ZCOUNT" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",4,5]
        expect(@tester.send(:REDIS_ZCOUNT,args)).to eq 2
      end
      it "REDIS_ZSCARD" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0"]
        expect(@tester.send(:REDIS_ZCARD,args)).to eq 3
      end
      it "REDIS_ZSCORE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0","a"]
        expect(@tester.send(:REDIS_ZSCORE,args)).to eq 2.0
      end
      it "REDIS_ZREMRANGEBYSCORE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",2,4]
        expect(@tester.send(:REDIS_ZREMRANGEBYSCORE,args)).to be true
        command = "UPDATE k.sarray SET value = {'b':5.0} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_ZREMRANGEBYRANK" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = ["k0",1,2]
        expect(@tester.send(:REDIS_ZREMRANGEBYRANK,args)).to be true
        command = "UPDATE k.sarray SET value = {'c':4.0} WHERE key = 'k0';"
        expect(@tester.command).to eq command
        args = ["k0",1,-1]
        expect(@tester.send(:REDIS_ZREMRANGEBYRANK,args)).to be true
        command = "UPDATE k.sarray SET value = {} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_ZUNIONSTORE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = {}
        args["key"]  = "dst"
        args["args"] = ["src0","src1"]
        args["option"] = {:weights => [1,2], :aggregate => "SUM"}
        expect(@tester.send(:REDIS_ZUNIONSTORE,args)).to be true
        command = "UPDATE k.sarray SET value = {'a':6.0,'b':15.0,'c':12.0} WHERE key = 'dst';"
        expect(@tester.command).to eq command
        @tester.value = "{}"
        expect(@tester.send(:REDIS_ZUNIONSTORE,args)).to be false
      end
      it "REDIS_ZINTERSTORE" do
        @tester.raiseError = false
        @tester.value = "{'a':2.0,'b':5.0,'c':4.0}"
        args = {}
        args["key"]  = "dst"
        args["args"] = ["src0","src1"]
        args["option"] = {:weights => [1,2], :aggregate => "SUM"}
        expect(@tester.send(:REDIS_ZINTERSTORE,args)).to be true
        command = "UPDATE k.sarray SET value = {'a':6.0,'b':15.0,'c':12.0} WHERE key = 'dst';"
        expect(@tester.command).to eq command
        @tester.value = "{}"
        expect(@tester.send(:REDIS_ZINTERSTORE,args)).to be false

      end
      it "redis_zget (success)" do
        @tester.raiseError = false
        ans = {:a => 2.0, :b => 3.0, :c => 4.0}
        @tester.value = "{'a':2.0,'b':3.0,'c':4.0}"
        args = ["k0"]
        command = "SELECT value FROM k.sarray WHERE key = 'k0';"
        expect(@tester.send(:redis_zget,args)).to include ans
        expect(@tester.command).to eq command
      end
      it "redis_zget (error)" do
        @tester.raiseError = true
        expect(@tester.send(:redis_zget,["k0"])).to include {}
      end
      it "createDocWithAggregate (SUM)" do
        data = {"a"=>[20,10],"b"=>[10,20]}
        ans =  {"a"=>30,"b"=>30}
        expect(@tester.send(:createDocWithAggregate,data,"SUM")).to eq ans
      end
      it "createDocWithAggregate (MAX)" do
        data = {"a"=>[20,10],"b"=>[10,20]}
        ans =  {"a" => 20,"b" => 20}
        expect(@tester.send(:createDocWithAggregate,data,"MAX")).to eq ans
      end
      it "createDocWithAggregate (MIN)" do
        data = {"a"=>[20,10],"b"=>[10,20]}
        ans =  {"a"=>10,"b"=>10}
        expect(@tester.send(:createDocWithAggregate,data,"MIN")).to eq ans
      end
      it "createDocWithAggregate (error)" do
        data = {"a"=>[20,10],"b"=>[10,20]}
        ans =  {}
        expect(@tester.send(:createDocWithAggregate,data,"error")).to eq ans
      end
    end
    context " > Redis (List) Operation" do 
      it "REDIS_LPUSH (success)" do
        @tester.raiseError = false
        args = ["key00","val0"]
        command = "UPDATE k.list SET value = ['val0'] + value WHERE key = 'key00';"
        expect(@tester.send(:REDIS_LPUSH,args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_LPUSH (error)" do
        @tester.raiseError = true
        args = ["key00","val0"]
        expect(@tester.send(:REDIS_LPUSH,args)).to eq false
      end
      it "REDIS_RPUSH (success)" do
        @tester.raiseError = false
        args = ["key00","val0"]
        command = "UPDATE k.list SET value = value + ['val0'] WHERE key = 'key00';"
        expect(@tester.send(:REDIS_RPUSH,args)).to eq true
        expect(@tester.command).to eq command
      end
      it "REDIS_RPUSH (error)" do
        @tester.raiseError = true
        args = ["key00","val0"]
        expect(@tester.send(:REDIS_RPUSH,args)).to eq false
      end
      it "REDIS_LPOP (success)" do
        @tester.raiseError = false
        @tester.value = "['val00','val11']"
        args = ["key00"]
        command = "DELETE value[0] FROM k.list WHERE key = 'key00';"
        expect(@tester.send(:REDIS_LPOP,args)).to eq "val00"
        expect(@tester.command).to eq command
      end
      it "REDIS_LPOP (error)" do
        @tester.raiseError = true
        args = ["key00"]
        expect(@tester.send(:REDIS_LPOP,args)).to eq false
      end
      it "REDIS_RPOP (success)" do
        @tester.raiseError = false
        @tester.value = "['val00','val11']"
        args = ["key00"]
        command = "DELETE value[1] FROM k.list WHERE key = 'key00';"
        expect(@tester.send(:REDIS_RPOP,args)).to eq "val11"
        expect(@tester.command).to eq command
      end
      it "REDIS_RPOP (error)" do
        @tester.raiseError = true
        args = ["key00"]
        expect(@tester.send(:REDIS_RPOP,args)).to eq false
      end
      it "REDIS_LRANGE" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0",1,2]
        ans  = ["a","b"]
        expect(@tester.send(:REDIS_LRANGE,args)).to match_array ans
        args = ["key0",1,-1]
        ans  = ["a","b","c"]
        expect(@tester.send(:REDIS_LRANGE,args)).to match_array ans
        args = ["key0",-1,3]
        ans  = ["a","b","c"]
        expect(@tester.send(:REDIS_LRANGE,args)).to match_array ans
      end
      it "REDIS_LREM" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0",1,"a"]
        expect(@tester.send(:REDIS_LREM,args)).to be true
        command = "UPDATE k.list SET value = ['b','c'] WHERE key = 'key0'"
        expect(@tester.command).to eq command
        
        args = ["key0",-1,"a"]
        expect(@tester.send(:REDIS_LREM,args)).to be true
        expect(@tester.command).to eq command
        
        args = ["key0",0,"a"]
        @tester.send(:REDIS_LREM,args)
        expect(@tester.send(:REDIS_LREM,args)).to be true
        expect(@tester.command).to eq command
      end
      it "REDIS_LINDEX" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0",1]
        expect(@tester.send(:REDIS_LINDEX,args)).to eq "b"
        command = "SELECT value FROM k.list WHERE key = 'key0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_RPOPLPUSH" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0","d"]
        expect(@tester.send(:REDIS_RPOPLPUSH,args)).to eq "c"
        command = "UPDATE k.list SET value = ['d'] + value WHERE key = 'key0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_LSET (success)" do
        @tester.raiseError = false
        @tester.value = true
        args = ["key0",2,"d"]
        expect(@tester.send(:REDIS_LSET,args)).to eq true
        command = "UPDATE k.list SET value[2] = 'd' WHERE key = 'key0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_LSET (error)" do
        @tester.raiseError = true
        args = ["key0",2,"d"]
        expect(@tester.send(:REDIS_LSET,args)).to eq false
      end
      it "REDIS_LTRIM" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0",1,3]
        expect(@tester.send(:REDIS_LTRIM,args)).to eq true
      end
      it "REDIS_LLEN" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        args = ["key0"]
        expect(@tester.send(:REDIS_LLEN,args)).to eq 3
      end
      it "redis_lget (index)" do
        @tester.raiseError = false
        @tester.value = "['a','b','c']"
        command = "SELECT value FROM k.list WHERE key = 'key0';"
        args = ["key0"]
        ans = ["a","b","c"]
        expect(@tester.send(:redis_lget,args)).to match_array ans
        expect(@tester.command).to eq command
      end
      it "redis_lget (error))" do
        @tester.raiseError = true
        @tester.value = "['a','b','c']"
        command = "SELECT value FROM k.list WHERE key = 'key0';"
        args = ["key0"]
        ans = ["a","b","c"]
        expect(@tester.send(:redis_lget,args)).to eq []
      end
      it "redis_lreset (success)" do
        @tester.raiseError = false
        args = ["key0"]
        values = ["a","b","c"]
        @tester.value = true
        expect(@tester.send(:redis_lreset,args,values)).to be true
        command = "UPDATE k.list SET value = ['#{values.join("','")}']"
        command += " WHERE key = '#{args[0]}'"
        expect(@tester.command).to eq command
      end
      it "redis_lreset (error)" do
        @tester.raiseError = true
        args = ["key0"]
        values = ["a","b","c"]
        @tester.value = true
        expect(@tester.send(:redis_lreset,args,values)).to be false
      end
    end
    context " > Redis (Hash) Operation" do
      it "REDIS_HSET (success)" do
        @tester.raiseError = false
        args = ["k0","f0","v0"]
        expect(@tester.send(:REDIS_HSET,args)).to be true
        command = "UPDATE k.hash SET value = value + {'f0':'v0'} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HSET (success) with hash" do
        @tester.raiseError = false
        args = ["k0"]
        hash = {"f0"=>"v0"}
        expect(@tester.send(:REDIS_HSET,args,hash)).to be true
        command = "UPDATE k.hash SET value = value + {'f0':'v0'} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HSET (error)" do
        @tester.raiseError = true
        args = ["k0","f0","v0"]
        expect(@tester.send(:REDIS_HSET,args,hash)).to be false
      end
      it "REDIS_HGET (success) with field" do
        @tester.raiseError = false
        args = ["k0","f0"]
        @tester.value = "{'f0':'v0'}"
        expect(@tester.send(:REDIS_HGET,args)).to eq "v0"
        command = "SELECT value FROM k.hash WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HGET (error)" do
        args = ["k0"]
        @tester.raiseError = true
        expect(@tester.send(:REDIS_HGET,args)).to eq nil
      end
      it "REDIS_HMGET (success)" do
        args = {"key"=>"k0", "args" => ["f0","f1"]}
        @tester.raiseError = false
        @tester.value = "{'f0':'v0','f1':'v1','f2':'v3'}"
        ans = ["v0","v1"]
        expect(@tester.send(:REDIS_HMGET,args)).to match_array ans
      end
      it "REDIS_HMGET (error)" do
        args = {"key"=>"k0", "args" => ["f0","f1"]}
        @tester.raiseError = true
        @tester.value = "{'f0':'v0','f1':'v1','f2':'v3'}"
        expect(@tester.send(:REDIS_HMGET,args)).to eq []
      end
      it "REDIS_HMSET" do
        args = {"key"=>"k0", "args" => {"f0"=>"v0"}}
        @tester.raiseError = false
        expect(@tester.send(:REDIS_HMSET,args)).to eq true
      end
      it "REDIS_HINCRBY" do
        args = ["k0","f0",20]
        @tester.value = "{'f0':'10'}"
        @tester.raiseError = false
        expect(@tester.send(:REDIS_HINCRBY,args)).to eq true
        command = "UPDATE k.hash SET value = value + {'f0':'30'} WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HEXISTS (true)" do
        @tester.value = "{'f0':'v0'}"
        @tester.raiseError = false
        args = ["k0","f0"]
        expect(@tester.send(:REDIS_HEXISTS,args)).to be true
      end
      it "REDIS_HEXISTS (false)" do
        @tester.value = "{'f0':'v0'}"
        @tester.raiseError = false
        args = ["k0","f1"]
        expect(@tester.send(:REDIS_HEXISTS,args)).to be false
      end
      it "REDIS_HDEL (success)" do
        @tester.raiseError = false
        @tester.value = "{'f0':'v0'}"
        args = ["k0","f0"]
        expect(@tester.send(:REDIS_HDEL,args)).to be true
        command = "DELETE value['f0'] FROM k.hash WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HDEL (error)" do
        @tester.raiseError = true
        @tester.value = "{'f0':'v0'}"
        args = ["k0","f0"]
        expect(@tester.send(:REDIS_HDEL,args)).to be false
        command = "DELETE value['f0'] FROM k.hash WHERE key = 'k0';"
        expect(@tester.command).to eq command
      end
      it "REDIS_HLEN" do
        @tester.raiseError = false
        schema = CassandraSchemaMock.new()
        schema.fields = ["f0","f1","f2"]
        @tester.schemas = {"k0" => schema}
        expect(@tester.send(:REDIS_HLEN,["k0"])).to eq 3
      end
      it "REDIS_HKEYS" do
        ans = ["f0","f1","f2"]
        @tester.raiseError = false
        schema = CassandraSchemaMock.new()
        schema.fields = ["f0","f1","f2"]
        @tester.schemas = {"k0" => schema}
        expect(@tester.send(:REDIS_HKEYS,["k0"])).to match_array ans
      end
      it "REDIS_HVALS" do
        @tester.raiseError = false
        schema = CassandraSchemaMock.new()
        schema.fields = ["f0","f1","f2"]
        @tester.schemas = {"k0" => schema}
        @tester.value = "{'f0':'v0','f1':'v1','f2':'v2'}"
        ans = ["v0","v1","v2"]
        expect(@tester.send(:REDIS_HVALS,["k0"])).to match_array ans
      end
      it "REDIS_HGETALL" do
        args = {"key"=>"k0"}
        @tester.raiseError = false
        schema = CassandraSchemaMock.new()
        schema.fields = ["f0","f1","f2"]
        @tester.schemas = {"k0" => schema}
        @tester.value = "{'f0':'v0','f1':'v1','f2':'v2'}"
        ans = {"f0"=>"v0","f1"=>"v1","f2"=>"v2"}
        expect(@tester.send(:REDIS_HGETALL,args)).to include ans
      end
    end
    context "Others & Private Method" do
      it "REDIS_FLUSHALL (success)" do
        @tester.raiseError = false
        @tester.schemas = {"k"=> CassandraSchemaMock.new}
        expect(@tester.send(:REDIS_FLUSHALL)).to be true
      end
      it "REDIS_FLUSHALL (success)" do
        @tester.raiseError = true
        @tester.schemas = {"k"=> CassandraSchemaMock.new}
        expect(@tester.send(:REDIS_FLUSHALL)).to be false
      end
      it "prepare_redis (ZUNIONSTORE)" do
        ans = {"operand" => "REDIS_ZUNIONSTORE", "args" => "OK"}
        expect(@tester.send(:prepare_redis,"ZUNIONSTORE","")).to include ans
      end
      it "prepare_redis (MSET)" do
        ans = {"operand" => "REDIS_MSET", "args" => "OK"}
        expect(@tester.send(:prepare_redis,"MSET","")).to include ans
      end
      it "prepare_redis (HMGET)" do
        ans = {"operand" => "REDIS_HMGET", "args" => "OK"}
        expect(@tester.send(:prepare_redis,"HMGET","")).to include ans
      end
      it "prepare_redis (HMSET)" do
        ans = {"operand" => "REDIS_HMSET", "args" => "OK"}
        expect(@tester.send(:prepare_redis,"HMSET","")).to include ans
      end
      it "prepare_redis (HGET)" do
        ans = {"operand" => "REDIS_HGET", "args" => ""}
        expect(@tester.send(:prepare_redis,"HGET","")).to include ans
      end
    end
  end
end
  
