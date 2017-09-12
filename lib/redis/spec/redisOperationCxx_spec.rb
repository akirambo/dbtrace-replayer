
require_relative "../../../spec/spec_helper"
require_relative "../src/redisRunner"

RSpec.describe 'RedisOperation (C++ API) Unit Test [Each Connection & Sync]' do
  before do
    @logger = DummyLogger.new
    @option = {
      :api => "cxx",
      :keepalive => false,
      :async => false
    }
    @option[:sourceDB] = "redis"
    @runner = RedisRunner.new("redis", @logger, @option)
  end
  ## STRINGS
  context 'STRING Operation' do
    before (:each) do
      @runner.send("FLUSHALL",[])
    end
    it "SET" do
      expect(@runner.send("SET",["test","correct"])).to eq "OK"
    end
    it "GET" do
      @runner.send("SET",["test","correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "DEL" do
      @runner.send("SET",["test","correct"])
      expect(@runner.send("DEL",["test"])).to eq "OK"
    end
    it "SETNX" do
      @runner.send("SET",["test","correct"])
      @runner.send("SETNX",["test","incorrect"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "SETEX" do
      @runner.send("SETEX",["test",1,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq ""
    end
    it "PSETEX" do
      @runner.send("PSETEX",["test",1000,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq ""
    end
    it "MSET" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "MGET" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      expect(@runner.send("MGET",["test0","test1"])).to eq "correct,correct"
    end
    it "MSETNX" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      @runner.send("MSETNX",["test0","incorrect","test1","incorrect"])
      @runner.send("MSETNX",["test2","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "INCR" do
      @runner.send("SET",["test",0])
      @runner.send("INCR",["test"])
      expect(@runner.send("GET",["test"])).to eq "1"
    end
    it "INCRBY" do
      @runner.send("SET",["test",0])
      @runner.send("INCRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "3"
    end
    it "DECR" do
      @runner.send("SET",["test",10])
      @runner.send("DECR",["test"])
      expect(@runner.send("GET",["test"])).to eq "9"
    end
    it "DECRBY" do
      @runner.send("SET",["test",10])
      @runner.send("DECRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "7"
    end
    it "APPEND" do
      @runner.send("SET",["test","correc"])
      @runner.send("APPEND",["test","t"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "GETSET" do
      @runner.send("SET",["test","pending"])
      expect(@runner.send("GETSET",["test","correct"])).to eq "pending"
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "STRLEN" do
      @runner.send("SET",["test","collect"])
      expect(@runner.send("STRLEN",["test"])).to eq "7"
    end
  end
  ## SETS
  context 'SETS Operation' do
    it "SADD/SMEMBERS" do
      @runner.send("SADD",{"key"=>"test_smember","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_smember","args"=>"elem2"})
      expect(@runner.send("SMEMBERS",["test_smember"])).to eq "elem2,elem1"
    end
    it "SREM" do
      @runner.send("SADD",{"key"=>"test_srem","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_srem","args"=>"elem2"})
      expect(@runner.send("SREM",["test_srem","elem1"])).to eq "OK"
    end
    it "SPOP" do
      @runner.send("SADD",{"key"=>"test_spop","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_spop","args"=>"elem2"})
      ans01 = @runner.send("SPOP",["test_spop"])
      ans02 = @runner.send("SMEMBERS",["test_spop"])
      if((ans01 == "elem1" and ans02 == "elem2") or
          (ans01 == "elem2" and ans02 == "elem1"))then
        expect(true).to be true
      else
        expect(true).to be false
      end
    end
    it "SMOVE" do
      @runner.send("SADD",{"key"=>"test_smove","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_smove","args"=>"elem2"})
      @runner.send("SMOVE",["test_smove","dst_smove","elem2"])
      expect(@runner.send("SMEMBERS",["test_smove"])).to eq "elem1"
      expect(@runner.send("SMEMBERS",["dst_smove"])).to eq "elem2"
    end
    it "SCARD" do
      @runner.send("SADD",{"key"=>"test_scard","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_scard","args"=>"elem2"})
      expect(@runner.send("SCARD",["test_scard"])).to eq "2"
    end
    it "SINTER" do
      @runner.send("SADD",{"key"=>"test_sinter0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_sinter0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sinter1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sinter1","args"=>"elem2"})
      expect(@runner.send("SINTER",["test_sinter0","test_sinter1"])).to eq "elem1"
    end
    it "SINTERSTORE" do
      @runner.send("SADD",{"key"=>"test_sinters0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_sinters0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sinters1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sinters1","args"=>"elem2"})
      args = {
        "key" => "dst_sinters",
        "args" => ["test_sinters0","test_sinters1"]
      }
      expect(@runner.send("SINTERSTORE",args)).to eq "OK"
      expect(@runner.send("SMEMBERS",["dst_sinters"])).to eq "elem1"
    end
    it "SISMEMBER" do
      @runner.send("SADD",{"key"=>"test_sismem","args"=>"elem1"})
      expect(@runner.send("SISMEMBER",["test_sismem","elem1"])).to eq "1"
      expect(@runner.send("SISMEMBER",["test_sismem","elem0"])).to eq "0"
    end
    it "SDIFF" do
      @runner.send("SADD",{"key"=>"test_diff0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_diff0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_diff1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_diff1","args"=>"elem2"})
      expect(@runner.send("SDIFF",["test_diff0","test_diff1"])).to eq "elem0"
    end
    it "SDIFFSTORE" do
      @runner.send("SADD",{"key"=>"test_diffs0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_diffs0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_diffs1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_diffs1","args"=>"elem2"})
      args = {
        "key" => "dst_diff",
        "args" => ["test_diffs0","test_diffs1"]
      }
      expect(@runner.send("SDIFFSTORE",args)).to eq "OK"
      expect(@runner.send("SMEMBERS",["dst_diff"])).to eq "elem0"
    end
    it "SUNION" do
      @runner.send("SADD",{"key"=>"test_sunion0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_sunion0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sunion1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sunion1","args"=>"elem2"})
      ans = @runner.send("SUNION",["test_sunion0","test_sunion1"])
      expect(ans.split(",")).to match_array ["elem0","elem1","elem2"]
    end
    it "SUNIONSTORE" do
      @runner.send("SADD",{"key"=>"test_sunions0","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_sunions0","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sunions1","args"=>"elem1"})
      @runner.send("SADD",{"key"=>"test_sunions1","args"=>"elem2"})
      ans = @runner.send("SUNION",["test_sunions0","test_sunions1"])
      args = {
        "key" => "dst_sunions",
        "args" => ["test_sunions0","test_sunions1"]
      }
      expect(@runner.send("SUNIONSTORE",args)).to eq "OK"
      expect(@runner.send("SMEMBERS",["dst_sunions"]).split(",")).to match_array ["elem0","elem1","elem2"]
    end
    it "SRANDMEMBER" do
      @runner.send("SADD",{"key"=>"test_srandmember","args"=>"elem0"})
      @runner.send("SADD",{"key"=>"test_srandmember","args"=>"elem1"})
      elem = @runner.send("SRANDMEMBER",["test_srandmember"])
      expect(["elem0","elem1"].include?(elem)).to be true
    end
  end
  ## SORTED SET
  context 'SORTED SET Operation' do
    it "ZADD" do
      @runner.send("ZADD",["zadd",300,"e3"])
      @runner.send("ZADD",["zadd",100,"e1"])
      @runner.send("ZADD",["zadd",200,"e2"])
      expect(@runner.send("ZRANGE",["zadd",0,2])).to eq "e1,e2,e3"
    end
    it "ZREM" do
      @runner.send("ZADD",["zrem",200,"e2"])
      @runner.send("ZADD",["zrem",100,"e1"])
      @runner.send("ZREM",["zrem","e1"])
      expect(@runner.send("ZRANGE",["zrem",0,2])).to eq "e2"
    end
    it "ZINCRBY" do
      @runner.send("ZADD",["zincrbys",200,"e2"])
      @runner.send("ZADD",["zincrbys",100,"e1"])
      @runner.send("ZINCRBY",["zincrbys",1000,"e1"])
      expect(@runner.send("ZRANGEBYSCORE",["zincrbys",1000,2000])).to eq "e1"
    end
    it "ZRANK" do
      @runner.send("ZADD",["zranks",200,"e2"])
      @runner.send("ZADD",["zranks",100,"e1"])
      @runner.send("ZADD",["zranks",300,"e3"])
      expect(@runner.send("ZRANK",["zranks","e1"])).to eq "0"
      expect(@runner.send("ZRANK",["zranks","e2"])).to eq "1"
      expect(@runner.send("ZRANK",["zranks","e3"])).to eq "2"
    end
    it "ZREVRANK" do
      @runner.send("ZADD",["zrrank",200,"e2"])
      @runner.send("ZADD",["zrrank",100,"e1"])
      @runner.send("ZADD",["zrrank",300,"e3"])
      expect(@runner.send("ZREVRANK",["zrrank","e1"])).to eq "2"
      expect(@runner.send("ZREVRANK",["zrrank","e2"])).to eq "1"
    end
    it "ZRANGE" do
      @runner.send("ZADD",["zrange",200,"e2"])
      @runner.send("ZADD",["zrange",100,"e1"])
      @runner.send("ZADD",["zrange",300,"e3"])
      expect(@runner.send("ZRANGE",["zrange",1,2])).to eq "e2,e3"
    end
    it "ZREVRANGE" do
      @runner.send("ZADD",["zrrange",200,"e2"])
      @runner.send("ZADD",["zrrange",100,"e1"])
      @runner.send("ZADD",["zrrange",300,"e3"])
      expect(@runner.send("ZREVRANGE",["zrrange",1,2])).to eq "e2,e1"
    end
    it "ZRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGEBYSCORE",["zset",200,300])).to eq "e2,e3"
    end
    it "ZCOUNT" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCOUNT",["zset",200,300])).to eq "2"
    end
    it "ZCARD" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCARD",["zset"])).to eq "3"
    end
    it "ZSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZSCORE",["zset","e2"])).to eq "200"
    end
    it "ZREMRANGEBYRANK" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYRANK",["zset",1,2])
      expect(@runner.send("ZCARD",["zset"])).to eq "1"
      expect(@runner.send("ZRANK",["zset","e1"])).to eq "0"
    end
    it "ZREMRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYSCORE",["zset",200,300])
      expect(@runner.send("ZCARD",["zset"])).to eq "1"
      expect(@runner.send("ZRANK",["zset","e1"])).to eq "0"
    end
    it "ZUNIONSTORE" do
      @runner.send("ZADD",["zusA",200,2])
      @runner.send("ZADD",["zusA",100,1])
      @runner.send("ZADD",["zusA",300,3])
      @runner.send("ZADD",["zusA",220,5])
      @runner.send("ZADD",["zusB",220,2])
      @runner.send("ZADD",["zusB",120,1])
      @runner.send("ZADD",["zusB",320,3])
      @runner.send("ZADD",["zusB",220,6])
      @runner.send("ZADD",["zusD",230,1])
      @runner.send("ZADD",["zusD",130,2])
      @runner.send("ZADD",["zusD",330,3])
      @runner.send("ZADD",["zusD",220,7])
      ## BASIC 
      args = {
        "key" => "zusC",
        "args" => ["zusA","zusB"],
        "option" => {}
      }
      
      expect(@runner.send("ZUNIONSTORE", args)).to eq "OK"
      expect(@runner.send("ZRANGE",
          ["zusC",0,5])).to eq "1,5,6,2,3"
      
      ## AGGREGATE [SUM]x2
      args = {
        "key" => "zusCsum",
        "args" => ["zusA","zusB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zusCsum","1"])).to eq "320"
      expect(@runner.send("ZSCORE",["zusCsum","2"])).to eq "620"
      expect(@runner.send("ZSCORE",["zusCsum","3"])).to eq "920"
      expect(@runner.send("ZSCORE",["zusCsum","5"])).to eq "440"
      expect(@runner.send("ZSCORE",["zusCsum","6"])).to eq "220"
      
      ## AGGREGATE [SUM]x3
      args = {
        "key" => "zusCsum2",
        "args" => ["zusA","zusB","zusD"],
        "option" => {
          :weights => ["2.0","1.0","1.5"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq "OK"
      expect(@runner.send("ZRANGE",["zusCsum2",0,5])).to eq  "6,7,5,1,2,3"
      expect(@runner.send("ZSCORE",["zusCsum2","1"])).to eq "665"
      expect(@runner.send("ZSCORE",["zusCsum2","2"])).to eq "815"
      expect(@runner.send("ZSCORE",["zusCsum2","3"])).to eq "1415"
      expect(@runner.send("ZSCORE",["zusCsum2","5"])).to eq "440"
      expect(@runner.send("ZSCORE",["zusCsum2","6"])).to eq "220"
      expect(@runner.send("ZSCORE",["zusCsum2","7"])).to eq "330"

      ## AGGREGATE [MIN]
      args = {
        "key" => "zusCmin",
        "args" => ["zusA","zusB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "min"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zusCmin","1"])).to eq "120"
      expect(@runner.send("ZSCORE",["zusCmin","2"])).to eq "220"
      expect(@runner.send("ZSCORE",["zusCmin","3"])).to eq "320"
      expect(@runner.send("ZSCORE",["zusCmin","5"])).to eq "440"
      expect(@runner.send("ZSCORE",["zusCmin","6"])).to eq "220"
      ## AGGREGATE [MAX]
      args = {
        "key" => "zusCmax",
        "args" => ["zusA","zusB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "max"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zusCmax","1"])).to eq "200"
      expect(@runner.send("ZSCORE",["zusCmax","2"])).to eq "400"
      expect(@runner.send("ZSCORE",["zusCmax","3"])).to eq "600"
      expect(@runner.send("ZSCORE",["zusCmax","5"])).to eq "440"
      expect(@runner.send("ZSCORE",["zusCmax","6"])).to eq "220"
    end
    
    it "ZINTERSTORE" do
      @runner.send("ZADD",["zisA",200,2])
      @runner.send("ZADD",["zisA",100,1])
      @runner.send("ZADD",["zisA",300,3])
      @runner.send("ZADD",["zisB",220,20])
      @runner.send("ZADD",["zisB",120,10])
      @runner.send("ZADD",["zisB",320,3])
      ## BASIC 
      args = {
        "key" => "zisC",
        "args" => ["zisA","zisB"],
        "option" => {}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq "OK"
      expect(@runner.send("ZRANGE",["zisC",0,5])).to eq "3"

      ## AGGREGATE [SUM]
      args = {
        "key" => "zisCsum",
        "args" => ["zisA","zisB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "sum"}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zisCsum",3])).to eq "920"
      ## AGGREGATE [MIN]
      args = {
        "key" => "zisCmin",
        "args" => ["zisA","zisB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "min"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zisCmin",3])).to eq "320"
      ## AGGREGATE [MAX]
      args = {
        "key" => "zisCmax",
        "args" => ["zisA","zisB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "max"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq "OK"
      expect(@runner.send("ZSCORE",["zisCmax",3])).to eq "600"
    end
  end
  ## LIST
  context 'LIST Operation' do
    it "LPUSH/LRANGE" do 
      expect(@runner.send("LPUSH", ["test_lpush","e2"])).to eq "OK"
      expect(@runner.send("LPUSH", ["test_lpush","e1"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_lpush",0,-1])).to eq "e1,e2"
    end
    it "RPUSH" do
      expect(@runner.send("RPUSH", ["test_rpush","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_rpush","e2"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_rpush",0,-1])).to eq "e1,e2"
    end
    it "LPOP" do
      expect(@runner.send("LPUSH", ["test_lpop","e2"])).to eq "OK"
      expect(@runner.send("LPUSH", ["test_lpop","e1"])).to eq "OK"
      expect(@runner.send("LPOP", ["test_lpop"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_lpop",0,-1])).to eq "e2"
    end
    it "RPOP" do
      expect(@runner.send("LPUSH", ["test_rpop","e2"])).to eq "OK"
      expect(@runner.send("LPUSH", ["test_rpop","e1"])).to eq "OK"
      expect(@runner.send("RPOP", ["test_rpop"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_rpop",0,-1])).to eq "e1"
    end
    it "LREM" do 
      expect(@runner.send("RPUSH", ["test_lrem","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lrem","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lrem","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lrem","e2"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lrem","e1"])).to eq "OK"
      expect(@runner.send("LREM", ["test_lrem",-2,"e1"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_lrem",0,-1])).to eq "e1,e0,e2"
    end
    it "LINDEX" do
      expect(@runner.send("RPUSH", ["test_lindex","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lindex","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lindex","e2"])).to eq "OK"
      expect(@runner.send("LINDEX", ["test_lindex",0])).to eq "e0"
      expect(@runner.send("LINDEX", ["test_lindex",1])).to eq "e1"
      expect(@runner.send("LINDEX", ["test_lindex",-1])).to eq "e2"
    end
    it "RPOPLPUSH" do
      expect(@runner.send("RPUSH", ["test_rplp","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_rplp","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_rplp","e3"])).to eq "OK"
      expect(@runner.send("RPOPLPUSH", ["test_rplp","e2"])).to eq "e3"
    end
    it "LSET" do
      expect(@runner.send("RPUSH", ["test_lset","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lset","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_lset","e2"])).to eq "OK"
      expect(@runner.send("LSET", ["test_lset",0,"f0"])).to eq "OK"
      expect(@runner.send("LSET", ["test_lset",-1,"f2"])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_lset",0,-1])).to eq "f0,e1,f2"
    end
    it "LTRIM" do
      expect(@runner.send("RPUSH", ["test_ltrim","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_ltrim","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_ltrim","e2"])).to eq "OK"
      expect(@runner.send("LTRIM", ["test_ltrim",1,-1])).to eq "OK"
      expect(@runner.send("LRANGE", ["test_ltrim",0,-1])).to eq "e1,e2"
    end
    it "LLEN" do
      expect(@runner.send("RPUSH", ["test_llen","e0"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_llen","e1"])).to eq "OK"
      expect(@runner.send("RPUSH", ["test_llen","e2"])).to eq "OK"
      expect(@runner.send("LLEN", ["test_llen"])).to eq "3"
    end
  end
  ## HASH
  context 'HASH Operation' do
    it "HSET/HGET" do
      @runner.send("HSET", ["hkey","field0","value"])
      expect(@runner.send("HGET", ["hkey","field0"])).to eq "value"
    end
    it "HMSET/HMGET" do
      args = {
        "key" => "hmkey",
        "args" => ["field0","value0","field1","value1"]
      }
      expect(@runner.send("HMSET", args)).to eq "OK"
      args = {
        "key" => "hmkey",
        "args" => ["field0","field1"]
      }
      expect(@runner.send("HMGET", args)).to eq "value0,value1"
    end
    it "HINCRBY" do
      @runner.send("HSET", ["key","field", 0])
      @runner.send("HINCRBY",["key","field",10])
      expect(@runner.send("HGET", ["key","field"])).to eq "10"
    end
    it "HEXISTS" do
      @runner.send("HSET", ["key","field", 0])
      expect(@runner.send("HEXISTS", ["key","field"])).to eq "1"
      expect(@runner.send("HEXISTS", ["key","field_no"])).to eq "0"
    end
    it "HDEL" do
      @runner.send("HSET", ["hd","field0", 0])
      @runner.send("HSET", ["hd","field1", 0])
      @runner.send("HDEL", ["hd","field0"])
      expect(@runner.send("HGET", ["hd","field1"])).to eq "0"
      expect(@runner.send("HGET", ["hd","field0"])).to eq ""
    end
    it "HLEN" do
      @runner.send("HSET", ["hln","field0", 0])
      @runner.send("HSET", ["hln","field1", 0])
      expect(@runner.send("HLEN", ["hln"])).to eq "2"
    end
    it "HKEYS" do
      @runner.send("HSET", ["hks","field0", 0])
      @runner.send("HSET", ["hks","field1", 0])
      expect(@runner.send("HKEYS", ["hks"])).to eq "field0,field1"
    end
    it "HVALS" do
      @runner.send("HSET", ["hval","field0", 0])
      @runner.send("HSET", ["hval","field1", 3])
      expect(@runner.send("HVALS", ["hval"])).to eq "0,3"
    end
    it "HGETALL" do
      @runner.send("HSET", ["hgall","field0", 0])
      @runner.send("HSET", ["hgall","field1", 2])
      expect(@runner.send("HGETALL",["hgall"])).to eq "field0,0,field1,2"
    end
  end
end


RSpec.describe 'RedisOperation (C++ API) Unit Test [Reuse Connection & Sync]' do
  before do
    @logger = DummyLogger.new
    @option = {
      :api => "cxx",
      :keepalive => true
    }
    @option[:sourceDB] = "redis"
    @runner = RedisRunner.new("redis", @logger, @option)
  end
  ## STRINGS
  context 'STRING Operation' do
    before (:each) do
      @runner.send("FLUSHALL",[])
    end
    it "SET" do
      expect(@runner.send("SET",["test","correct"])).to eq "OK"
    end
    it "GET" do
      @runner.send("SET",["test","correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
=begin
    it "DEL" do
      @runner.send("SET",["test","correct"])
      expect(@runner.send("DEL",["test"])).to eq 1
    end
    it "SETNX" do
      @runner.send("SET",["test","correct"])
      @runner.send("SETNX",["test","incorrect"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "SETEX" do
      @runner.send("SETEX",["test",1,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq nil
    end
    it "PSETEX" do
      @runner.send("PSETEX",["test",1000,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq nil
    end
    it "MSET" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "MSETNX" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      @runner.send("MSETNX",["test0","incorrect","test1","incorrect"])
      @runner.send("MSETNX",["test2","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "INCR" do
      @runner.send("SET",["test",0])
      @runner.send("INCR",["test"])
      expect(@runner.send("GET",["test"])).to eq "1"
    end
    it "INCRBY" do
      @runner.send("SET",["test",0])
      @runner.send("INCRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "3"
    end
    it "DECR" do
      @runner.send("SET",["test",10])
      @runner.send("DECR",["test"])
      expect(@runner.send("GET",["test"])).to eq "9"
    end
    it "DECRBY" do
      @runner.send("SET",["test",10])
      @runner.send("DECRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "7"
    end
    it "APPEND" do
      @runner.send("SET",["test","correc"])
      @runner.send("APPEND",["test","t"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "GETSET" do
      @runner.send("SET",["test","pending"])
      expect(@runner.send("GETSET",["test","correct"])).to eq "pending"
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
=end
  end
=begin
  ## SETS
  context 'SETS Operation' do
    it "SADD/SMEMBERS" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SMEMBERS",["test"])).to match_array ["elem2","elem1"]
    end
    it "SREM" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SREM",["test","elem1"])).to eq true
    end
    it "SPOP" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      @runner.send("SPOP",["test"])
      @runner.send("SPOP",["test"])
      expect(@runner.send("SMEMBERS",["test"])).to eq []
    end
    it "SMOVE" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      @runner.send("SMOVE",["test","dst","elem2"])
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem2"]
    end
    it "SCARD" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SCARD",["test"])).to eq 2
    end
    it "SINTER" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SINTER",["test0","test1"])).to eq ["elem1"]
    end
    it "SINTERSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SINTERSTORE",args)).to eq 1
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem1"]
    end
    it "SDIFF" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SDIFF",["test0","test1"])).to eq ["elem0"]
    end
    it "SDIFFSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SDIFFSTORE",args)).to eq 1
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem0"]
    end
    it "SUNION" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SUNION",["test0","test1"])).to match_array ["elem0","elem1","elem2"]
    end
    it "SUNIONSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SUNIONSTORE",args)).to eq 3
      expect(@runner.send("SMEMBERS",["dst"])).to match_array ["elem0","elem1","elem2"]
    end
    it "SRANDMEMBER" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      data = @runner.send("SRANDMEMBER",["test"])
      expect(["elem1","elem2"].include?(data)).to eq true
    end
  end
  ## SORTED SET
  context 'SORTED SET Operation' do
    it "ZADD" do
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",200,"e2"])
      expect(@runner.send("ZRANGE",["zset",0,2])).to eq ["e1","e2","e3"]
    end
    it "ZREM" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZREM",["zset","e1"])
      expect(@runner.send("ZRANGE",["zset",0,2])).to eq ["e2"]
    end
    it "ZINCRBY" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZINCRBY",["zset",1000,1])
      expect(@runner.send("ZRANGEBYSCORE",["zset",1000,2000])).to eq ["1"]
    end
    it "ZRANK" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZADD",["zset",300,3])
      expect(@runner.send("ZRANK",["zset",1])).to eq 0
      expect(@runner.send("ZRANK",["zset",2])).to eq 1
    end
    it "ZREVRANK" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZADD",["zset",300,3])
      expect(@runner.send("ZREVRANK",["zset",1])).to eq 2
      expect(@runner.send("ZREVRANK",["zset",2])).to eq 1
    end
    it "ZRANGE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGE",["zset",1,2])).to eq ["e2","e3"]
    end
    it "ZREVRANGE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGE",["zset",1,2])).to eq ["e2","e3"]
    end
    it "ZRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGEBYSCORE",["zset",200,300])).to eq ["e2","e3"]
    end
    it "ZCOUNT" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCOUNT",["zset",200,300])).to eq 2
    end
    it "ZCARD" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCARD",["zset"])).to eq 3
    end
    it "ZSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZSCORE",["zset","e2"])).to eq 200.0
    end
    it "ZREMRANGEBYRANK" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYRANK",["zset",1,2])
      expect(@runner.send("ZCARD",["zset"])).to eq 1
      expect(@runner.send("ZRANK",["zset","e1"])).to eq 0
    end
    it "ZREMRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYSCORE",["zset",200,300])
      expect(@runner.send("ZCARD",["zset"])).to eq 1
      expect(@runner.send("ZRANK",["zset","e1"])).to eq 0
    end
    it "ZUNIONSTORE" do
      @runner.send("ZADD",["zsetA",200,2])
      @runner.send("ZADD",["zsetA",100,1])
      @runner.send("ZADD",["zsetA",300,3])
      @runner.send("ZADD",["zsetA",220,5])
      @runner.send("ZADD",["zsetB",220,2])
      @runner.send("ZADD",["zsetB",120,1])
      @runner.send("ZADD",["zsetB",320,3])
      @runner.send("ZADD",["zsetB",220,6])
      @runner.send("ZADD",["zsetD",230,1])
      @runner.send("ZADD",["zsetD",130,2])
      @runner.send("ZADD",["zsetD",330,3])
      @runner.send("ZADD",["zsetD",220,7])
      ## BASIC 
      args = {
        "key" => "zsetC",
        "args" => ["zsetA","zsetB"],
        "option" => {}
      }
      
      expect(@runner.send("ZUNIONSTORE", args)).to eq 5
      expect(@runner.send("ZRANGE",
          ["zsetC",0,5])).to eq ["1","5","6","2","3"]
      
      ## AGGREGATE [SUM]x2
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCsum","1"])).to eq 320.0 
      expect(@runner.send("ZSCORE",["zsetCsum","2"])).to eq 620.0
      expect(@runner.send("ZSCORE",["zsetCsum","3"])).to eq 920.0
      expect(@runner.send("ZSCORE",["zsetCsum","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCsum","6"])).to eq 220.0
      
      ## AGGREGATE [SUM]x3
      args = {
        "key" => "zsetCsum2",
        "args" => ["zsetA","zsetB","zsetD"],
        "option" => {
          :weights => ["2.0","1.0","1.5"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 6
      expect(@runner.send("ZRANGE",
          ["zsetCsum2",0,5])).to eq  ["6", "7", "5", "1", "2", "3"]
      expect(@runner.send("ZSCORE",["zsetCsum2","1"])).to eq 665.0 
      expect(@runner.send("ZSCORE",["zsetCsum2","2"])).to eq 815.0
      expect(@runner.send("ZSCORE",["zsetCsum2","3"])).to eq 1415.0
      expect(@runner.send("ZSCORE",["zsetCsum2","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCsum2","6"])).to eq 220.0
      expect(@runner.send("ZSCORE",["zsetCsum2","7"])).to eq 330.0

      ## AGGREGATE [MIN]
      args = {
        "key" => "zsetCmin",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "min"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCmin","1"])).to eq 120.0 
      expect(@runner.send("ZSCORE",["zsetCmin","2"])).to eq 220.0
      expect(@runner.send("ZSCORE",["zsetCmin","3"])).to eq 320.0
      expect(@runner.send("ZSCORE",["zsetCmin","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCmin","6"])).to eq 220.0
      ## AGGREGATE [MAX]
      args = {
        "key" => "zsetCmax",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "max"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCmax","1"])).to eq 200.0 
      expect(@runner.send("ZSCORE",["zsetCmax","2"])).to eq 400.0
      expect(@runner.send("ZSCORE",["zsetCmax","3"])).to eq 600.0
      expect(@runner.send("ZSCORE",["zsetCmax","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCmax","6"])).to eq 220.0
    end
    
    it "ZINTERSTORE" do
      @runner.send("ZADD",["zsetA",200,2])
      @runner.send("ZADD",["zsetA",100,1])
      @runner.send("ZADD",["zsetA",300,3])
      @runner.send("ZADD",["zsetB",220,20])
      @runner.send("ZADD",["zsetB",120,10])
      @runner.send("ZADD",["zsetB",320,3])
      ## BASIC 
      args = {
        "key" => "zsetC",
        "args" => ["zsetA","zsetB"],
        "option" => {}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZRANGE",["zsetC",0,5])).to eq ["3"]

      ## AGGREGATE [SUM]
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "sum"}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCsum",3])).to eq 920.0
      ## AGGREGATE [MIN]
      args = {
        "key" => "zsetCmin",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "min"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCmin",3])).to eq 320.0
      ## AGGREGATE [MAX]
      args = {
        "key" => "zsetCmax",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "max"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCmax",3])).to eq 600.0
    end
  end
  ## HASH
  context 'HASH Operation' do
    it "HSET/HGET" do
      @runner.send("HSET", ["key","field","value"])
      expect(@runner.send("HGET", ["key","field"])).to eq "value"
    end
    it "HMSET/HMGET" do
      args = {
        "key" => "key",
        "args" => ["field0","value0","field1","value1"]
      }
      expect(@runner.send("HMSET", args)).to eq "OK"
      args = {
        "key" => "key",
        "args" => ["field0","field1"]
      }
      expect(@runner.send("HMGET", args,false)).to eq ["value0","value1"]
    end
    it "HINCRBY" do
      @runner.send("HSET", ["key","field", 0])
      @runner.send("HINCRBY",["key","field",10])
      expect(@runner.send("HGET", ["key","field"])).to eq "10"
    end
    it "HEXISTS" do
      @runner.send("HSET", ["key","field", 0])
      expect(@runner.send("HEXISTS", ["key","field"])).to eq true
      expect(@runner.send("HEXISTS", ["key","field_no"])).to eq false
    end
    it "HDEL" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      @runner.send("HDEL", ["key","field0"])
      expect(@runner.send("HGET", ["key","field1"])).to eq "0"
      expect(@runner.send("HGET", ["key","field0"])).to eq nil
    end
    it "HLEN" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HLEN", ["key"])).to eq 2
    end
    it "HKEYS" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HKEYS", ["key"])).to eq ["field0","field1"]
    end
    it "HVALS" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HVALS", ["key"])).to eq ["0","0"]
    end
    it "HGETALL" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      hash = {"field0"=>"0", "field1"=>"0"}
      expect(@runner.send("HGETALL",["key"])).to eq hash
    end
  end
=end

end
RSpec.describe 'RedisOperation (C++ API) Unit Test [Reuse Connection & Async]' do
  before do
    @logger = DummyLogger.new
    @option = {
      :api => "cxx",
      :keepalive => true,
      :async => true
    }
    @option[:sourceDB] = "redis"
    @runner = RedisRunner.new("redis", @logger, @option)
  end
  ## STRINGS
  context 'STRING Operation' do
    before (:each) do
      @runner.send("FLUSHALL",[])
    end
    it "SET" do
      expect(@runner.send("SET",["test0","correct"])).to eq "OK"
      expect(@runner.send("SET",["test1","correct"])).to eq "OK"
      expect(@runner.send("SET",["test2","correct"])).to eq "OK"
      expect(@runner.send("SET",["test3","correct"])).to eq "OK"
    end
    it "GET" do
      @runner.send("SET",["test0","correct"])
      @runner.send("SET",["test1","correct"])
      @runner.send("SET",["test2","correct"])
      @runner.send("SET",["test3","correct"])
      expect(@runner.send("GET",["test0"],true)).to eq "correct"
    end
=begin
    it "DEL" do
      @runner.send("SET",["test","correct"])
      expect(@runner.send("DEL",["test"])).to eq 1
    end
    it "SETNX" do
      @runner.send("SET",["test","correct"])
      @runner.send("SETNX",["test","incorrect"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "SETEX" do
      @runner.send("SETEX",["test",1,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq nil
    end
    it "PSETEX" do
      @runner.send("PSETEX",["test",1000,"correct"])
      expect(@runner.send("GET",["test"])).to eq "correct"
      sleep(1)
      expect(@runner.send("GET",["test"])).to eq nil
    end
    it "MSET" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "MSETNX" do
      @runner.send("MSET",["test0","correct","test1","correct"])
      @runner.send("MSETNX",["test0","incorrect","test1","incorrect"])
      @runner.send("MSETNX",["test2","correct"])
      expect(@runner.send("GET",["test0"])).to eq "correct"
      expect(@runner.send("GET",["test1"])).to eq "correct"
    end
    it "INCR" do
      @runner.send("SET",["test",0])
      @runner.send("INCR",["test"])
      expect(@runner.send("GET",["test"])).to eq "1"
    end
    it "INCRBY" do
      @runner.send("SET",["test",0])
      @runner.send("INCRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "3"
    end
    it "DECR" do
      @runner.send("SET",["test",10])
      @runner.send("DECR",["test"])
      expect(@runner.send("GET",["test"])).to eq "9"
    end
    it "DECRBY" do
      @runner.send("SET",["test",10])
      @runner.send("DECRBY",["test",3])
      expect(@runner.send("GET",["test"])).to eq "7"
    end
    it "APPEND" do
      @runner.send("SET",["test","correc"])
      @runner.send("APPEND",["test","t"])
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
    it "GETSET" do
      @runner.send("SET",["test","pending"])
      expect(@runner.send("GETSET",["test","correct"])).to eq "pending"
      expect(@runner.send("GET",["test"])).to eq "correct"
    end
=end
  end
=begin
  ## SETS
  context 'SETS Operation' do
    it "SADD/SMEMBERS" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SMEMBERS",["test"])).to match_array ["elem2","elem1"]
    end
    it "SREM" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SREM",["test","elem1"])).to eq true
    end
    it "SPOP" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      @runner.send("SPOP",["test"])
      @runner.send("SPOP",["test"])
      expect(@runner.send("SMEMBERS",["test"])).to eq []
    end
    it "SMOVE" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      @runner.send("SMOVE",["test","dst","elem2"])
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem2"]
    end
    it "SCARD" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      expect(@runner.send("SCARD",["test"])).to eq 2
    end
    it "SINTER" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SINTER",["test0","test1"])).to eq ["elem1"]
    end
    it "SINTERSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SINTERSTORE",args)).to eq 1
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem1"]
    end
    it "SDIFF" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SDIFF",["test0","test1"])).to eq ["elem0"]
    end
    it "SDIFFSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SDIFFSTORE",args)).to eq 1
      expect(@runner.send("SMEMBERS",["dst"])).to eq ["elem0"]
    end
    it "SUNION" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      expect(@runner.send("SUNION",["test0","test1"])).to match_array ["elem0","elem1","elem2"]
    end
    it "SUNIONSTORE" do
      @runner.send("SADD",["test0","elem0"])
      @runner.send("SADD",["test0","elem1"])
      @runner.send("SADD",["test1","elem1"])
      @runner.send("SADD",["test1","elem2"])
      args = {
        "key" => "dst",
        "args" => ["test0","test1"]
      }
      expect(@runner.send("SUNIONSTORE",args)).to eq 3
      expect(@runner.send("SMEMBERS",["dst"])).to match_array ["elem0","elem1","elem2"]
    end
    it "SRANDMEMBER" do
      @runner.send("SADD",["test","elem1"])
      @runner.send("SADD",["test","elem2"])
      data = @runner.send("SRANDMEMBER",["test"])
      expect(["elem1","elem2"].include?(data)).to eq true
    end
  end
  ## SORTED SET
  context 'SORTED SET Operation' do
    it "ZADD" do
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",200,"e2"])
      expect(@runner.send("ZRANGE",["zset",0,2])).to eq ["e1","e2","e3"]
    end
    it "ZREM" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZREM",["zset","e1"])
      expect(@runner.send("ZRANGE",["zset",0,2])).to eq ["e2"]
    end
    it "ZINCRBY" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZINCRBY",["zset",1000,1])
      expect(@runner.send("ZRANGEBYSCORE",["zset",1000,2000])).to eq ["1"]
    end
    it "ZRANK" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZADD",["zset",300,3])
      expect(@runner.send("ZRANK",["zset",1])).to eq 0
      expect(@runner.send("ZRANK",["zset",2])).to eq 1
    end
    it "ZREVRANK" do
      @runner.send("ZADD",["zset",200,2])
      @runner.send("ZADD",["zset",100,1])
      @runner.send("ZADD",["zset",300,3])
      expect(@runner.send("ZREVRANK",["zset",1])).to eq 2
      expect(@runner.send("ZREVRANK",["zset",2])).to eq 1
    end
    it "ZRANGE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGE",["zset",1,2])).to eq ["e2","e3"]
    end
    it "ZREVRANGE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGE",["zset",1,2])).to eq ["e2","e3"]
    end
    it "ZRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZRANGEBYSCORE",["zset",200,300])).to eq ["e2","e3"]
    end
    it "ZCOUNT" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCOUNT",["zset",200,300])).to eq 2
    end
    it "ZCARD" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZCARD",["zset"])).to eq 3
    end
    it "ZSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      expect(@runner.send("ZSCORE",["zset","e2"])).to eq 200.0
    end
    it "ZREMRANGEBYRANK" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYRANK",["zset",1,2])
      expect(@runner.send("ZCARD",["zset"])).to eq 1
      expect(@runner.send("ZRANK",["zset","e1"])).to eq 0
    end
    it "ZREMRANGEBYSCORE" do
      @runner.send("ZADD",["zset",200,"e2"])
      @runner.send("ZADD",["zset",100,"e1"])
      @runner.send("ZADD",["zset",300,"e3"])
      @runner.send("ZREMRANGEBYSCORE",["zset",200,300])
      expect(@runner.send("ZCARD",["zset"])).to eq 1
      expect(@runner.send("ZRANK",["zset","e1"])).to eq 0
    end
    it "ZUNIONSTORE" do
      @runner.send("ZADD",["zsetA",200,2])
      @runner.send("ZADD",["zsetA",100,1])
      @runner.send("ZADD",["zsetA",300,3])
      @runner.send("ZADD",["zsetA",220,5])
      @runner.send("ZADD",["zsetB",220,2])
      @runner.send("ZADD",["zsetB",120,1])
      @runner.send("ZADD",["zsetB",320,3])
      @runner.send("ZADD",["zsetB",220,6])
      @runner.send("ZADD",["zsetD",230,1])
      @runner.send("ZADD",["zsetD",130,2])
      @runner.send("ZADD",["zsetD",330,3])
      @runner.send("ZADD",["zsetD",220,7])
      ## BASIC 
      args = {
        "key" => "zsetC",
        "args" => ["zsetA","zsetB"],
        "option" => {}
      }
      
      expect(@runner.send("ZUNIONSTORE", args)).to eq 5
      expect(@runner.send("ZRANGE",
          ["zsetC",0,5])).to eq ["1","5","6","2","3"]
      
      ## AGGREGATE [SUM]x2
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCsum","1"])).to eq 320.0 
      expect(@runner.send("ZSCORE",["zsetCsum","2"])).to eq 620.0
      expect(@runner.send("ZSCORE",["zsetCsum","3"])).to eq 920.0
      expect(@runner.send("ZSCORE",["zsetCsum","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCsum","6"])).to eq 220.0
      
      ## AGGREGATE [SUM]x3
      args = {
        "key" => "zsetCsum2",
        "args" => ["zsetA","zsetB","zsetD"],
        "option" => {
          :weights => ["2.0","1.0","1.5"],
          :aggregate => "sum"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 6
      expect(@runner.send("ZRANGE",
          ["zsetCsum2",0,5])).to eq  ["6", "7", "5", "1", "2", "3"]
      expect(@runner.send("ZSCORE",["zsetCsum2","1"])).to eq 665.0 
      expect(@runner.send("ZSCORE",["zsetCsum2","2"])).to eq 815.0
      expect(@runner.send("ZSCORE",["zsetCsum2","3"])).to eq 1415.0
      expect(@runner.send("ZSCORE",["zsetCsum2","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCsum2","6"])).to eq 220.0
      expect(@runner.send("ZSCORE",["zsetCsum2","7"])).to eq 330.0

      ## AGGREGATE [MIN]
      args = {
        "key" => "zsetCmin",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "min"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCmin","1"])).to eq 120.0 
      expect(@runner.send("ZSCORE",["zsetCmin","2"])).to eq 220.0
      expect(@runner.send("ZSCORE",["zsetCmin","3"])).to eq 320.0
      expect(@runner.send("ZSCORE",["zsetCmin","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCmin","6"])).to eq 220.0
      ## AGGREGATE [MAX]
      args = {
        "key" => "zsetCmax",
        "args" => ["zsetA","zsetB"],
        "option" => {
          :weights => ["2.0","1.0"],
          :aggregate => "max"
        }
      }
      expect(@runner.send("ZUNIONSTORE",args)).to eq 5
      expect(@runner.send("ZSCORE",["zsetCmax","1"])).to eq 200.0 
      expect(@runner.send("ZSCORE",["zsetCmax","2"])).to eq 400.0
      expect(@runner.send("ZSCORE",["zsetCmax","3"])).to eq 600.0
      expect(@runner.send("ZSCORE",["zsetCmax","5"])).to eq 440.0
      expect(@runner.send("ZSCORE",["zsetCmax","6"])).to eq 220.0
    end
    
    it "ZINTERSTORE" do
      @runner.send("ZADD",["zsetA",200,2])
      @runner.send("ZADD",["zsetA",100,1])
      @runner.send("ZADD",["zsetA",300,3])
      @runner.send("ZADD",["zsetB",220,20])
      @runner.send("ZADD",["zsetB",120,10])
      @runner.send("ZADD",["zsetB",320,3])
      ## BASIC 
      args = {
        "key" => "zsetC",
        "args" => ["zsetA","zsetB"],
        "option" => {}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZRANGE",["zsetC",0,5])).to eq ["3"]

      ## AGGREGATE [SUM]
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "sum"}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCsum",3])).to eq 920.0
      ## AGGREGATE [MIN]
      args = {
        "key" => "zsetCmin",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "min"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCmin",3])).to eq 320.0
      ## AGGREGATE [MAX]
      args = {
        "key" => "zsetCmax",
        "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "max"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCmax",3])).to eq 600.0
    end
  end
  ## HASH
  context 'HASH Operation' do
    it "HSET/HGET" do
      @runner.send("HSET", ["key","field","value"])
      expect(@runner.send("HGET", ["key","field"])).to eq "value"
    end
    it "HMSET/HMGET" do
      args = {
        "key" => "key",
        "args" => ["field0","value0","field1","value1"]
      }
      expect(@runner.send("HMSET", args)).to eq "OK"
      args = {
        "key" => "key",
        "args" => ["field0","field1"]
      }
      expect(@runner.send("HMGET", args,false)).to eq ["value0","value1"]
    end
    it "HINCRBY" do
      @runner.send("HSET", ["key","field", 0])
      @runner.send("HINCRBY",["key","field",10])
      expect(@runner.send("HGET", ["key","field"])).to eq "10"
    end
    it "HEXISTS" do
      @runner.send("HSET", ["key","field", 0])
      expect(@runner.send("HEXISTS", ["key","field"])).to eq true
      expect(@runner.send("HEXISTS", ["key","field_no"])).to eq false
    end
    it "HDEL" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      @runner.send("HDEL", ["key","field0"])
      expect(@runner.send("HGET", ["key","field1"])).to eq "0"
      expect(@runner.send("HGET", ["key","field0"])).to eq nil
    end
    it "HLEN" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HLEN", ["key"])).to eq 2
    end
    it "HKEYS" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HKEYS", ["key"])).to eq ["field0","field1"]
    end
    it "HVALS" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      expect(@runner.send("HVALS", ["key"])).to eq ["0","0"]
    end
    it "HGETALL" do
      @runner.send("HSET", ["key","field0", 0])
      @runner.send("HSET", ["key","field1", 0])
      hash = {"field0"=>"0", "field1"=>"0"}
      expect(@runner.send("HGETALL",["key"])).to eq hash
    end
  end
=end
end
