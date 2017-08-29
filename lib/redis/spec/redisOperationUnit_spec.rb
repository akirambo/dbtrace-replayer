
require_relative "../../../spec/spec_helper"
require_relative "./env/redisOperationTester"

RSpec.describe 'RedisOperation Unit Test (Check Generated Command)' do
  before(:all) do
    @tester = RedisOperationTester.new()
  end
  ## STRINGS
  context 'STRING Operation' do
    it "SET" do
      @tester.sync
      expect(@tester.send("SET",(["test","correct"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["SET test correct"]
    end

    it "GET" do
      @tester.sync
      expect(@tester.send("GET",["test"])).to eq "syncReply"
      expect(@tester.getCommand[0]).to eq "GET test"
    end
    it "DEL" do
      @tester.sync
      expect(@tester.send("DEL",["test"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "DEL test"
    end

    it "SETNX" do
      @tester.sync
      expect(@tester.send("SETNX",["test","value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "SETNX test value"
    end

    it "SETEX" do
      @tester.sync
      expect(@tester.send("SETEX",["test",1,"value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "SETEX test 1 value"
    end
    it "PSETEX" do
      @tester.sync
      expect(@tester.send("PSETEX",["test",100,"value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "PSETEX test 100 value"
    end
    it "MSET" do
      @tester.sync
      expect(@tester.send("MSET",["test0","correct","test1","correct"])).to eq "OK"
      expect(@tester.getCommand).to eq ["MSET test0 correct test1 correct"]
    end
    it "MGET" do
      @tester.sync
      expect(@tester.send("MGET",["test0"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["MGET test0"]
    end
    it "MSETNX" do
      @tester.sync
      expect(@tester.send("MSETNX",["test2","correct"])).to eq "OK"
      expect(@tester.getCommand).to eq ["MSETNX test2 correct"]
    end
    it "INCR" do
      @tester.sync
      expect(@tester.send("INCR",(["test"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["INCR test"]
    end
    it "INCRBY" do
      @tester.sync
      expect(@tester.send("INCRBY",(["test",3]))).to eq "OK"
      expect(@tester.getCommand).to eq ["INCRBY test 3"]
    end
    it "DECR" do
      @tester.sync
      expect(@tester.send("DECR",(["test"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["DECR test"]
    end
    it "DECRBY" do
      @tester.sync
      expect(@tester.send("DECRBY",(["test",3]))).to eq "OK"
      expect(@tester.getCommand).to eq ["DECRBY test 3"]
    end
    it "APPEND" do
      @tester.sync
      expect(@tester.send("APPEND",(["test","t"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["APPEND test t"]
    end
    it "GETSET" do
      @tester.sync
      expect(@tester.send("GETSET",(["test","after"]))).to eq "syncReply"
      expect(@tester.getCommand).to eq ["GETSET test after"]
    end
    it "STRLEN" do
      @tester.sync
      expect(@tester.send("STRLEN",(["test"]))).to eq "syncReply"
      expect(@tester.getCommand).to eq ["STRLEN test"]
    end
  end

  ## SETS
  context 'SETS Operation' do
    it "SADD" do
      @tester.sync
      args = {"key"=>"test","args"=>"elem1"}
      expect(@tester.send("SADD",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["SADD test elem1"]
    end
    it "SMEMBERS" do
      @tester.sync
      expect(@tester.send("SMEMBERS",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SMEMBERS test"]
    end
    it "SISMEMBER" do
      @tester.sync
      expect(@tester.send("SISMEMBER",["test","elem1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SISMEMBER test elem1"]
    end
    it "SREM" do
      @tester.sync
      expect(@tester.send("SREM",["test","elem1"])).to eq "OK"
      expect(@tester.getCommand).to eq ["SREM test elem1"]
    end
    it "SPOP" do
      @tester.sync
      expect(@tester.send("SPOP",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SPOP test"]
    end
    it "SMOVE" do
      @tester.sync
      expect(@tester.send("SMOVE",["test","dst","elem2"])).to eq "OK"
      expect(@tester.getCommand).to eq ["SMOVE test dst elem2"]
    end
    it "SCARD" do
      @tester.sync
      expect(@tester.send("SCARD",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SCARD test"]
    end
    it "SINTER" do
      @tester.sync
      expect(@tester.send("SINTER",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SINTER test0 test1"]
    end
    it "SINTERSTORE" do
      @tester.sync
      args = {"key" => "dst","args" => ["test0","test1"]}
      expect(@tester.send("SINTERSTORE",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["SINTERSTORE dst test0 test1"]
    end
    it "SDIFF" do
      @tester.sync
      expect(@tester.send("SDIFF",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SDIFF test0 test1"]
    end
    it "SDIFFSTORE" do
      @tester.sync
      args = {"key" => "dst", "args" => ["test0","test1"]}
      expect(@tester.send("SDIFFSTORE",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["SDIFFSTORE dst test0 test1"]
    end
    it "SUNION" do
      @tester.sync
      expect(@tester.send("SUNION",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SUNION test0 test1"]
    end
    it "SUNIONSTORE" do
      @tester.sync
      args = {"key" => "dst","args" => ["test0","test1"]}
      expect(@tester.send("SUNIONSTORE",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["SUNIONSTORE dst test0 test1"]
    end
    it "SRANDMEMBER" do
      @tester.sync
      expect(@tester.send("SRANDMEMBER",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["SRANDMEMBER test"]
    end
  end
  ## SORTED SET
  context 'SORTED SET Operation' do
    it "ZADD" do
      @tester.sync
      expect(@tester.send("ZADD",["zset",300,"e3"])).to eq "OK"
      expect(@tester.getCommand).to eq ["ZADD zset 300 e3"]
    end
    it "ZRANGE" do
      @tester.sync
      expect(@tester.send("ZRANGE",["zset",0,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZRANGE zset 0 2"]
    end
    it "ZREM" do
      @tester.sync
      expect(@tester.send("ZREM",["zset","e1"])).to eq "OK"
      expect(@tester.getCommand).to eq ["ZREM zset e1"]
    end
    it "ZINCRBY" do
      @tester.sync
      expect(@tester.send("ZINCRBY",["zset",1000,1])).to eq "OK"
      expect(@tester.getCommand).to eq ["ZINCRBY zset 1000 1"]
    end
    it "ZRANK" do
      @tester.sync
      expect(@tester.send("ZRANK",["zset",1])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZRANK zset 1"]
    end
    it "ZREVRANK" do
      @tester.sync
      expect(@tester.send("ZREVRANK",["zset",1])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZREVRANK zset 1"]
    end
    it "ZRANGE" do
      @tester.sync
      expect(@tester.send("ZRANGE",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZRANGE zset 1 2"]
    end
    it "ZREVRANGE" do
      @tester.sync
      expect(@tester.send("ZREVRANGE",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZREVRANGE zset 1 2"]
    end
    it "ZRANGEBYSCORE" do
      @tester.sync
      expect(@tester.send("ZRANGEBYSCORE",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZRANGEBYSCORE zset 200 300"]
    end
    it "ZCOUNT" do
      @tester.sync
      expect(@tester.send("ZCOUNT",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZCOUNT zset 200 300"]
    end
    it "ZCARD" do
      @tester.sync
      expect(@tester.send("ZCARD",["zset"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZCARD zset"]
    end
    it "ZSCORE" do
      @tester.sync
      expect(@tester.send("ZSCORE",["zset","e2"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZSCORE zset e2"]
    end
    it "ZREMRANGEBYRANK" do
      @tester.sync
      expect(@tester.send("ZREMRANGEBYRANK",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZREMRANGEBYRANK zset 1 2"]
    end
    it "ZREMRANGEBYSCORE" do
      @tester.sync
      expect(@tester.send("ZREMRANGEBYSCORE",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["ZREMRANGEBYSCORE zset 200 300"]
    end
    it "ZUNIONSTORE" do
      ## BASIC 
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],"options" => {}}
      expect(@tester.send("ZUNIONSTORE", args)).to eq "OK"
      expect(@tester.getCommand).to eq ["ZUNIONSTORE zsetC 2 zsetA zsetB"]
      
      ## AGGREGATE [SUM]x2
      @tester.sync
      args = {"key" => "zsetC", "args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"],:aggregate => "sum"}}
      expect(@tester.send("ZUNIONSTORE",args)).to eq "OK"
      ans =  ["ZUNIONSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate sum"]
      expect(@tester.getCommand).to eq ans
      
      ## AGGREGATE [SUM]x3
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB","zsetD"],
        "options" => {:weights => ["2.0","1.0","1.5"],:aggregate => "sum"}}
      expect(@tester.send("ZUNIONSTORE",args)).to eq "OK"
      ans =  ["ZUNIONSTORE zsetC 3 zsetA zsetB zsetD weights 2.0 1.0 1.5 aggregate sum"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MIN]
      @tester.sync
      args = {"key" => "zsetC", "args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"],:aggregate => "min"}}
      expect(@tester.send("ZUNIONSTORE",args)).to eq "OK"
      ans =  ["ZUNIONSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate min"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MAX]
      @tester.sync
      args = {
        "key" => "zsetC","args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"],:aggregate => "max"}}
      expect(@tester.send("ZUNIONSTORE",args)).to eq "OK"
      ans =  ["ZUNIONSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate max"]
      expect(@tester.getCommand).to eq ans
    end
    
    it "ZINTERSTORE" do
      @tester.sync
      ## BASIC 
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],"options" => {}}
      expect(@tester.send("ZINTERSTORE",args)).to eq "OK"
      ans =  ["ZINTERSTORE zsetC 2 zsetA zsetB"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [SUM]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "sum"}}
      
      expect(@tester.send("ZINTERSTORE",args)).to eq "OK"
      ans =  ["ZINTERSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate sum"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MIN]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "min"}}      
      expect(@tester.send("ZINTERSTORE", args)).to eq "OK"
      ans =  ["ZINTERSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate min"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MAX]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "max"}
      }
      expect(@tester.send("ZINTERSTORE", args)).to eq "OK"
      ans =  ["ZINTERSTORE zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate max"]
      expect(@tester.getCommand).to eq ans
    end
  end
  ## LIST
  context 'LIST Operation' do
    it "LPUSH" do
      @tester.sync
      expect(@tester.send("LPUSH", ["test_lpush","e1"])).to eq "OK"
      ans =  ["LPUSH test_lpush e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "LRANGE" do
      @tester.sync
      expect(@tester.send("LRANGE", ["test_lpush",0,-1])).to eq "syncReply"
      ans =  ["LRANGE test_lpush 0 -1"]
      expect(@tester.getCommand).to eq ans
    end
    it "RPUSH" do
      @tester.sync
      expect(@tester.send("RPUSH", ["test_rpush","e1"])).to eq "OK"
      ans =  ["RPUSH test_rpush e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "LPOP" do
      @tester.sync
      expect(@tester.send("LPOP", ["test_lpop"])).to eq "OK"
      ans =  ["LPOP test_lpop"]
      expect(@tester.getCommand).to eq ans
    end
    it "RPOP" do
      @tester.sync
      expect(@tester.send("RPOP", ["test_rpop"])).to eq "OK"
      ans =  ["RPOP test_rpop"]
      expect(@tester.getCommand).to eq ans
    end
    it "LREM" do 
      @tester.sync
      expect(@tester.send("LREM", ["test_lrem",-2,"e1"])).to eq "OK"
      ans =  ["LREM test_lrem -2 e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "LINDEX" do
      @tester.sync
      expect(@tester.send("LINDEX", ["test_lindex",0])).to eq "syncReply"
      ans =  ["LINDEX test_lindex 0"]
      expect(@tester.getCommand).to eq ans
    end
    it "RPOPLPUSH" do
      @tester.sync
      expect(@tester.send("RPOPLPUSH", ["test_rplp","e2"])).to eq "syncReply"
      ans =  ["RPOPLPUSH test_rplp e2"]
      expect(@tester.getCommand).to eq ans
    end
    it "LSET" do
      @tester.sync
      expect(@tester.send("LSET", ["test_lset",-1,"f2"])).to eq "OK"
      ans =  ["LSET test_lset -1 f2"]
      expect(@tester.getCommand).to eq ans
    end
    it "LTRIM" do
      @tester.sync
      expect(@tester.send("LTRIM", ["test_ltrim",1,-1])).to eq "OK"
      ans =  ["LTRIM test_ltrim 1 -1"]
      expect(@tester.getCommand).to eq ans
    end
    it "LLEN" do
      @tester.sync
      expect(@tester.send("LLEN", ["test_llen"])).to eq "syncReply"
      ans =  ["LLEN test_llen"]
      expect(@tester.getCommand).to eq ans
    end
  end
  ## HASH
  context 'HASH Operation' do
    it "HSET" do
      @tester.sync
      expect(@tester.send("HSET", ["key","field","value"])).to eq "OK"
      ans =  ["HSET key field value"]
      expect(@tester.getCommand).to eq ans
    end
    it "HGET" do
      @tester.sync
      expect(@tester.send("HGET", ["key","field"])).to eq "syncReply"
      ans =  ["HGET key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "HMSET" do
      @tester.sync
      args = {"key" => "key", "args" => ["field0","value0","field1","value1"]} 
      expect(@tester.send("HMSET", args)).to eq "OK"
      ans =  ["HMSET key field0 value0 field1 value1"]
      expect(@tester.getCommand).to eq ans
    end
    it "HMGET" do
      @tester.sync
      args = {"key" => "key","args" => ["field0","field1"]}
      expect(@tester.send("HMGET", args,false)).to eq "syncReply"
      ans =  ["HMGET key field0 field1"]
      expect(@tester.getCommand).to eq ans
    end
    it "HINCRBY" do
      @tester.sync
      expect(@tester.send("HINCRBY",["key","field",10])).to eq "OK"
      ans =  ["HINCRBY key field 10"]
      expect(@tester.getCommand).to eq ans
    end
    it "HEXISTS" do
      @tester.sync
      expect(@tester.send("HEXISTS", ["key","field"])).to eq "syncReply"
      ans =  ["HEXISTS key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "HDEL" do
      @tester.sync
      expect(@tester.send("HDEL", ["key","field"])).to eq "OK"
      ans =  ["HDEL key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "HLEN" do
      @tester.sync
      expect(@tester.send("HLEN", ["key"])).to eq "syncReply"
      ans =  ["HLEN key"]
      expect(@tester.getCommand).to eq ans
    end
    it "HKEYS" do
      @tester.sync
      expect(@tester.send("HKEYS", ["key"])).to eq "syncReply"
      ans =  ["HKEYS key"]
      expect(@tester.getCommand).to eq ans
    end
    it "HVALS" do
      @tester.sync
      expect(@tester.send("HVALS", ["key"])).to eq "syncReply"
      ans =  ["HVALS key"]
      expect(@tester.getCommand).to eq ans
    end
    it "HGETALL" do
      @tester.sync
      expect(@tester.send("HGETALL",["key"])).to eq "syncReply"
      ans =  ["HGETALL key"]
      expect(@tester.getCommand).to eq ans
    end
  end
  context "FLUSHALL" do
    it "Sync with args and initFlag=false" do
      ## Pattern 1
      @tester.sync
      expect(@tester.send("FLUSHALL",["key","field"],false)).to eq "OK"
      ans = ["FLUSHALL key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with args and initFlag=true" do
      ## Pattern 2
      @tester.sync
      expect(@tester.send("FLUSHALL",["key","field"],true)).to eq "OK"
      ans = ["FLUSHALL key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with initFlag=false" do
      ## Pattern 3
      @tester.sync
      expect(@tester.send("FLUSHALL",[],false)).to eq "OK"
      ans = ["FLUSHALL"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with initFlag=true" do
      ## Pattern 4
      @tester.sync
      expect(@tester.send("FLUSHALL",[],true)).to eq "OK"
      ans = ["FLUSHALL"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with args and initFlag=false" do
      ## Pattern 5
      @tester.async
      expect(@tester.send("FLUSHALL",["key","field"],false)).to eq "OK"
      ans = ["FLUSHALL key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with args and initFlag=true" do
      ## Pattern 6
      @tester.async
      expect(@tester.send("FLUSHALL",["key","field"],true)).to eq "OK"
      ans = ["FLUSHALL key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with initFlag=false" do
      ## Pattern 7
      @tester.async
      expect(@tester.send("FLUSHALL",[],false)).to eq "OK"
      ans = ["FLUSHALL"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with initFlag=true" do
      ## Pattern 8
      @tester.async
      expect(@tester.send("FLUSHALL",[],true)).to eq "OK"
      ans = ["FLUSHALL"]
      expect(@tester.getCommand).to eq ans
    end
  end
  context "Common Method Test" do
    it "KEYS type='keyspace'" do
      @tester.sync
      expect(@tester.send("KEYS","k1","keyspace")).to match_array ["k1.t1","k1.t2"]
    end
    it "KEYS type='table'" do
      @tester.sync
      expect(@tester.send("KEYS","t1","table")).to match_array ["k1.t1","k2.t1"]
    end
    it "prepare_REDIS" do
      expect(@tester.send("prepare_REDIS","ZUNIONSTORE",[])["args"]).to eq "extractZ_X_STORE_ARGS"
      expect(@tester.send("prepare_REDIS","MSET",[])["args"]).to eq "args2hash"
      expect(@tester.send("prepare_REDIS","HMGET",[])["args"]).to eq "args2key_args"
    end
  end
  context "CXX Executer" do
    it "redisCxxReply" do
      @tester.sync
      expect(@tester.send("redisCxxReply")).to eq "syncReply"
      @tester.async
      expect(@tester.send("redisCxxReply")).to eq "asyncReply"
    end
    it "redisCxxExecuter (sync)" do
      @tester.sync
      expect(@tester.send("redisCxxExecuter","dummy","dummy",false,false)).to eq "OK"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",false,true)).to eq "OK"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",true,false)).to eq "syncReply"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",true,true)).to eq "syncReply"
    end
    it "redisCxxExecuter (async)" do
      @tester.async
      expect(@tester.send("redisCxxExecuter","dummy","dummy",false,false)).to eq "OK"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",false,true)).to eq "OK"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",true,false)).to eq "OK"
      expect(@tester.send("redisCxxExecuter","dummy","dummy",true,true)).to eq "asyncReply"
    end
    it "redisAsyncExecuter" do
      # Pattern 1
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redisAsyncExecuter",nil,false)).to eq "OK"
      # Pattern 2
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redisAsyncExecuter","dummy",true)).to eq "OK"
      # Pattern 3
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redisAsyncExecuter","dummy",false)).to eq "OK"
      # Pattern 4
      @tester.async
      @tester.setPoolRequestSize(256)
      expect(@tester.send("redisAsyncExecuter","dummy",false)).to eq "OK"
      # Pattern 5
      @tester.async
      @tester.setPooledQuerySize(-1)
      expect(@tester.send("redisAsyncExecuter","dummy",true)).to eq "OK"
    end
    it "redisOptionHash2Command" do
      # Pattern 1 
      hash = {:test => "a", "test2" => "b", "test3" => ["c","d"], :test4 => ["e","f"]}
      ans = " test a test2 b test3 c d test4 e f"
      expect(@tester.send("redisOptionHash2Command",hash)).to eq ans
    end
  end
end
