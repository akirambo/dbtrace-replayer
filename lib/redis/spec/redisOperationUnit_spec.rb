
require_relative "../../../spec/spec_helper"
require_relative "./env/redisOperationTester"

RSpec.describe 'RedisOperation Unit Test (Check Generated Command)' do
  before(:all) do
    @tester = RedisOperationTester.new()
  end
  ## STRINGS
  context 'STRING Operation' do
    it "set" do
      @tester.sync
      expect(@tester.send("set",(["test","correct"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["set test correct"]
    end

    it "get" do
      @tester.sync
      expect(@tester.send("get",["test"])).to eq "syncReply"
      expect(@tester.getCommand[0]).to eq "get test"
    end

    it "del" do
      @tester.sync
      expect(@tester.send("del",["test"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "del test"
    end

    it "setnx" do
      @tester.sync
      expect(@tester.send("setnx",["test","value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "setnx test value"
    end

    it "setex" do
      @tester.sync
      expect(@tester.send("setex",["test",1,"value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "setex test 1 value"
    end

    it "psetex" do
      @tester.sync
      expect(@tester.send("psetex",["test",100,"value"])).to eq "OK"
      expect(@tester.getCommand[0]).to eq "psetex test 100 value"
    end

    it "mset" do
      @tester.sync
      expect(@tester.send("mset",["test0","correct","test1","correct"])).to eq "OK"
      expect(@tester.getCommand).to eq ["mset test0 correct test1 correct"]
    end
    
    it "mget" do
      @tester.sync
      expect(@tester.send("mget",["test0"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["mget test0"]
    end

    it "msetnx" do
      @tester.sync
      expect(@tester.send("msetnx",["test2","correct"])).to eq "OK"
      expect(@tester.getCommand).to eq ["msetnx test2 correct"]
    end

    it "incr" do
      @tester.sync
      expect(@tester.send("incr",(["test"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["incr test"]
    end

    it "incrby" do
      @tester.sync
      expect(@tester.send("incrby",(["test",3]))).to eq "OK"
      expect(@tester.getCommand).to eq ["incrby test 3"]
    end
    
    it "decr" do
      @tester.sync
      expect(@tester.send("decr",(["test"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["decr test"]
    end
    
    it "decrby" do
      @tester.sync
      expect(@tester.send("decrby",(["test",3]))).to eq "OK"
      expect(@tester.getCommand).to eq ["decrby test 3"]
    end

    it "append" do
      @tester.sync
      expect(@tester.send("append",(["test","t"]))).to eq "OK"
      expect(@tester.getCommand).to eq ["append test t"]
    end
    
    it "getset" do
      @tester.sync
      expect(@tester.send("getset",(["test","after"]))).to eq "syncReply"
      expect(@tester.getCommand).to eq ["getset test after"]
    end

    it "strlen" do
      @tester.sync
      expect(@tester.send("strlen",(["test"]))).to eq "syncReply"
      expect(@tester.getCommand).to eq ["strlen test"]
    end
  end

  ## SETS
  context 'SETS Operation' do
    it "sadd" do
      @tester.sync
      args = {"key"=>"test","args"=>["elem1"]}
      expect(@tester.send("sadd",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["sadd test elem1"]
    end
    it "smembers" do
      @tester.sync
      expect(@tester.send("smembers",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["smembers test"]
    end
    
    it "sismember" do
      @tester.sync
      expect(@tester.send("sismember",["test","elem1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["sismember test elem1"]
    end
    it "srem" do
      @tester.sync
      expect(@tester.send("srem",["test","elem1"])).to eq "OK"
      expect(@tester.getCommand).to eq ["srem test elem1"]
    end
    it "spop" do
      @tester.sync
      expect(@tester.send("spop",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["spop test"]
    end
    it "smove" do
      @tester.sync
      expect(@tester.send("smove",["test","dst","elem2"])).to eq "OK"
      expect(@tester.getCommand).to eq ["smove test dst elem2"]
    end
    it "scard" do
      @tester.sync
      expect(@tester.send("scard",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["scard test"]
    end
    it "sinter" do
      @tester.sync
      expect(@tester.send("sinter",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["sinter test0 test1"]
    end
    it "sinterstore" do
      @tester.sync
      args = {"key" => "dst","args" => ["test0","test1"]}
      expect(@tester.send("sinterstore",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["sinterstore dst test0 test1"]
    end
    it "sdiff" do
      @tester.sync
      expect(@tester.send("sdiff",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["sdiff test0 test1"]
    end
    it "sdiffstore" do
      @tester.sync
      args = {"key" => "dst", "args" => ["test0","test1"]}
      expect(@tester.send("sdiffstore",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["sdiffstore dst test0 test1"]
    end
    it "sunion" do
      @tester.sync
      expect(@tester.send("sunion",["test0","test1"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["sunion test0 test1"]
    end
    it "sunionstore" do
      @tester.sync
      args = {"key" => "dst","args" => ["test0","test1"]}
      expect(@tester.send("sunionstore",args)).to eq "OK"
      expect(@tester.getCommand).to eq ["sunionstore dst test0 test1"]
    end
    it "srandmember" do
      @tester.sync
      expect(@tester.send("srandmember",["test"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["srandmember test"]
    end
  end
  ## SORTED SET
  context 'sorted set Operation' do
    it "zadd" do
      @tester.sync
      expect(@tester.send("zadd",["zset",300,"e3"])).to eq "OK"
      expect(@tester.getCommand).to eq ["zadd zset 300 e3"]
    end
    it "zrange" do
      @tester.sync
      expect(@tester.send("zrange",["zset",0,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrange zset 0 2"]
    end
    it "zrem" do
      @tester.sync
      expect(@tester.send("zrem",["zset","e1"])).to eq "OK"
      expect(@tester.getCommand).to eq ["zrem zset e1"]
    end
    it "zincrby" do
      @tester.sync
      expect(@tester.send("zincrby",["zset",1000,1])).to eq "OK"
      expect(@tester.getCommand).to eq ["zincrby zset 1000 1"]
    end
    it "zrank" do
      @tester.sync
      expect(@tester.send("zrank",["zset",1])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrank zset 1"]
    end
    it "zrevrank" do
      @tester.sync
      expect(@tester.send("zrevrank",["zset",1])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrevrank zset 1"]
    end
    it "zrange" do
      @tester.sync
      expect(@tester.send("zrange",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrange zset 1 2"]
    end
    it "zrevrange" do
      @tester.sync
      expect(@tester.send("zrevrange",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrevrange zset 1 2"]
    end
    it "zrangebyscore" do
      @tester.sync
      expect(@tester.send("zrangebyscore",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zrangebyscore zset 200 300"]
    end
    it "zcount" do
      @tester.sync
      expect(@tester.send("zcount",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zcount zset 200 300"]
    end
    it "zcard" do
      @tester.sync
      expect(@tester.send("zcard",["zset"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zcard zset"]
    end
    it "zscore" do
      @tester.sync
      expect(@tester.send("zscore",["zset","e2"])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zscore zset e2"]
    end
    it "zremrangebyrank" do
      @tester.sync
      expect(@tester.send("zremrangebyrank",["zset",1,2])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zremrangebyrank zset 1 2"]
    end
    it "zremrangebyscore" do
      @tester.sync
      expect(@tester.send("zremrangebyscore",["zset",200,300])).to eq "syncReply"
      expect(@tester.getCommand).to eq ["zremrangebyscore zset 200 300"]
    end
    it "zunionstore" do
      ## BASIC 
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],"option" => {}}
      expect(@tester.send("zunionstore", args)).to eq "OK"
      expect(@tester.getCommand).to eq ["zunionstore zsetC 2 zsetA zsetB"]
      
      ## AGGREGATE [SUM]x2
      @tester.sync
      args = {"key" => "zsetC", "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"],:aggregate => "sum"}}
      expect(@tester.send("zunionstore",args)).to eq "OK"
      ans = ["zunionstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate sum"]
      expect(@tester.getCommand).to eq ans
      
      ## AGGREGATE [SUM]x3
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB","zsetD"],
        "option" => {:weights => ["2.0","1.0","1.5"],:aggregate => "sum"}}
      expect(@tester.send("zunionstore",args)).to eq "OK"
      ans = ["zunionstore zsetC 3 zsetA zsetB zsetD weights 2.0 1.0 1.5 aggregate sum"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MIN]
      @tester.sync
      args = {"key" => "zsetC", "args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"],:aggregate => "min"}}
      expect(@tester.send("zunionstore",args)).to eq "OK"
      ans = ["zunionstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate min"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MAX]
      @tester.sync
      args = {
        "key" => "zsetC","args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"],:aggregate => "max"}}
      expect(@tester.send("zunionstore",args)).to eq "OK"
      ans = ["zunionstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate max"]
      expect(@tester.getCommand).to eq ans
    end
    
    it "zinterstore" do
      @tester.sync
      ## BASIC 
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],"option" => {}}
      expect(@tester.send("zinterstore",args)).to eq "OK"
      ans =  ["zinterstore zsetC 2 zsetA zsetB"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [SUM]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "sum"}}
      
      expect(@tester.send("zinterstore",args)).to eq "OK"
      ans = ["zinterstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate sum"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MIN]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "min"}}      
      expect(@tester.send("zinterstore", args)).to eq "OK"
      ans = ["zinterstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate min"]
      expect(@tester.getCommand).to eq ans

      ## AGGREGATE [MAX]
      @tester.sync
      args = {"key" => "zsetC","args" => ["zsetA","zsetB"],
        "option" => {:weights => ["2.0","1.0"], :aggregate => "max"}
      }
      expect(@tester.send("zinterstore", args)).to eq "OK"
      ans = ["zinterstore zsetC 2 zsetA zsetB weights 2.0 1.0 aggregate max"]
      expect(@tester.getCommand).to eq ans
    end
  end
  ## LIST
  context 'LIST Operation' do
    it "lpush" do
      @tester.sync
      expect(@tester.send("lpush", ["test_lpush","e1"])).to eq "OK"
      ans = ["lpush test_lpush e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "lrange" do
      @tester.sync
      expect(@tester.send("lrange", ["test_lpush",0,-1])).to eq "syncReply"
      ans = ["lrange test_lpush 0 -1"]
      expect(@tester.getCommand).to eq ans
    end
    it "rpush" do
      @tester.sync
      expect(@tester.send("rpush", ["test_rpush","e1"])).to eq "OK"
      ans = ["rpush test_rpush e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "lpop" do
      @tester.sync
      expect(@tester.send("lpop", ["test_lpop"])).to eq "OK"
      ans = ["lpop test_lpop"]
      expect(@tester.getCommand).to eq ans
    end
    it "rpop" do
      @tester.sync
      expect(@tester.send("rpop", ["test_rpop"])).to eq "OK"
      ans = ["rpop test_rpop"]
      expect(@tester.getCommand).to eq ans
    end
    it "lrem" do 
      @tester.sync
      expect(@tester.send("lrem", ["test_lrem",-2,"e1"])).to eq "OK"
      ans = ["lrem test_lrem -2 e1"]
      expect(@tester.getCommand).to eq ans
    end
    it "lindex" do
      @tester.sync
      expect(@tester.send("lindex", ["test_lindex",0])).to eq "syncReply"
      ans = ["lindex test_lindex 0"]
      expect(@tester.getCommand).to eq ans
    end
    it "rpoplpush" do
      @tester.sync
      expect(@tester.send("rpoplpush", ["test_rplp","e2"])).to eq "syncReply"
      ans = ["rpoplpush test_rplp e2"]
      expect(@tester.getCommand).to eq ans
    end
    it "lset" do
      @tester.sync
      expect(@tester.send("lset", ["test_lset",-1,"f2"])).to eq "OK"
      ans = ["lset test_lset -1 f2"]
      expect(@tester.getCommand).to eq ans
    end
    it "ltrim" do
      @tester.sync
      expect(@tester.send("ltrim", ["test_ltrim",1,-1])).to eq "OK"
      ans = ["ltrim test_ltrim 1 -1"]
      expect(@tester.getCommand).to eq ans
    end
    it "llen" do
      @tester.sync
      expect(@tester.send("llen", ["test_llen"])).to eq "syncReply"
      ans = ["llen test_llen"]
      expect(@tester.getCommand).to eq ans
    end
  end
  ## HASH
  context 'HASH Operation' do
    it "hset" do
      @tester.sync
      expect(@tester.send("hset", ["key","field","value"])).to eq "OK"
      ans =  ["hset key field value"]
      expect(@tester.getCommand).to eq ans
    end
    it "hget" do
      @tester.sync
      expect(@tester.send("hget", ["key","field"])).to eq "syncReply"
      ans =  ["hget key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "hmset" do
      @tester.sync
      args = {"key" => "key", "args" => ["field0","value0","field1","value1"]} 
      expect(@tester.send("hmset", args)).to eq "OK"
      ans =  ["hmset key field0 value0 field1 value1"]
      expect(@tester.getCommand).to eq ans
    end
    it "hmget" do
      @tester.sync
      args = {"key" => "key","args" => ["field0","field1"]}
      expect(@tester.send("hmget", args,false)).to eq "syncReply"
      ans =  ["hmget key field0 field1"]
      expect(@tester.getCommand).to eq ans
    end
    it "hincrby" do
      @tester.sync
      expect(@tester.send("hincrby",["key","field",10])).to eq "OK"
      ans =  ["hincrby key field 10"]
      expect(@tester.getCommand).to eq ans
    end
    it "hexists" do
      @tester.sync
      expect(@tester.send("hexists", ["key","field"])).to eq "syncReply"
      ans =  ["hexists key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "hdel" do
      @tester.sync
      expect(@tester.send("hdel", ["key","field"])).to eq "OK"
      ans =  ["hdel key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "hlen" do
      @tester.sync
      expect(@tester.send("hlen", ["key"])).to eq "syncReply"
      ans =  ["hlen key"]
      expect(@tester.getCommand).to eq ans
    end
    it "hkeys" do
      @tester.sync
      expect(@tester.send("hkeys", ["key"])).to eq "syncReply"
      ans =  ["hkeys key"]
      expect(@tester.getCommand).to eq ans
    end
    it "hvals" do
      @tester.sync
      expect(@tester.send("hvals", ["key"])).to eq "syncReply"
      ans =  ["hvals key"]
      expect(@tester.getCommand).to eq ans
    end
    it "hgetall" do
      @tester.sync
      expect(@tester.send("hgetall",["key"])).to eq "syncReply"
      ans =  ["hgetall key"]
      expect(@tester.getCommand).to eq ans
    end
  end
  context "flushall" do
    it "Sync with args and initFlag=false" do
      ## Pattern 1
      @tester.sync
      expect(@tester.send("flushall",["key","field"],false)).to eq "OK"
      ans = ["flushall key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with args and initFlag=true" do
      ## Pattern 2
      @tester.sync
      expect(@tester.send("flushall",["key","field"],true)).to eq "OK"
      ans = ["flushall key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with initFlag=false" do
      ## Pattern 3
      @tester.sync
      expect(@tester.send("flushall",[],false)).to eq "OK"
      ans = ["flushall"]
      expect(@tester.getCommand).to eq ans
    end
    it "Sync with initFlag=true" do
      ## Pattern 4
      @tester.sync
      expect(@tester.send("flushall",[],true)).to eq "OK"
      ans = ["flushall"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with args and initFlag=false" do
      ## Pattern 5
      @tester.async
      expect(@tester.send("flushall",["key","field"],false)).to eq "OK"
      ans = ["flushall key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with args and initFlag=true" do
      ## Pattern 6
      @tester.async
      expect(@tester.send("flushall",["key","field"],true)).to eq "OK"
      ans = ["flushall key field"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with initFlag=false" do
      ## Pattern 7
      @tester.async
      expect(@tester.send("flushall",[],false)).to eq "OK"
      ans = ["flushall"]
      expect(@tester.getCommand).to eq ans
    end
    it "Async with initFlag=true" do
      ## Pattern 8
      @tester.async
      expect(@tester.send("flushall",[],true)).to eq "OK"
      ans = ["flushall"]
      expect(@tester.getCommand).to eq ans
    end
  end
  context "Common MethLod test" do
    it "keys type='keyspace'" do
      @tester.sync
      expect(@tester.send("keys","k1","keyspace")).to match_array ["k1.t1","k1.t2"]
    end
    it "keys type='table'" do
      @tester.sync
      expect(@tester.send("keys","t1","table")).to match_array ["k1.t1","k2.t1"]
    end
    it "prepare_redis" do
      expect(@tester.send("prepare_redis","zunionstore",[])["args"]).to eq "extract_z_x_store_args"
      expect(@tester.send("prepare_redis","hmget",[])["args"]).to eq "args2key_args"
    end
  end
  context "CXX Executer" do
    it "redis_cxx_reply" do
      @tester.sync
      expect(@tester.send("redis_cxx_reply")).to eq "syncReply"
      @tester.async
      expect(@tester.send("redis_cxx_reply")).to eq "asyncReply"
    end
    it "redis_cxx_executer (sync)" do
      @tester.sync
      expect(@tester.send("redis_cxx_executer","dummy","dummy",false,false)).to eq "OK"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",false,true)).to eq "OK"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",true,false)).to eq "syncReply"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",true,true)).to eq "syncReply"
    end
    it "redis_cxx_executer (async)" do
      @tester.async
      expect(@tester.send("redis_cxx_executer","dummy","dummy",false,false)).to eq "OK"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",false,true)).to eq "OK"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",true,false)).to eq "OK"
      expect(@tester.send("redis_cxx_executer","dummy","dummy",true,true)).to eq "asyncReply"
    end
    it "redis_async_executer" do
      # Pattern 1
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redis_async_executer",nil,false)).to eq "OK"
      # Pattern 2
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redis_async_executer","dummy",true)).to eq "OK"
      # Pattern 3
      @tester.async
      @tester.setPooledQuerySize(2)
      expect(@tester.send("redis_async_executer","dummy",false)).to eq "OK"
      # Pattern 4
      @tester.async
      @tester.setPoolRequestSize(256)
      expect(@tester.send("redis_async_executer","dummy",false)).to eq "OK"
      # Pattern 5
      @tester.async
      @tester.setPooledQuerySize(-1)
      expect(@tester.send("redis_async_executer","dummy",true)).to eq "OK"
    end
    it "redis_optionhash2command" do
      # Pattern 1 
      hash = {:test => "a", "test2" => "b", "test3" => ["c","d"], :test4 => ["e","f"]}
      ans = " test a test2 b test3 c d test4 e f"
      expect(@tester.send("redis_optionhash2command",hash)).to eq ans
    end
  end
end
