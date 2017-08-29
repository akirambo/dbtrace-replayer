
require_relative "../../../spec/spec_helper"
require_relative "../src/redisRunner"

RSpec.describe 'RedisOperation (Ruby API) Test With Database' do
  before do
    @logger = DummyLogger.new
    @options = {
      :api => "ruby"
    }
    @options[:sourceDB] = "redis"
    @runner = RedisRunner.new("redis", @logger, @options)
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
  end
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
        "options" => {}
      }
      
      expect(@runner.send("ZUNIONSTORE", args)).to eq 5
      expect(@runner.send("ZRANGE",
          ["zsetC",0,5])).to eq ["1","5","6","2","3"]
      
      ## AGGREGATE [SUM]x2
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "options" => {
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
        "options" => {
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
        "options" => {
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
        "options" => {
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
        "options" => {}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZRANGE",["zsetC",0,5])).to eq ["3"]

      ## AGGREGATE [SUM]
      args = {
        "key" => "zsetCsum",
        "args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "sum"}
      }
      expect(@runner.send("ZINTERSTORE",args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCsum",3])).to eq 920.0
      ## AGGREGATE [MIN]
      args = {
        "key" => "zsetCmin",
        "args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "min"}
      }
      expect(@runner.send("ZINTERSTORE", args)).to eq 1
      expect(@runner.send("ZSCORE",["zsetCmin",3])).to eq 320.0
      ## AGGREGATE [MAX]
      args = {
        "key" => "zsetCmax",
        "args" => ["zsetA","zsetB"],
        "options" => {:weights => ["2.0","1.0"], :aggregate => "max"}
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
end
