# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redis2MongodbOperation"

module Redis2MongodbTester
  class ParserMock
    def extractZ_X_STORE_ARGS(a)
      return "#{__method__}"
    end
    def args2hash(a)
      return "#{__method__}"
    end
    def args2key_args(a)
      return "#{__method__}"
    end
    def args2key_hash(a)
      return "#{__method__}"
    end
  end
  class Mock
    attr_reader :command, :value
    include Redis2MongodbOperation
    def initialize
      @logger = DummyLogger.new
      @parser = ParserMock.new
      @command = nil
      @value   = nil
      @queryReturn = false
      @findReturn = false
      @findReturnHashFlag = false
      @aggregationReturn = nil
    end
    def setQueryValue(ret)
      @queryReturn = ret
    end
    def setFindValue(ret,bool=false)
      @findReturn = ret
      @findReturnHashFlag = bool
    end
    def setAggregationValue(ret)
      @aggregationReturn = ret
    end
    def INSERT(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def FIND(a)
      @value = a
      @command = "#{__method__}"
      if(@findReturnHashFlag)then
        return @findReturn[a["key"]]
      end
      return @findReturn
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
    def AGGREGATE(a)
      @value = a
      @command = "#{__method__}"
      return @aggregationReturn
    end
    def COUNT(a)
      @value = a
      @command = "#{__method__}"
      return 10
    end
    def DROP(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
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

  RSpec.describe 'Redis2Mongodb Unit TEST' do
    before do
      @tester = Mock.new()
    end
    context "String Operation" do
      it "REDIS_SET" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SET,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_SETEX" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SETEX,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_SETNX" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SETNX,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_PSETEX" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_PSETEX,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_GET" do
        args = ["id00","value00"]
        @tester.setFindValue([{"value"=>"results"}])
        expect(@tester.send(:REDIS_GET,args)).to eq "results"
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_MSET" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_MSET,args)).to be true
        expect(@tester.command).to eq "INSERT"
        @tester.setQueryValue(false)
        expect(@tester.send(:REDIS_MSET,args)).to be false
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_MSETNX" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_MSETNX,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_MGET" do
        args = ["id00","id01"]
        @tester.setFindValue([{"value"=>"results"},{"value"=>"results"}])
        expect(@tester.send(:REDIS_MGET,args)).to match_array ["results","results"]
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_INCR" do
        args = ["id00"]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_INCR,args)).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_INCRBY" do
        args = ["id00",1]
      @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_INCRBY,args)).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_DECR" do
        args = ["id00"]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_DECR,args)).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_DECRBY" do
        args = ["id00",1]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_DECRBY,args)).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_APPEND" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_APPEND,args)).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_GETSET" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_GETSET,args)).to eq "t"
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_STRLEN" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_STRLEN,args)).to eq 1
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_DEL" do
        args = ["id00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_DEL,args)).to be true
        expect(@tester.command).to eq "DELETE"
      end
    end
    context "LIST" do
      it "REDIS_LPUSH" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LPUSH,["a","b"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_RPUSH" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_RPUSH,["a","b"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_LLEN" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LLEN,["a"])).to eq 10
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_LRANGE" do
        values = [{"value"=>"a"},{"value"=>"a"},{"value"=>"a"}]
        @tester.setFindValue(values)
        expect(@tester.send(:REDIS_LRANGE,["a","0","-1"])).to eq ["a","a","a"]
      end
      it "REDIS_LTRIM" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LTRIM,["a","0","-1"])).to be true
      end
      it "REDIS_LINDEX" do
        values = [{"value"=>"a"},{"value"=>"a"},{"value"=>"a"}]
        @tester.setFindValue(values)
        ans = ["a","a","a"]
        expect(@tester.send(:REDIS_LINDEX,["a","0","-1"])).to match_array ans
      end
      it "REDIS_LSET" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LSET,["a","0","-1"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_LREM" do
        values = [{"value"=>"a","index"=>1},{"value"=>"a","index"=>2},{"value"=>"a","index"=>3}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LREM,["a","4","-1"])).to be true
      end
      it "REDIS_LPOP" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LPOP,["2"])).to eq "a"
        values = [{"value"=>"a","index"=>1},{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_LPOP,["2"])).to eq ""
      end
      it "REDIS_RPOP" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        expect(@tester.send(:REDIS_RPOP,["key"])).to eq "a"
      end
      it "REDIS_RPOPLPUSH" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        expect(@tester.send(:REDIS_RPOPLPUSH,["key","key"])).to eq "a"
      end
    end
    context "SET Operation" do
      it "REDIS_SADD" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SADD,["a","b"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_SREM" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SREM,["a","b"])).to be true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_SISMEMBER" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SISMEMBER,["a","b"])).to be true
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_SPOP" do
        @tester.setFindValue([{"value" =>"good"},{"value"=>"good"}])
        expect(@tester.send(:REDIS_SPOP,["a"])).to eq "good"
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_SMOVE" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SMOVE,["src","dst","member"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_SCARD" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_SCARD,["src","dst"])).to eq 10
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_SINTER" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:REDIS_SINTER,["a","b"])).to eq ["good"]
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_SINTERSTORE" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:REDIS_SINTERSTORE,["a","b"])).to eq ["good"]
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_SDIFF" do
        @tester.setFindValue({"b"=>[{"value"=>"good"}],
            "a"=>[{"value"=>"bad"},{"value"=>"good"}]},true)
        expect(@tester.send(:REDIS_SDIFF,["a","b"])).to match_array ["bad"]
      end
      it "REDIS_SDIFFSTORE" do
        @tester.setFindValue({"b"=>[{"value"=>"good"}],
            "a"=>[{"value"=>"bad"},{"value"=>"good"}]},true)
        expect(@tester.send(:REDIS_SDIFFSTORE,["dst","a","b"])).to be true
      end
      it "REDIS_SRANDMEMBER" do
        @tester.setFindValue([{"value" =>"good"},{"value" =>"good"}])
        hash = {"value" => "good"}
        expect(@tester.send(:REDIS_SRANDMEMBER,["a","b"])).to include hash
      end
      it "REDIS_SMEMBERS" do
        @tester.setFindValue([{"value" =>"good"},{"value" =>"good"}])
        ans = ["good","good"]
        expect(@tester.send(:REDIS_SMEMBERS,["a","b"])).to match_array ans
      end
      it "REDIS_SUNION" do
        @tester.setFindValue({"a"=>[{"value"=>"good"}],"b"=>[{"value"=>"bad"}]},true)
        expect(@tester.send(:REDIS_SUNION,["a","b"])).to match_array ["good","bad"]
      end
      it "REDIS_SUNIONSTORE" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:REDIS_SUNIONSTORE,["dst","a","b"])).to be true
      end
    end
    context "Sorted SET Operation" do
      it "REDIS_ZADD" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_ZADD,["k",100,"v"])).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_ZREM" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_ZREM,["k","v"])).to be true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_ZINCRBY" do
        @tester.setQueryValue(false)
        expect(@tester.send(:REDIS_ZINCRBY,["k",1,"v"])).to be false
        expect(@tester.command).to eq "INSERT"
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_ZINCRBY,["k",1,"v"])).to be true
        expect(@tester.command).to eq "UPDATE"
      end
      it "REDIS_ZRANK" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:REDIS_ZRANK,["k","v"])).to eq 10
        expect(@tester.command).to eq "COUNT"
        @tester.setFindValue([{"score"=>nil}])
        expect(@tester.send(:REDIS_ZRANK,["k","v"])).to eq "v"
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_ZREVRANK" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:REDIS_ZREVRANK,["k","v"])).to eq 10
        expect(@tester.command).to eq "COUNT"
        @tester.setFindValue([{"score"=>nil}])
        expect(@tester.send(:REDIS_ZREVRANK,["k","v"])).to eq "v"
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_ZRAGNE" do
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZRANGE,["k","v","10"])).to eq [10,20]
        expect(@tester.command).to eq "AGGREGATE"
      end
      it "REDIS_ZREVRANGE" do
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZREVRANGE,["k","v","10"])).to eq [10,20]
        expect(@tester.command).to eq "AGGREGATE"
      end
      it "REDIS_ZRAGNEBYSCORE" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZRANGEBYSCORE,["k","v","10"])).to eq "[{\"value\":10},{\"value\":20}]"
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_ZCOUNT" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZCOUNT,["k","v","10"])).to eq 10
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_ZCARD" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZCARD,["k","v","10"])).to eq 10
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_ZSCORE" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZSCORE,["k","v","10"])).to eq "[{\"value\":10},{\"value\":20}]"
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_ZREMRANGEBYRANK" do
        @tester.setQueryValue(true)
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:REDIS_ZREMRANGEBYRANK,["k","0","100"])).to eq true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_ZREMRANGEBYSCORE" do
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_ZREMRANGEBYSCORE,["k","10","200"])).to eq true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_UNIONSTORE" do
        @tester.setFindValue([{"value"=>"a"},{"value"=>"b"}])
        @tester.setQueryValue(true)
        args = {"args"=>["a","b"],"options"=>{:weights=>[1.2,1],:aggregate=>"sum"}}
        expect(@tester.send(:REDIS_ZUNIONSTORE,args)).to eq true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_ZINTERSTORE" do
        @tester.setFindValue([{"value"=>"a"},{"value"=>"b"}])
        @tester.setQueryValue(true)
        args = {"args"=>["a","b"],"options"=>{:weights=>[1.2,1],:aggregate=>"sum"}}
        expect(@tester.send(:REDIS_ZINTERSTORE,args)).to eq true
        expect(@tester.command).to eq "INSERT"
      end
    end
    context "HASH Operation" do
      it "REDIS_HSET" do
        args = ["k","f","v"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_HSET,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_HGET" do
        args = ["k","f"]
        @tester.setQueryValue(true)
        ans = {"field"=>"f","value"=>"v"}
        @tester.setFindValue(ans)
        expect(@tester.send(:REDIS_HGET,args)).to eq ans.to_json
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_HMGET" do
        args = {"key"=>"k","args"=>["f1","v1"]}
        @tester.setQueryValue(true)
        ans = {"field"=>"f1","value"=>"v1"}
        @tester.setFindValue(ans)
        expect(@tester.send(:REDIS_HMGET,args)).to eq ans.to_json
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_HMSET" do
        args = {"key"=>"k","args"=>{"f1"=>"2","f2"=>"v"}}
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_HMSET,args)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_HINCRBY" do
        args = ["k","f",20]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_HINCRBY,args)).to be true
        expect(@tester.command).to eq "UPDATE"
        @tester.setQueryValue(false)
        expect(@tester.send(:REDIS_HINCRBY,args)).to be false
        expect(@tester.command).to eq "INSERT"
      end
      it "REDIS_HEXISTS" do
        args = ["k","f"]
        @tester.setFindValue(["a","b"])
        expect(@tester.send(:REDIS_HEXISTS,args)).to be true
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_HDEL" do
        args = ["k","f"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_HDEL,args)).to be true
        expect(@tester.command).to eq "DELETE"
      end
      it "REDIS_HLEN" do
        args = ["k"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_HLEN,args)).to eq 10
        expect(@tester.command).to eq "COUNT"
      end
      it "REDIS_HKEYS" do
        args = ["k"]
        ans = [{"field"=>"a"},{"field"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:REDIS_HKEYS,args)).to match_array ["a","b"]
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_HVALS" do
        args = ["k"]
        ans = [{"value"=>"a"},{"value"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:REDIS_HVALS,args)).to match_array ["a","b"]
        expect(@tester.command).to eq "FIND"
      end
      it "REDIS_HGETALL" do
        args = ["k"]
        ans = [{"value"=>"a"},{"value"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:REDIS_HGETALL,args)).to include ans.to_json
        expect(@tester.command).to eq "FIND"
      end
    end
    context "Private Method (String)" do
      it "getString" do
        @tester.setFindValue([])
        expect(@tester.send(:getString,["a"])).to eq ""
      end
      it "getStrings" do
        @tester.setFindValue([])
        expect(@tester.send(:getStrings,["a","c"])).to eq []
      end
    end
    context "Others Operation & Private Method (Others)" do
      it "REDIS_FLUSHALL" do
        args = ["a"]
        @tester.setQueryValue(true)
        expect(@tester.send(:REDIS_FLUSHALL,args)).to eq true
        expect(@tester.command).to eq "DROP"
      end

      it "prepare_REDIS" do
        ans = {"operand"=>"REDIS_test","args"=>"test"}
        expect(@tester.send(:prepare_REDIS,"test","test")).to include ans
        ans = {"operand"=>"REDIS_ZUNIONSTORE","args"=>"extractZ_X_STORE_ARGS"}
        expect(@tester.send(:prepare_REDIS,"ZUNIONSTORE","test")).to include ans
        ans = {"operand"=>"REDIS_MSET","args"=>"args2hash"}
        expect(@tester.send(:prepare_REDIS,"MSET","test")).to include ans
        ans = {"operand"=>"REDIS_HMGET","args"=>"args2key_args"}
        expect(@tester.send(:prepare_REDIS,"HMGET","test")).to include ans
        ans = {"operand"=>"REDIS_HMSET","args"=>"args2key_hash"}
        expect(@tester.send(:prepare_REDIS,"HMSET","test")).to include ans
      end
    end
    context "Private Method (SET/sortedSET)" do
      it "pushSet" do
        @tester.setQueryValue(true)
        expect(@tester.send(:pushSet,"k","v")).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "getSet" do
        @tester.setFindValue("good")
        expect(@tester.send(:getSet,"k")).to eq "good"
        expect(@tester.command).to eq "FIND"
      end
      it "delSet" do
        @tester.setQueryValue(true)
        expect(@tester.send(:delSet,"k","v")).to be true
      end
      it "pushSortedSet" do
        @tester.setQueryValue(true)
        expect(@tester.send(:pushSortedSet,"k","v",1)).to be true
        expect(@tester.command).to eq "INSERT"
      end
      it "getScoreByVaue" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:getScoreByValue,"k","v")).to eq "100"
        expect(@tester.command).to eq "FIND"
        @tester.setFindValue([])
        expect(@tester.send(:getScoreByValue,"k","v")).to be nil
        expect(@tester.command).to eq "FIND"
      end
      it "createDocsWithAggregate" do
        data = {"a"=>[10,20,30]}
        ans = @tester.send(:createDocsWithAggregate,"dst",data,"sum")
        expect(ans[0][1]["score"]).to eq 60
        ans = @tester.send(:createDocsWithAggregate,"dst",data,"max")
        expect(ans[0][1]["score"]).to eq 30
        ans = @tester.send(:createDocsWithAggregate,"dst",data,"min")
        expect(ans[0][1]["score"]).to eq 10
        ans = @tester.send(:createDocsWithAggregate,"dst",data,"error")
        expect(ans).to eq []
      end
    end
    context "Private Method (List)" do
      it "getNewIndex" do
        @tester.setAggregationValue(nil)
        expect(@tester.send(:getNewIndex,"key","max")).to eq 0
        @tester.setAggregationValue([{"max"=>2,"min"=>1}])
        expect(@tester.send(:getNewIndex,"key","max")).to eq 3
        expect(@tester.send(:getNewIndex,"key","min")).to eq 0
      end
      it "updateIndex" do
        @tester.setQueryValue(true)
        expect(@tester.send(:updateIndex,"key","lpush")).to be true
        expect(@tester.send(:updateIndex,"key","ltrim",{"end"=> 10, "start"=> 1})).to be true
        expect(@tester.send(:updateIndex,"key","lset","1")).to be true
        expect(@tester.send(:updateIndex,"key","lrem","1")).to be true
        expect(@tester.send(:updateIndex,"key","lpop","1")).to be true
        expect(@tester.send(:updateIndex,"key","error")).to be false
      end
      it "sortedArrayGetRange" do
        args = ["a","b","c"]
        ans = ["a","b","c"]
        expect(@tester.send(:sortedArrayGetRange,0,-1,args)).to match_array ans
        expect(@tester.send(:sortedArrayGetRange,-1,-1,args)).to match_array ans
        ans = ["a","b"]
        expect(@tester.send(:sortedArrayGetRange,0,1,ans)).to match_array ans
      end
      it "aggregateScore(sum)" do
        expect(@tester.send(:aggregateScore,"SUM",1,1,10)).to eq 11
      end
      it "aggregateScore(max)" do
        expect(@tester.send(:aggregateScore,"MAX",1,1,10)).to eq  10
      end
      it "aggregateScore(min)" do
        expect(@tester.send(:aggregateScore,"MIN",1,1,10)).to eq 1
      end
      it "aggregateScore(error)" do
        expect(@tester.send(:aggregateScore,"ERROR",1,1,10)).to eq 0
      end
      it "aggregateScore(nil operation)" do
        expect(@tester.send(:aggregateScore,nil,1,1,10)).to eq 11
      end
    end
  end
end

