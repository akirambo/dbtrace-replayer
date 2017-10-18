# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redis2MongodbOperation"

module Redis2MongodbTester
  class ParserMock
    def extract_z_x_store_args(a)
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
    def insert(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def find(a)
      @value = a
      @command = "#{__method__}"
      if(@findReturnHashFlag)then
        return @findReturn[a["key"]]
      end
      return @findReturn
    end
    def update(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def delete(a)
      @value = a
      @command = "#{__method__}"
      return @queryReturn
    end
    def aggregate(a)
      @value = a
      @command = "#{__method__}"
      return @aggregationReturn
    end
    def count(a)
      @value = a
      @command = "#{__method__}"
      return 10
    end
    def drop(a)
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
      it "redis_set" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_set,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_setex" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_setex,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_setnx" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_setnx,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_psetex" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_psetex,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_get" do
        args = ["id00","value00"]
        @tester.setFindValue([{"value"=>"results"}])
        expect(@tester.send(:redis_get,args)).to eq "results"
        expect(@tester.command).to eq "find"
      end
      it "redis_mset" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_mset,args)).to be true
        expect(@tester.command).to eq "insert"
        @tester.setQueryValue(false)
        expect(@tester.send(:redis_mset,args)).to be false
        expect(@tester.command).to eq "insert"
      end
      it "redis_msetnx" do
        args = ["id00","value00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_msetnx,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_mget" do
        args = ["id00","id01"]
        @tester.setFindValue([{"value"=>"results"},{"value"=>"results"}])
        expect(@tester.send(:redis_mget,args)).to match_array ["results","results"]
        expect(@tester.command).to eq "find"
      end
      it "redis_incr" do
        args = ["id00"]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_incr,args)).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_incrby" do
        args = ["id00",1]
      @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_incrby,args)).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_decr" do
        args = ["id00"]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_decr,args)).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_decrby" do
        args = ["id00",1]
        @tester.setFindValue([{"value"=>"1"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_decrby,args)).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_append" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_append,args)).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_getset" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_getset,args)).to eq "t"
        expect(@tester.command).to eq "update"
      end
      it "redis_strlen" do
        args = ["id00","m"]
        @tester.setFindValue([{"value"=>"t"}])
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_strlen,args)).to eq 1
        expect(@tester.command).to eq "find"
      end
      it "redis_del" do
        args = ["id00"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_del,args)).to be true
        expect(@tester.command).to eq "delete"
      end
    end
    context "list" do
      it "redis_lpush" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_lpush,["a","b"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_rpush" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_rpush,["a","b"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_llen" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_llen,["a"])).to eq 10
        expect(@tester.command).to eq "count"
      end
      it "redis_lrange" do
        values = [{"value"=>"a"},{"value"=>"a"},{"value"=>"a"}]
        @tester.setFindValue(values)
        expect(@tester.send(:redis_lrange,["a","0","-1"])).to eq ["a","a","a"]
      end
      it "redis_ltrim" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_ltrim,["a","0","-1"])).to be true
      end
      it "redis_lindex" do
        values = [{"value"=>"a"},{"value"=>"a"},{"value"=>"a"}]
        @tester.setFindValue(values)
        ans = ["a","a","a"]
        expect(@tester.send(:redis_lindex,["a","0","-1"])).to match_array ans
      end
      it "redis_lset" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_lset,["a","0","-1"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_lrem" do
        values = [{"value"=>"a","index"=>1},{"value"=>"a","index"=>2},{"value"=>"a","index"=>3}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_lrem,["a","4","-1"])).to be true
      end
      it "redis_lpop" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_lpop,["2"])).to eq "a"
        values = [{"value"=>"a","index"=>1},{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_lpop,["2"])).to eq ""
      end
      it "redis_rpop" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        expect(@tester.send(:redis_rpop,["key"])).to eq "a"
      end
      it "redis_rpoplpush" do
        values = [{"value"=>"a","index"=>0}]
        @tester.setFindValue(values)
        expect(@tester.send(:redis_rpoplpush,["key","key"])).to eq "a"
      end
    end
    context "SET Operation" do
      it "redis_sadd" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_sadd,["a","b"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_srem" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_srem,["a","b"])).to be true
        expect(@tester.command).to eq "delete"
      end
      it "redis_sismember" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_sismember,["a","b"])).to be true
        expect(@tester.command).to eq "count"
      end
      it "redis_spop" do
        @tester.setFindValue([{"value" =>"good"},{"value"=>"good"}])
        expect(@tester.send(:redis_spop,["a"])).to eq "good"
        expect(@tester.command).to eq "delete"
      end
      it "redis_smove" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_smove,["src","dst","member"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_scard" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_scard,["src","dst"])).to eq 10
        expect(@tester.command).to eq "count"
      end
      it "redis_sinter" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:redis_sinter,["a","b"])).to eq ["good"]
        expect(@tester.command).to eq "find"
      end
      it "redis_sinterstore" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:redis_sinterstore,["a","b"])).to eq ["good"]
        expect(@tester.command).to eq "insert"
      end
      it "redis_sdiff" do
        @tester.setFindValue({"b"=>[{"value"=>"good"}],
            "a"=>[{"value"=>"bad"},{"value"=>"good"}]},true)
        expect(@tester.send(:redis_sdiff,["a","b"])).to match_array ["bad"]
      end
      it "redis_sdiffstore" do
        @tester.setFindValue({"b"=>[{"value"=>"good"}],
            "a"=>[{"value"=>"bad"},{"value"=>"good"}]},true)
        expect(@tester.send(:redis_sdiffstore,["dst","a","b"])).to be true
      end
      it "redis_srandmember" do
        @tester.setFindValue([{"value" =>"good"},{"value" =>"good"}])
        hash = {"value" => "good"}
        expect(@tester.send(:redis_srandmember,["a","b"])).to include hash
      end
      it "redis_smembers" do
        @tester.setFindValue([{"value" =>"good"},{"value" =>"good"}])
        ans = ["good","good"]
        expect(@tester.send(:redis_smembers,["a","b"])).to match_array ans
      end
      it "redis_sunion" do
        @tester.setFindValue({"a"=>[{"value"=>"good"}],"b"=>[{"value"=>"bad"}]},true)
        expect(@tester.send(:redis_sunion,["a","b"])).to match_array ["good","bad"]
      end
      it "redis_sunionstore" do
        @tester.setFindValue([{"value" =>"good"}])
        expect(@tester.send(:redis_sunionstore,["dst","a","b"])).to be true
      end
    end
    context "Sorted SET Operation" do
      it "redis_zadd" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_zadd,["k",100,"v"])).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_zrem" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_zrem,["k","v"])).to be true
        expect(@tester.command).to eq "delete"
      end
      it "redis_zincrby" do
        @tester.setQueryValue(false)
        expect(@tester.send(:redis_zincrby,["k",1,"v"])).to be false
        expect(@tester.command).to eq "insert"
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_zincrby,["k",1,"v"])).to be true
        expect(@tester.command).to eq "update"
      end
      it "redis_zrank" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:redis_zrank,["k","v"])).to eq 10
        expect(@tester.command).to eq "count"
        @tester.setFindValue([{"score"=>nil}])
        expect(@tester.send(:redis_zrank,["k","v"])).to eq "v"
        expect(@tester.command).to eq "find"
      end
      it "redis_zrevrank" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:redis_zrevrank,["k","v"])).to eq 10
        expect(@tester.command).to eq "count"
        @tester.setFindValue([{"score"=>nil}])
        expect(@tester.send(:redis_zrevrank,["k","v"])).to eq "v"
        expect(@tester.command).to eq "find"
      end
      it "redis_zragne" do
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zrange,["k","v","10"])).to eq [10,20]
        expect(@tester.command).to eq "aggregate"
      end
      it "redis_zrevrange" do
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zrevrange,["k","v","10"])).to eq [10,20]
        expect(@tester.command).to eq "aggregate"
      end
      it "redis_zragnebyscore" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zrangebyscore,["k","v","10"])).to eq "[{\"value\":10},{\"value\":20}]"
        expect(@tester.command).to eq "find"
      end
      it "redis_zcount" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zcount,["k","v","10"])).to eq 10
        expect(@tester.command).to eq "count"
      end
      it "redis_zcard" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zcard,["k","v","10"])).to eq 10
        expect(@tester.command).to eq "count"
      end
      it "redis_zscore" do
        @tester.setFindValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zscore,["k","v","10"])).to eq "[{\"value\":10},{\"value\":20}]"
        expect(@tester.command).to eq "find"
      end
      it "redis_zremrangebyrank" do
        @tester.setQueryValue(true)
        @tester.setAggregationValue([{"value"=>10},{"value"=>20}])
        expect(@tester.send(:redis_zremrangebyrank,["k","0","100"])).to eq true
        expect(@tester.command).to eq "delete"
      end
      it "redis_zremrangebyscore" do
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_zremrangebyscore,["k","10","200"])).to eq true
        expect(@tester.command).to eq "delete"
      end
      it "redis_unionstore" do
        @tester.setFindValue([{"value"=>"a"},{"value"=>"b"}])
        @tester.setQueryValue(true)
        args = {"args"=>["a","b"],"option"=>{:weights=>[1.2,1],:aggregate=>"sum"}}
        expect(@tester.send(:redis_zunionstore,args)).to eq true
        expect(@tester.command).to eq "insert"
      end
      it "redis_zinterstore" do
        @tester.setFindValue([{"value"=>"a"},{"value"=>"b"}])
        @tester.setQueryValue(true)
        args = {"args"=>["a","b"],"option"=>{:weights=>[1.2,1],:aggregate=>"sum"}}
        expect(@tester.send(:redis_zinterstore,args)).to eq true
        expect(@tester.command).to eq "insert"
      end
    end
    context "HASH Operation" do
      it "redis_hset" do
        args = ["k","f","v"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_hset,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_hget" do
        args = ["k","f"]
        @tester.setQueryValue(true)
        ans = {"field"=>"f","value"=>"v"}
        @tester.setFindValue(ans)
        expect(@tester.send(:redis_hget,args)).to eq ans.to_json
        expect(@tester.command).to eq "find"
      end
      it "redis_hmget" do
        args = {"key"=>"k","args"=>["f1","v1"]}
        @tester.setQueryValue(true)
        ans = {"field"=>"f1","value"=>"v1"}
        @tester.setFindValue(ans)
        expect(@tester.send(:redis_hmget,args)).to eq ans.to_json
        expect(@tester.command).to eq "find"
      end
      it "redis_hmset" do
        args = {"key"=>"k","args"=>{"f1"=>"2","f2"=>"v"}}
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_hmset,args)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "redis_hincrby" do
        args = ["k","f",20]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_hincrby,args)).to be true
        expect(@tester.command).to eq "update"
        @tester.setQueryValue(false)
        expect(@tester.send(:redis_hincrby,args)).to be false
        expect(@tester.command).to eq "insert"
      end
      it "redis_hexists" do
        args = ["k","f"]
        @tester.setFindValue(["a","b"])
        expect(@tester.send(:redis_hexists,args)).to be true
        expect(@tester.command).to eq "find"
      end
      it "redis_hdel" do
        args = ["k","f"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_hdel,args)).to be true
        expect(@tester.command).to eq "delete"
      end
      it "redis_hlen" do
        args = ["k"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_hlen,args)).to eq 10
        expect(@tester.command).to eq "count"
      end
      it "redis_hkeys" do
        args = ["k"]
        ans = [{"field"=>"a"},{"field"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:redis_hkeys,args)).to match_array ["a","b"]
        expect(@tester.command).to eq "find"
      end
      it "redis_hvals" do
        args = ["k"]
        ans = [{"value"=>"a"},{"value"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:redis_hvals,args)).to match_array ["a","b"]
        expect(@tester.command).to eq "find"
      end
      it "redis_hgetall" do
        args = ["k"]
        ans = [{"value"=>"a"},{"value"=>"b"}]
        @tester.setFindValue(ans)
        expect(@tester.send(:redis_hgetall,args)).to include ans.to_json
        expect(@tester.command).to eq "find"
      end
    end
    context "Private Method (String)" do
      it "get_string" do
        @tester.setFindValue([])
        expect(@tester.send(:get_string,["a"])).to eq ""
      end
      it "get_strings" do
        @tester.setFindValue([])
        expect(@tester.send(:get_strings,["a","c"])).to eq []
      end
    end
    context "Others Operation & Private Method (Others)" do
      it "redis_flushall" do
        args = ["a"]
        @tester.setQueryValue(true)
        expect(@tester.send(:redis_flushall,args)).to eq true
        expect(@tester.command).to eq "drop"
      end

      it "prepare_redis" do
        ans = {"operand"=>"redis_test","args"=>"test"}
        expect(@tester.send(:prepare_redis,"test","test")).to include ans
        ans = {"operand"=>"redis_zunionstore","args"=>"extract_z_x_store_args"}
        expect(@tester.send(:prepare_redis,"zunionstore","test")).to include ans
        ans = {"operand"=>"redis_mset","args"=>"args2hash"}
        expect(@tester.send(:prepare_redis,"mset","test")).to include ans
        ans = {"operand"=>"redis_hmget","args"=>"args2key_args"}
        expect(@tester.send(:prepare_redis,"hmget","test")).to include ans
        ans = {"operand"=>"redis_hmset","args"=>"args2key_hash"}
        expect(@tester.send(:prepare_redis,"hmset","test")).to include ans
      end
    end
    context "Private Method (SET/sortedSET)" do
      it "push_set" do
        @tester.setQueryValue(true)
        expect(@tester.send(:push_set,"k","v")).to be true
        expect(@tester.command).to eq "insert"
      end
      it "get_set" do
        @tester.setFindValue("good")
        expect(@tester.send(:get_set,"k")).to eq "good"
        expect(@tester.command).to eq "find"
      end
      it "del_set" do
        @tester.setQueryValue(true)
        expect(@tester.send(:del_set,"k","v")).to be true
      end
      it "push_sorted_set" do
        @tester.setQueryValue(true)
        expect(@tester.send(:push_sorted_set,"k","v",1)).to be true
        expect(@tester.command).to eq "insert"
      end
      it "get_score_by_value" do
        @tester.setFindValue([{"score"=>"100"}])
        expect(@tester.send(:get_score_by_value,"k","v")).to eq "100"
        expect(@tester.command).to eq "find"
        @tester.setFindValue([])
        expect(@tester.send(:get_score_by_value,"k","v")).to be nil
        expect(@tester.command).to eq "find"
      end
      it "createDocsWithaggregate" do
        data = {"a"=>[10,20,30]}
        ans = @tester.send(:create_docs_with_aggregate,"dst",data,"sum")
        expect(ans[0][1]["score"]).to eq 60
        ans = @tester.send(:create_docs_with_aggregate,"dst",data,"max")
        expect(ans[0][1]["score"]).to eq 30
        ans = @tester.send(:create_docs_with_aggregate,"dst",data,"min")
        expect(ans[0][1]["score"]).to eq 10
        ans = @tester.send(:create_docs_with_aggregate,"dst",data,"error")
        expect(ans).to eq []
      end
    end
    context "Private Method (List)" do
      it "getNewIndex" do
        @tester.setAggregationValue(nil)
        expect(@tester.send(:get_new_index,"key","max")).to eq 0
        @tester.setAggregationValue([{"max"=>2,"min"=>1}])
        expect(@tester.send(:get_new_index,"key","max")).to eq 3
        expect(@tester.send(:get_new_index,"key","min")).to eq 0
      end
      it "update_index" do
        @tester.setQueryValue(true)
        expect(@tester.send(:update_index,"key","lpush")).to be true
        expect(@tester.send(:update_index,"key","ltrim",{"end"=> 10, "start"=> 1})).to be true
        expect(@tester.send(:update_index,"key","lset","1")).to be true
        expect(@tester.send(:update_index,"key","lrem","1")).to be true
        expect(@tester.send(:update_index,"key","lpop","1")).to be true
        expect(@tester.send(:update_index,"key","error")).to be false
      end
      it "sorted_array_get_range" do
        args = ["a","b","c"]
        ans = ["a","b","c"]
        expect(@tester.send(:sorted_array_get_range,0,-1,args)).to match_array ans
        expect(@tester.send(:sorted_array_get_range,-1,-1,args)).to match_array ans
        ans = ["a","b"]
        expect(@tester.send(:sorted_array_get_range,0,1,ans)).to match_array ans
      end
      it "aggregate_score(sum)" do
        expect(@tester.send(:aggregate_score,"SUM",1,1,10)).to eq 11
      end
      it "aggregate_score(max)" do
        expect(@tester.send(:aggregate_score,"MAX",1,1,10)).to eq  10
      end
      it "aggregate_score(min)" do
        expect(@tester.send(:aggregate_score,"MIN",1,1,10)).to eq 1
      end
      it "aggregate_score(error)" do
        expect(@tester.send(:aggregate_score,"ERROR",1,1,10)).to eq 0
      end
      it "aggregate_score(nil operation)" do
        expect(@tester.send(:aggregate_score,nil,1,1,10)).to eq 11
      end
      it "get_list" do
        expect(@tester.send("get_list","key")).to eq false
      end
    end
  end
end

