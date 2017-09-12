
require_relative "../../../spec/spec_helper"
require_relative "../src/redisArgumentParser"

RSpec.describe 'RedisArgumentParser Unit Test)' do
  before(:all) do
    @tester = RedisArgumentParser.new(DummyLogger.new())
  end
  context 'Convert Method' do
    it 'extractZ_X_STORE_ARGS' do
      args = ["dst", "2", "set", "set2", "WEIGHTS", "2.0", "1.0", "AGGREGATE", "sum"]
      ans = {"key" => "dst", "args" => ["set","set2"],
        "option" => {:weights => ["2.0","1.0"],:aggregate => "sum"}}
      expect(@tester.extractZ_X_STORE_ARGS(args)).to include(ans)
      args = ["dst", "2", "set", "set2", "WEIGHTS", "2.0", "1.0", "HOGE", "sum"]
      ans = {"key" => "dst", "args" => ["set","set2"],
        "option" => {:weights => ["2.0","1.0"]}}
      expect(@tester.extractZ_X_STORE_ARGS(args)).to include(ans)
    end
    it 'args2hash' do
      args = ["key0","v1","key1","v2"]
      ans  = {"key0" => "v1","key1" => "v2"}
      expect(@tester.args2hash(args)).to include(ans)
    end
    it 'args2key_args' do
      args = ["key","field0","field1"]
      ans = {"key" => "key","args" => ["field0","field1"]}
      expect(@tester.args2key_args(args)).to match_array ans
    end
    it 'args2key_hash' do
      args = ["key","f3","v3","f4","v4","f5","10"]
      ans = {"key" => "key", "args" => {"f3"=>"v3","f4"=>"v4","f5"=>"10"}} 
      expect(@tester.args2key_hash(args)).to include(ans)
    end

  end
end
