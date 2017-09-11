

require_relative "../../../spec/spec_helper"
require_relative "../src/memcachedParser"

RSpec.describe 'Memcached Parser Unit Test' do
  context 'Parser' do
    before(:each) do
      dir = File.dirname(__FILE__)
      @filename = "#{dir}/input/memcached_all_command_binary_protocol.log"
      logger = DummyLogger.new
      options = {
        :inputFormat => "basic"
      }
      @parser = MemcachedParser.new(@filename,options,logger)
    end
    it "parse (basic)" do
      ans = {"get" => ["a","get","key"]}
      expect(@parser.parse("a get key ")).to eq ans
    end
    it "parse (error)" do
      expect(@parser.parse("a error key ")).to eq nil
    end
    it "parse_multilines" do
      ans = ["flush", "set", "get", "incr", "decr", "add", "replace", "append", "prepend", "delete"]
      expect(@parser.parse_multilines(@filename)).to match_array ans
    end
    it "parse_multilines (ERROR)" do
      dir = File.dirname(__FILE__)
      @filename = "#{dir}/input/memcached_error_command_binary_protocol.log"
      expect(@parser.parse_multilines(@filename)).to match_array []
    end
    it "integer_string?(true)" do
      expect(@parser.integer_string?("123")).to eq true
    end
    it "integer_string?(false)" do
      expect(@parser.integer_string?("aaa")).to eq false
    end
    it "random_string" do
      expect(@parser.send(:random_string,10).size).to eq 10
    end
    it "register_logs(arg size = 3)" do
      logs   = [["com",5,5]]
      @parser.send(:register_logs,logs)
      instance = @parser.instance_variable_get(:@logs)
      command = instance.log[0]
      expect(command["com"][0].size).to eq 5
      expect(command["com"][1].size).to eq 5
    end
    it "register_logs(arg size = 4)" do
      logs   = [["com",5,5,"key"]]
      @parser.send(:register_logs,logs)
      instance = @parser.instance_variable_get(:@logs)
      command = instance.log[0]
      expect(command["com"][0]).to eq "key"
      expect(command["com"][1].size).to eq 5
    end
    it "register_logs(arg size = 5)" do
      logs   = [["com",3,5,"key","value"]]
      @parser.send(:register_logs,logs)
      instance = @parser.instance_variable_get(:@logs)
      command = instance.log[0]
      expect(command["com"][0]).to eq "key"
      expect(command["com"][1]).to eq "value"
    end
    it "register_logs(error)" do
      logs   = [["com",5,5,"key","value","mm"]]
      result = @parser.send(:register_logs,logs)
    end
  end
end
