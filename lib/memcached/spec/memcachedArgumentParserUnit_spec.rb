

require_relative "../../../spec/spec_helper"
require_relative "../src/memcachedArgumentParser"

RSpec.describe 'Memcached Argument Parser Unit Test' do
  context 'Prepare Arguments' do
    before do
      @logger = DummyLogger.new
      @option = {
        :inputFormat => "basic"
      }
      @parser = MemcachedArgumentParser.new(@logger,@option)
    end
    it "structure_type" do
      result = @parser.structure_type("SETTYPE",[nil,nil,"key","0","10","3"])
      expect(result).to eq "keyValue"
    end
    it "memcached_set" do
      result = @parser.exec("set",[nil,nil,"key","0","10","3"])
      expect(result.size).to eq 3
      expect(result[0]).to eq "key"
      expect(result[1]).to eq "10"
      ## check numeric value (3bytes)
      expect(result[2] < 1000).to eq true
    end

    it "memcached_add" do
      result = @parser.exec("add",[nil,nil,"key","0","10","3"])
      expect(result.size).to eq 3
      expect(result[0]).to eq "key"
      expect(result[1]).to eq "10"
      ## check numeric value (3bytes)
      expect(result[2] < 1000).to eq true
    end
    it "memcached_replace" do
      result = @parser.exec("replace",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "memcached_append" do
      result = @parser.exec("append",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "memcached_prepend" do
      result = @parser.exec("prepend",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "memcached_get" do
      result = @parser.exec("get",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end
    it "memcached_gets" do
      result = @parser.exec("gets",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end
    it "memcached_delete" do
      result = @parser.exec("delete",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end
    
    it "memcached_incr" do
      result = @parser.exec("incr",[nil,nil,"key",100])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      expect(result[1]).to eq 100
    end
    it "memcached_decr" do
      result = @parser.exec("decr",[nil,nil,"key",100])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      expect(result[1]).to eq 100
    end
    it "memcached_cas" do
      result = @parser.exec("cas",[nil,nil,"key","0","0","3"])
      expect(result ).to eq nil
    end
    it "memcached_flush" do
      result = @parser.exec("flush",[])
      expect(result ).to eq []
    end
  end


  context 'Prepare Arguments' do
    before(:all) do
      @logger = DummyLogger.new
      @option = {
        :inputFormat => "binary"
      }
      @parser = MemcachedArgumentParser.new(@logger,@option)
    end
    it "memcached_set" do
      args = "dummy"
      expect(@parser.exec("set", args)).to eq args
    end
    it "memcached_get" do
      args = "dummy"
      expect(@parser.exec("get", args)).to eq ["d"]
    end
    it "memcached_incr" do
      args = "dummy"
      expect(@parser.exec("incr", args)).to eq args
    end
  end
end
