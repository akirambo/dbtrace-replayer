

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
    it "structureType" do
      result = @parser.structureType("SETTYPE",[nil,nil,"key","0","10","3"])
      expect(result).to eq "keyValue"
    end
    it "MEMCACHED_SET" do
      result = @parser.exec("SET",[nil,nil,"key","0","10","3"])
      expect(result.size).to eq 3
      expect(result[0]).to eq "key"
      expect(result[1]).to eq "10"
      ## check numeric value (3bytes)
      expect(result[2] < 1000).to eq true
    end

    it "MEMCACHED_ADD" do
      result = @parser.exec("ADD",[nil,nil,"key","0","10","3"])
      expect(result.size).to eq 3
      expect(result[0]).to eq "key"
      expect(result[1]).to eq "10"
      ## check numeric value (3bytes)
      expect(result[2] < 1000).to eq true
    end
    it "MEMCACHED_REPLACE" do
      result = @parser.exec("REPLACE",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "MEMCACHED_APPEND" do
      result = @parser.exec("APPEND",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "MEMCACHED_PREPEND" do
      result = @parser.exec("PREPEND",[nil,nil,"key","0","0","3"])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      ## check numeric value (3bytes)
      expect(result[1] < 1000).to eq true
    end
    it "MEMCACHED_GET" do
      result = @parser.exec("GET",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end

    it "MEMCACHED_GETS" do
      result = @parser.exec("GETS",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end
    
    it "MEMCACHED_DELETE" do
      result = @parser.exec("DELETE",[nil,nil,"key"])
      expect(result.size).to eq 1
      expect(result[0]).to eq "key"
    end
    
    it "MEMCACHED_INCR" do
      result = @parser.exec("INCR",[nil,nil,"key",100])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      expect(result[1]).to eq 100
    end
    it "MEMCACHED_DECR" do
      result = @parser.exec("DECR",[nil,nil,"key",100])
      expect(result.size).to eq 2
      expect(result[0]).to eq "key"
      expect(result[1]).to eq 100
    end
    it "MEMCACHED_CAS" do
      result = @parser.exec("CAS",[nil,nil,"key","0","0","3"])
      expect(result ).to eq nil
    end
    it "MEMCACHED_FLUSH" do
      result = @parser.exec("FLUSH",[])
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
    it "MEMCACHED_SET" do
      args = "dummy"
      expect(@parser.exec("SET", args)).to eq args
    end
    it "MEMCACHED_GET" do
      args = "dummy"
      expect(@parser.exec("GET", args)).to eq ["d"]
    end
    it "MEMCACHED_INCR" do
      args = "dummy"
      expect(@parser.exec("INCR", args)).to eq args
    end
  end
end
