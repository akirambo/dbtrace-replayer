
require_relative "../../../spec/spec_helper"
require_relative "../src/redisRunner"

RSpec.describe 'Memcached TO Redis Unit Test' do
  before do
    @logger = Logger.new(STDOUT)
    @logger.level = Logger::FATAL
    @options = {
      :sourceDB => "memcached"
    }
    @runner = RedisRunner.new("memcached", @logger,@options)
    @runner.send(:operation,"FLUSHALL",[])
  end
  context 'Operation' do
    it "MEMCACHED_SET/MEMCACHED_GET WITH EXPIRE_TIME" do
      ## Not Supported [flag, size]
      @runner.send(:MEMCACHED_SET,["key",1,"value"])
      sleep(2)
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq nil
    end
    it "MEMCACHED_SET/MEMCACHED_GET NO EXPIRE_TIME" do
      ## Not Supported [flag, size]
      @runner.send(:MEMCACHED_SET,["key","value"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "value"
    end
    it "MEMCACHED_ADD" do
      ## Not Supported [flag, size]
      @runner.send(:MEMCACHED_ADD,["key","correct"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "correct"
      @runner.send(:MEMCACHED_ADD,["key","incorrect"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "correct"
    end
    it "MEMCACHED_GETS" do
      @runner.send(:MEMCACHED_SET,["key","value"])
      expect(@runner.send(:MEMCACHED_GETS,["key"])).to eq "value"
    end
    it "MEMCACHED_CAS" do
      @runner.send(:MEMCACHED_CAS,["key","value",1])
      expect(@runner.send(:MEMCACHED_GETS,["key"])).to eq "value"
    end
    it "MEMCACHED_REPLACE" do
      ## Not Supported [flag, size]
      @runner.send(:MEMCACHED_SET,["key",10,"incorrect"])
      @runner.send(:MEMCACHED_REPLACE,["key",10,"correct"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "correct"
    end
    it "MEMCACHED_APPEND" do
      @runner.send(:MEMCACHED_SET,["key",10,"corr"])
      @runner.send(:MEMCACHED_APPEND,["key",10,"ect"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "correct"
    end
    it "MEMCACHED_PREPEND" do
      @runner.send(:MEMCACHED_SET,["key",10,"ect"])
      @runner.send(:MEMCACHED_PREPEND,["key",10,"corr"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "correct"
    end
    it "MEMCACHED_INCR" do
      @runner.send(:MEMCACHED_SET,["key",10,100])
      @runner.send(:MEMCACHED_INCR,["key",100])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "200"
    end
    it "MEMCACHED_DECR" do
      @runner.send(:MEMCACHED_SET,["key",10,100])
      @runner.send(:MEMCACHED_DECR,["key",100])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq "0"
    end
    it "MEMCACHED_DELETE" do
      @runner.send(:MEMCACHED_SET,["key",10,"value"])
      @runner.send(:MEMCACHED_DELETE,["key"])
      expect(@runner.send(:MEMCACHED_GET,["key"])).to eq nil
    end
  end
end
