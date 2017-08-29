
require_relative "../../../../spec/spec_helper"
require_relative "../../src/redisOperation"
require_relative "redisCxxClientMock"

class ParserMock
  def initialize()
  end
  def extractZ_X_STORE_ARGS(args)
    return "extractZ_X_STORE_ARGS"
  end
  def args2hash(args)
    return "args2hash"
  end
  def args2key_args(args)
    return "args2key_args"
  end
  def args2key_hash(args)
    return "args2key_hash"
  end
end

class MetricsMock
  def initialize()
  end
  def start_monitor(a,b)
  end
  def end_monitor(a,b)
  end
end

class RedisOperationTester
  include RedisOperation
  def initialize()
    @logger = DummyLogger.new
    @poolRequestSize = 0
    @metrics = MetricsMock.new
    @option = {
      :async => false,
      :poolRequestMaxSize => 128
    }
    @client = RedisCxxClientMock.new
    @args = []
    @parser = ParserMock.new
  end
  def sync
    @option[:async] = false
    @client.init
  end
  def async
    @option[:async] = true
    @client.init
  end
  def getCommand
    return @client.queries
  end
  def setPoolRequestSize(size)
    @poolRequestSize = size
  end
  def setPooledQuerySize(size)
    @client.setPooledQuerySize(size)
  end
  private
  ## Dummry Method
  def connect 
    # do nothing (dummy)
  end
  def close
    # do nothing (dummy)
  end
  def monitor(a,b)
    # do nothing (dummy)
  end
  def addCount(m)
    # do nothing (dummy)
  end
  def addDuration(a,b,c)
    # do nothing (dummy)
  end
  def addTotalDuration(a,b)
    # do nothing (dummy)
  end
end
