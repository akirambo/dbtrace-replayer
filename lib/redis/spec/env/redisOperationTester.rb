
require_relative "../../../../spec/spec_helper"
require_relative "../../src/redisOperation"
require_relative "redisCxxClientMock"

class ParserMock
  def initialize()
  end
  def extract_z_x_store_args(args)
    return "extract_z_x_store_args"
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
    @pool_request_size = 0
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
    @pool_request_size = size
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
  def add_count(m)
    # do nothing (dummy)
  end
  def add_duration(a,b,c)
    # do nothing (dummy)
  end
  def add_total_duration(a,b)
    # do nothing (dummy)
  end
end
