
class RedisCxxClientMock
  attr_reader :queries, :keys, :pooledQuerySize
  def initialize
    @pooledQuerySize = 0
    @queries = []
    @keys = ["k1.t1","k1.t2","k2.t1"]
  end
  def syncConnect(h,p)
    # dummy (do nothing)
  end
  def syncClose
    # dummy (do nothing)
  end
  def commitQuery(query)
    @queries.push(query)
  end
  def syncExecuter(query)
    @queries.push(query)
    return "OK"
  end
  def asyncExecuter
    @queries = []
    return "OK"
  end
  def getAsyncReply
    return "asyncReply"
  end
  def getReply
    return "syncReply"
  end
  def getDuration
    return 0.1
  end
  
  ## For TEST
  def init
    @queries = []
  end
  def setPooledQuerySize(size)
    @pooledQuerySize = size
  end
end

