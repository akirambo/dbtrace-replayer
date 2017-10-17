
class RedisCxxClientMock
  attr_reader :queries, :keys, :pooledQuerySize
  def initialize
    @pooledQuerySize = 0
    @queries = []
    @keys = "k1.t1,k1.t2,k2.t1"
    @keys_flag = false
    @empty_flag = false
  end
  def empty_reply
    @empty_flag = true
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
    @keys_flag = if query == "keys *"
                   true
                 else
                   false
                 end
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
    if @keys_flag
      unless @empty_flag
        return @keys
      else
        @empty_flag = false
        return ""
      end
    end
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

