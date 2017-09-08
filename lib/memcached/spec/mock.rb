
module MemcachedUnitTest
  class ParserMock
    def exec(a,b)
      return "OK"
    end
    def extractZ_X_STORE_ARGS(a)
      return "OK"
    end
    def args2hash(a)
      return "OK"
    end
    def args2key_args(a)
      return "OK"
    end
    def args2key_hash(a)
      return "OK"
    end
  end
  class ClientMock
    attr_accessor :queryReturn, :replyValue
    def initialize
      @queryReturn = false
      @replyValue    = ""
    end
    def getDuration
      return 0.1
    end
    def syncExecuter(operand,key,value,expiretime)
      return @queryReturn
    end
    def commitGetKey(a)
    end
    def getReply()
      return @replyValue
    end
    def keys()
      return "test00,test01,test02,test03"
    end
  end
  class QueryParserMock
    attr_accessor :cond
    def getParameter(a)
      return @cond
    end
    def createKey2RealKey(a,b)
    end
    def createGroupKey(a,b)
      return "groupKey"
    end
  end
  class QueryProcessorMock
    attr_accessor :returnValue
    def aggregation(a,b,c,d)
      return 10
    end
    def query(a,b)
      return @returnValue
    end
  end
  class UtilsMock
    attr_accessor :docs
    def initialize
      @docs = []
    end
    def symbolhash2stringhash(a)
      return @docs
    end
    def stringhash2symbolhash(a)
      return @docs
    end
  end

end
