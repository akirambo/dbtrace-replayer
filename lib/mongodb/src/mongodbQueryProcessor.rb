#
# Copyright (c) 2017, Carnegie Mellon University.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the University nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
# HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

class MongodbQueryProcessor
  MONGODB_NUMERIC_QUERY = ["$gt","$gte","$lt","$lte"]
  MONGODB_STRING_QUERY  = ["$eq","$ne","$in","$nin"]
  def initialize(logger)
    @logger = logger
  end
  ## symbol [Variable] means that document is symbol type or not 
  def aggregation(result,doc,conds,key2realkey=nil)
    if(key2realkey == nil)then
      key2realkey = {}
      doc.each_key{|key|
        key2realkey["$"+key] = key
      }
    end
    if(conds.class == Hash)then
      conds.each{|k,c|
        case k.to_s
        when "$sum" then
          result = sum(result,doc,c,key2realkey)
        when "$max" then
          result = max(result,doc,c,key2realkey)
        when "$min" then
          result = min(result,doc,c,key2realkey)
        else
          @logger.warn("Unsupported Aggregation !!")
        end
      }
      return result
    else
      return realValue(doc,key2realkey[conds])
    end
  end
  def query(conds,value)
    conds.each_key{|cond_key|
      if(MONGODB_NUMERIC_QUERY.include?(cond_key))then
        num = 0
        cond_num = 0
        if(value.include?("."))then
          num = value.to_f
          cond_num = cond[cond_key].to_f
        else
          num = value.to_i
          cond_num = cond[cond_key].to_i
        end
        if(!numericQuery(cond_key,num,cond_num))then
          return false
        end
      elsif(MONGODB_STRING_QUERY.include?(cond_key))then
        if(!stringQuery(cond_key,value,cond[cond_key]))then
          return false
        end
      else
        @logger.warn("Unsupported operation '#{cond_key}' !!")
      end
    }
    return true
  end
private
  def realValue(doc,conds__)
    conds = conds__.split("..")
    case conds.size
    when 1 then
      if(doc[conds[0]])then
        return doc[conds[0]]
      else
        return doc[conds[0].to_sym]
      end
    when 2 then
      if(doc[conds[0]])then
        return doc[conds[0]][conds[1]]
      else
        return doc[conds[0].to_sym][conds[1].to_sym]
      end
    when 3 then
      if(doc[conds[0]])then
        return doc[conds[0]][conds[1]][conds[2]]
      else
        return doc[conds[0].to_sym][conds[1].to_sym][conds[2].to_sym]
      end
    else
      @logger.error("[ERROR] Unsupported Deep layer(it means bigger than 3) Document.")
    end
  end

  ### Operater
  def sum(result,doc,c,key2realkey)
    if(c.to_i == 1)then
      #  count the number of document 
      if(result == nil)then
        result = 0
      end
      result += 1
    else
      if(result == nil)then
        result = 0
      end
      value = realValue(doc,key2realkey[c])
      result += value.to_i
    end
    return result
  end
  def max(result,doc,c,key2realkey)
    if(result == nil)then
      result = realValue(doc,key2realkey[c]).to_i
    else
      _result_ = realValue(doc,key2realkey[c]).to_i
      if(_result_ > result)then
        result = _result_
      end
    end
    return result
  end
  def min(result,doc,c,key2realkey)
    if(result == nil)then
      result = realValue(doc,key2realkey[c]).to_i
    else
      _result_ = realValue(doc,key2realkey[c]).to_i
      if(_result_ < result)then
        result = _result_
      end
    end
    return result
  end
  def numericQuery(operation,value,cond_value)
    case operation
    when "$gt" then
      ## Return false
      if(value <= cond_value)then
        return false
      end
    when "$gte" then
      ## Return false
      if(value < cond_value)then
        return false
      end
    when "$lt" then
      ## Return false
      if(value >= cond_value)then
        return false
      end
    when "$lte" then
      ## Return false
      if(value < cond_value)then
        return false
      end
    else
      @logger.warn("Unsupported NUMERIC operation '#{operation}' !!")
    end
    return true
  end
  def stringQuery(operation,value,cond_value)
    case operation
    when "$eq" then
      ## Return false
      if(value != cond_value)then
        return false
      end
    when "$ne" then
      ## Return false
      if(value == cond_value)then
        return false
      end
    when "$in" then
      @logger.warn("Unsupported STRING operation '#{operation}' !!")
    when "$nin" then
      @logger.warn("Unsupported STRING operation '#{operation}' !!")
    else
      @logger.warn("Unsupported STRING operation '#{operation}' !!")
    end
    return true
  end
end
