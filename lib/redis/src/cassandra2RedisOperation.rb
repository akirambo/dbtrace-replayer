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

module Cassandra2RedisOperation
  private
  ###############
  ## OPERATION ##
  ###############
  # @conv {"INSERT" => ["SET","HMSET"]}
  def CASSANDRA_INSERT(args)
    primaryKey = args["primaryKey"]
    if(args["schema_fields"] == 2)then
      ### String (Key-Value)
      key = "#{args["table"]}--#{args["args"][primaryKey]}"
      fieldnames = args["args"].keys
      fieldnames.delete(args["primaryKey"])
      value = args["args"][fieldnames[0]]
      return SET([key, value])
    else
      ### Hash
      args["key"] = "#{args["table"]}--#{args["args"][primaryKey]}"
      args["args"].delete(primaryKey)
      return HMSET(args)
    end
  end
  # @conv {"SELECT" => ["GET","HMGET"]}
  def CASSANDRA_SELECT(args)
    data = []
    primaryKey = args["primaryKey"]
    idx = args["cond_keys"].index(primaryKey)
    key = "#{args["table"]}--#{args["cond_values"][0].gsub(/\"/,"")}"
    if(idx and args["cond_values"][idx])then
      key = "#{args["table"]}--#{args["cond_values"][idx].gsub(/\"/,"")}"
    end
    if(args["schema_fields"] == 2)then
      ### String (Key-Value)
      data = [GET([key])]
    else
      ### Hash
      ## create Key
      _args_ = {
        "key"  => key,
        "args" => args["fields"]
      }
      data = HMGET(_args_,false)
    end
    return data
  end
  # @conv {"UPDATE" => ["INSERT","HMSET"]}
  def CASSANDRA_UPDATE(args)
    ### convert arguments
    primaryKey = args["primaryKey"]
    idx = args["cond_keys"].index(primaryKey)
    args["args"] = args["set"]
    args["args"][primaryKey] = args["cond_values"][idx]
    return CASSANDRA_INSERT(args)
  end
  # @conv {"DELETE" => ["DEL","HDEL"]}
  def CASSANDRA_DELETE(args)
    if(args["schema_fields"] == 2 or args["fields"] != "*")then
      ## Strings(Key-Value )
      return DEL([args["table"]])
    else
      ## Hash
      return HDEL([args["table"],args["fields"]])
    end
  end
  # @conv {"DROP" => ["SMEMBER","SREM","SADD"]}
  def CASSANDRA_DROP(args)
    targetKeys = KEYS(args["key"],args["type"])
    ## drop Table
    return DEL(targetKeys)
  end

  ##############
  ## JAVA API ##
  ##############
=begin
  # @conv {"BATCH_MUTATE" => ["SADD"]}
  def CASSANDRA_BATCH_MUTATE(args)
    if(args["counterColumn"])then
      args["args"].each{|arg|
        val = cassandraSerialize(arg)
        SADD(args["key"], val)
        ## set key list
        SADD("__keylist__", args["key"])    
      }
    else
      val = cassandraSerialize(args["args"])
      SADD(args["key"], val)
      ## set key list
      SADD("__keylist__", args["key"])    
    end
    return
  end
  # @conv {"GET_SLICE" => ["SMEMBERS"]}
  def CASSANDRA_GET_SLICE(args)
    val = SMEMBERS([args["key"]])
    results = cassandraDeserialize(val)
    if(args["limit"])then
      results = results.take(args["limit"].to_i)
    end
    data = []
    results.each{|row|
      if(cassandraQuery(row, args))then
        data.push(selectField(row, args))
      end
    }
    #data.each{|r| puts r}
  end
  # @conv {"GET_RANGE_SLICES" => ["SMEMBERS"]}
  def CASSANDRA_GET_RANGE_SLICES(args)
    val = SMEMBERS([args["key"]])
    results = cassandraDeserialize(val)
    if(args["limit"])then
      results = results.take(args["limit"].to_i)
    end
    data = []
    results.each{|row|
      if(cassandraQuery(row, args))then
        data.push(selectField(row, args))
      end
    }
    #data.each{|r| puts r}
  end
 # @conv {"MULTIGET_SLICE" => ["SMEMBERS"]}
  def CASSANDRA_MULTIGET_SLICE(args)
    val = SMEMBERS([args["key"]])
    results = cassandraDeserialize(val)
    if(args["limit"])then
      results = results.take(args["limit"].to_i)
    end
    data = []
    results.each{|row|
      if(cassandraQuery(row, args))then
        data.push(selectField(row, args))
      end
    }
    #data.each{|r| puts r}
  end
=end 
  #############
  ## PREPARE ##
  #############
  def prepare_CASSANDRA(operand,args)
    ## PREPARE OPERATION & ARGS
    result = {}
    result["operand"] = "CASSANDRA_#{operand.upcase}"
    result["args"]    = @parser.exec(operand.upcase,args)
    return result
  end
  
  def cassandraQuery(result, args)
    ## where
    if(args["where"])then
      args["where"].each{|__cond__|
        __cond__ = __cond__.split("=")
        fieldname = __cond__[0]
        value = __cond__[1]
        if(result[fieldname] != value)then
          return false
        end
      }
    end
    return true
  end
  def selectField(hash, args)
    row = {}
    if(args["fields"][0] == "*")then
      return hash
    else
      args["fields"][0].split(",").each{|field|
        row[field] = hash[field]
      }
    end
    return row
  end
=begin
  def cassandraSerialize(hash)
    return convJSON(hash)
  end
  def cassandraDeserialize(array)
    result = []
    array.each{|row|
      result.push(parseJSON(row))
    }
    return result
  end
=end
end
