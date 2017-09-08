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

module Cassandra2MemcachedOperation
  private
  ###############
  ## OPERATION ##
  ###############
  # @conv {"INSERT" => ["SET"]}
  def CASSANDRA_INSERT(args)
    primaryKey = args["primaryKey"]
    key = "#{args["table"]}--#{args["args"][primaryKey]}"
    args["args"].delete(primaryKey)
    values = args["args"]
    ## keylist
    if(args["schema_fields"] == 2)then
      ### String (Key-Value)
      field = values.keys[0]
      value = values[field]
      return SET([key,value])
    else
      ### Value is JSON
      str = convert_json(values)
      return SET([key,str])
    end
  end
  # @conv {"SELECT" => ["GET"]}
  ## args = {"key"=>tablename, "fields" => [],"where"=> [], "limit" =>number}
  def CASSANDRA_SELECT(args)
    results = []
    index = args["cond_keys"].index(args["primaryKey"])
    key = "#{args["table"]}--#{args["cond_values"][index]}"
    if(args["schema_fields"] == 2)then
      ### String (Key-Value)
      results = [GET([key])]
    else
      ### String (Key-JSON)
      strJSON = GET([key])
      docs = []
      if(strJSON.size > 0)then
        docs = parse_json(strJSON)
      end
      docs.each{|field,value|
        if(args["fields"].include?(field))then
          results.push(value)
        end
      }
    end
    return results
  end
  # @conv {"UPDATE" =>["SET","GET"]}
  def CASSANDRA_UPDATE(args)
    args["args"] = {}
    index = args["cond_keys"].index(args["primaryKey"])
    args["args"][args["cond_keys"][index]] = args["cond_values"][index]
    args["set"].each{|f,v|
      args["args"][f] = v
    }
    if(args["schema_fields"] == 2)then
      ## Key-Value
      return CASSANDRA_INSERT(args)
    elsif(args["schema_fields"] == args["set"].keys.size)then
      ## Key-JSON Full Update
      return CASSANDRA_INSERT(args)
    else
      index = args["cond_keys"].index(args["primaryKey"])
      key = "#{args["table"]}--#{args["cond_values"][index]}"
      ## Key-JSON Partical Update
      strJSON = GET([key])
      docs = parse_json(strJSON)
      args["set"].each{|f,v|
        docs[f] = v
      }
      str = convert_json(docs)
      return SET([key,str])
    end
  end
  # @conv {"DELETE" =>["DELETE"]}
  def CASSANDRA_DELETE(args)
    index = args["cond_keys"].index(args["primaryKey"])
    key = "#{args["table"]}--#{args["cond_values"][index]}"
    return DELETE([key])
  end
  # @conv {"DROP" =>["GET","SET","DELETE"] }
  def CASSANDRA_DROP(args)
    keylist = KEYLIST()
    pattern = ""
    if(args["type"] == "table")then
      pattern = ".#{args["key"]}"
    elsif(args["type"] == "keyspace")then
      pattern = "#{args["key"]}."
    end
    deletes = []
    keylist.each{|k|
      if(k.include?(pattern))then
        deletes.push(k)
      end
    }
    deletes.each{|dk|
      DELETE([dk])
    }
    return true
  end
  ##############
  ## JAVA API ##
  ##############
=begin
  # @conv {"BATCH_MUTATE" => ["cassandraDeserialize@client","cassandraSerialize@client","GET","SET"]}
  def CASSANDRA_BATCH_MUTATE(args)
    data = GET([args["key"]],false)
    docs = cassandraDeserialize(data)
    if(args["counterColumn"])then 
      args["args"].each{|arg|
        docs.push(convert_json(arg))
        value = cassandraSerialize(docs)
        SET([args["key"],value])
        value = GET(["__keyslist__"],false)
        keys = cassandraDeserialize(value)
        value = cassandraSerialize(keys)
        SET(["__keylist__",value])
      }
    else
      docs.push(convert_json(args["args"]))
      value = cassandraSerialize(docs)
      SET([args["key"],value])
      value = GET(["__keyslist__"],false)
      keys = cassandraDeserialize(value)
      value = cassandraSerialize(keys)
      SET(["__keylist__",value])
    end
  end
  # @conv {"GET_SLICE" => ["cassandraDeserialize@client","GET"]}
  def CASSANDRA_GET_SLICE(args)
    results = []
    data = GET([args["key"]],false)
    docs = cassandraDeserialize(data)
    docs = docs.take(args["limit"].to_i)
    docs.each{|doc|
      row = parse_json(doc)
      if(CASSANDRA_JUDGE(row, args))then
        results.push(selectField(row, args))
      end
    }
    results.each{|result|
      puts result
    }
  end
  
  # @conv {"GET_RANGE_SLICES" => ["cassandraDeserialize@client","GET"]}
  def CASSANDRA_GET_RANGE_SLICES(args)
    results = []
    data = GET([args["key"]],false)
    docs = cassandraDeserialize(data)
    docs = docs.take(args["limit"].to_i)
    docs.each{|doc|
      row = parse_json(doc)
      if(CASSANDRA_JUDGE(row, args))then
        results.push(selectField(row, args))
      end
    }
    results.each{|result|
      puts result
    }
  end

  # @conv {"MULTIGET_SLICE" => ["cassandraDeserialize@client","GET"]}
  def CASSANDRA_MULTIGET_SLICE(args)
    results = []
    data = GET([args["key"]],false)
    docs = cassandraDeserialize(data)
    if(args["limit"])then
      docs = docs.take(args["limit"].to_i)
    end
    docs.each{|doc|
      row = parse_json(doc)
      if(CASSANDRA_JUDGE(row, args))then
        results.push(selectField(row, args))
      end
    }
    results.each{|result|
      puts result
    }
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
  def CASSANDRA_JUDGE(doc, args)
    args["where"].each{|__cond__|
      __cond__ = __cond__.split("=")
      fieldname = __cond__[0]
      value = __cond__[1]
      if(doc[fieldname] != value)then
        return false
      end
    }
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
  def cassandraSerialize(array)
    return array.join("__A__")
  end
  def cassandraDeserialize(str)
    return str.split("__A__")
  end

end
