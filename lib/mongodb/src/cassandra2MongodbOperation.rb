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

module Cassandra2MongodbOperation
  private
  ###############
  ## OPERATION ##
  ###############
=begin
  collection => column family
  doc => {"fieldname"=> "value",....}
=end
  # @conv {"INSERT" => ["INSERT"]}
  def CASSANDRA_INSERT(args)
    docname = args["table"]
    doc = args["args"]
    primaryKey = args["primaryKey"]
    doc["_id"] = doc[primaryKey]
    doc.delete(primaryKey)
    return INSERT([[docname, doc]])
  end
  # @conv {"SELECT" => ["FIND"]}
  def CASSANDRA_SELECT(args)
    cond = {
      "key"  => args["table"],
      "filter" => { "_id" => getPrimaryKey(args)},
      "projection" => nil
    }
    if(args["fields"] != ["*"])then
      if(!cond["projection"])then
        cond["projection"] = {}
      end
      args["fields"].each{|field|
        cond["projection"][field] = 1
      }
    end
    result = []
    FIND(cond).each{|doc|
      doc.delete("_id")
      result = doc.values
    }
    return result
  end
  # @conv {"UPDATE" => ["UPDATE"]}
  def CASSANDRA_UPDATE(args)
    cond = {
      "key" => args["table"],
      "query" => {"_id" => getPrimaryKey(args)},
      "update" => {"$set" => args["set"]}
    }
    return UPDATE(cond)
  end
  # @conv {"DELETE" => ["UPDATE"]}
  def CASSANDRA_DELETE(args)
    cond = {
      "key" => args["table"],
      "query" => {"_id" => getPrimaryKey(args)},
      "update" => {"$unset" => {}}
    }
    args["fields"].each{|field|
      cond["update"]["$unset"][field] = 1
    }
    return UPDATE(cond)
  end
  # @conv {"DROP" => [""]}
  def CASSANDRA_DROP(args)
    return DROP([])
  end
  
  ##############
  ## JAVA API ##
  ##############
=begin
  # @conv {"BATCH_MUTATE" => ["INSERT"]}
  def CASSANDRA_BATCH_MUTATE(args)
    if(args["counterColumn"])then
      args["args"].each{|arg|
        INSERT([[args["key"], arg]])
      }
    else
      INSERT([[args["key"], args["args"]]])
    end
  end
  # @conv {"GET_SLICE" => ["FIND"]}
  def CASSANDRA_GET_SLICE(args)
    cond = {
      "key"  => args["key"],
      "filter" => nil,
      "projection" => {}
    }
    if(args["fields"] != ["*"])then
      args["fields"][0].split(",").each{|field|
        cond["projection"][field] = 1
      }
    end
    FIND(cond,false).each{|doc|
      doc.delete("_id")
      #puts doc
    }
  end
  # @conv {"MULTIGET_SLICE" => ["FIND"]}
  def CASSANDRA_MULTIGET_SLICE(args)
    cond = {
      "key"  => args["key"],
      "filter" => nil,
      "projection" => {}
    }
    if(args["fields"] != ["*"])then
      args["fields"][0].split(",").each{|field|
        cond["projection"][field] = 1
      }
    end
    counter = 0
    FIND(cond,false).each{|doc|
      doc.delete("_id")
      counter += 1
      if(counter <= args["limit"].to_i)then
        #puts doc
      end
    }
  end
  # @conv {"GET_RANGE_SLICES" => ["FIND"]}
  def CASSANDRA_GET_RANGE_SLICES(args)
    cond = {
      "key"  => args["key"],
      "filter" => nil,
      "projection" => {}
    }
    if(args["fields"] != ["*"])then
      args["fields"][0].split(",").each{|field|
        cond["projection"][field] = 1
      }
    end
    counter = 0
    FIND(cond,false).each{|doc|
      doc.delete("_id")
      counter += 1
      if(counter <= args["limit"].to_i)then
        #puts doc
      end
    }
  end
=end  
  #############
  ## PREPARE ##
  #############
  def prepare_cassandra(operand,args)
    ## PREPARE OPERATION & ARGS
    result = {}
    result["operand"] = "CASSANDRA_#{operand.upcase}"
    result["args"]    = @parser.exec(operand.upcase,args)
    return result
  end
  
  def CASSANDRA_JUDGE(result, args)
    ## where
    args["where"].each{|__cond__|
      __cond__ = __cond__.split("=")
      fieldname = __cond__[0]
      value = __cond__[1]
      if(result[fieldname] != value)then
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
  def cassandraSerialize(hash)
    return convert_json(hash)
  end
  def cassandraDeserialize(array)
    result = []
    array.each{|row|
      result.push(parse_json(row))
    }
    return result
  end
  def getPrimaryKey(args)
    primaryKey = args["primaryKey"]
    index = args["cond_keys"].index(primaryKey)
    id = args["cond_values"][index]
    return id
  end
end
  

