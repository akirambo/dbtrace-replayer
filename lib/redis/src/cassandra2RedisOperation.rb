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
    primarykey = args["primaryKey"]
    if args["schema_fields"] == 2
      ### String (Key-Value)
      key = "#{args["table"]}--#{args["args"][primaryKey]}"
      fieldnames = args["args"].keys
      fieldnames.delete(args["primaryKey"])
      value = args["args"][fieldnames[0]]
      return SET([key, value])
    else
      ### Hash
      args["key"] = "#{args["table"]}--#{args["args"][primaryKey]}"
      args["args"].delete(primarykey)
      return HMSET(args)
    end
  end

  # @conv {"SELECT" => ["GET","HMGET"]}
  def CASSANDRA_SELECT(args)
    data = []
    primarykey = args["primaryKey"]
    idx = args["cond_keys"].index(primarykey)
    key = "#{args["table"]}--#{args["cond_values"][0].delete(/\"/)}"
    if idx && args["cond_values"][idx]
      key = "#{args["table"]}--#{args["cond_values"][idx].delete(/\"/)}"
    end
    if args["schema_fields"] == 2
      ### String (Key-Value)
      data = [GET([key])]
    else
      ### Hash
      ## create Key
      args__ = {
        "key"  => key,
        "args" => args["fields"],
      }
      data = HMGET(args__, false)
    end
    data
  end

  # @conv {"UPDATE" => ["INSERT","HMSET"]}
  def CASSANDRA_UPDATE(args)
    ### convert arguments
    primarykey = args["primaryKey"]
    idx = args["cond_keys"].index(primarykey)
    args["args"] = args["set"]
    args["args"][primarykey] = args["cond_values"][idx]
    CASSANDRA_INSERT(args)
  end

  # @conv {"DELETE" => ["DEL","HDEL"]}
  def CASSANDRA_DELETE(args)
    if args["schema_fields"] == 2 || args["fields"] != "*"
      ## Strings(Key-Value )
      return DEL([args["table"]])
    end
    ## Hash
    HDEL([args["table"], args["fields"]])
  end

  # @conv {"DROP" => ["SMEMBER","SREM","SADD"]}
  def CASSANDRA_DROP(args)
    targetkeys = KEYS(args["key"], args["type"])
    ## drop Table
    DEL(targetkeys)
  end

  #############
  ## PREPARE ##
  #############
  def prepare_CASSANDRA(operand, args)
    ## PREPARE OPERATION & ARGS
    result = {}
    result["operand"] = "CASSANDRA_#{operand.upcase}"
    result["args"] = @parser.exec(operand.upcase, args)
    result
  end

  def cassandraQuery(result, args)
    ## where
    if args["where"]
      args["where"].each do |cond__|
        cond__ = cond__.split("=")
        fieldname = cond__[0]
        value = cond__[1]
        if result[fieldname] != value
          return false
        end
      end
    end
    true
  end

  def selectField(hash, args)
    row = {}
    if args["fields"][0] == "*"
      return hash
    else
      args["fields"][0].split(",").each do |field|
        row[field] = hash[field]
      end
    end
    row
  end
end
