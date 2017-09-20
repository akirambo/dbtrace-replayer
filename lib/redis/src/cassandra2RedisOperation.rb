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
  # @conv {"insert" => ["set","hmset"]}
  def cassandra_insert(args)
    primarykey = args["primaryKey"]
    if args["schema_fields"] == 2
      ### String (Key-Value)
      key = "#{args["table"]}--#{args["args"][primarykey]}"
      fieldnames = args["args"].keys
      fieldnames.delete(args["primaryKey"])
      value = args["args"][fieldnames[0]]
      return set([key, value])
    else
      ### Hash
      args["key"] = "#{args["table"]}--#{args["args"][primarykey]}"
      args["args"].delete(primarykey)
      return hmset(args)
    end
  end

  # @conv {"select" => ["get","hmget"]}
  def cassandra_select(args)
    data = []
    primarykey = args["primaryKey"]
    idx = args["cond_keys"].index(primarykey)
    key = "#{args["table"]}--#{args["cond_values"][0].delete("\"")}"
    if idx && args["cond_values"][idx]
      key = "#{args["table"]}--#{args["cond_values"][idx].delete("\"")}"
    end
    if args["schema_fields"] == 2
      ### String (Key-Value)
      data = [get([key])]
    else
      ### Hash
      ## create Key
      args__ = {
        "key"  => key,
        "args" => args["fields"],
      }
      data = hmget(args__, false)
    end
    data
  end

  # @conv {"update" => ["insert","hmset"]}
  def cassandra_update(args)
    ### convert arguments
    primarykey = args["primaryKey"]
    idx = args["cond_keys"].index(primarykey)
    args["args"] = args["set"]
    args["args"][primarykey] = args["cond_values"][idx]
    cassandra_insert(args)
  end

  # @conv {"delete" => ["del","hdel"]}
  def cassandra_delete(args)
    if args["schema_fields"] == 2 || args["fields"] != "*"
      ## Strings(Key-Value )
      return del([args["table"]])
    end
    ## hash
    hdel([args["table"], args["fields"]])
  end

  # @conv {"drop" => ["smember","srem","sadd"]}
  def cassandra_drop(args)
    targetkeys = keys(args["key"], args["type"])
    ## drop Table
    del(targetkeys)
  end

  #############
  ## PREPARE ##
  #############
  def prepare_cassandra(operand, args)
    ## PREPARE OPERATION & ARGS
    result = {}
    result["operand"] = "cassandra_#{operand.downcase}"
    result["args"] = @parser.exec(operand.downcase, args)
    result
  end
end
