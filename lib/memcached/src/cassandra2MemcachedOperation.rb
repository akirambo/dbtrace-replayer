
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
  def cassandra_insert(args)
    primarykey = args["primaryKey"]
    args["args"].delete(primarykey)
    values = args["args"]
    ## keylist
    if args["schema_fields"] == 2
      ### String (Key-Value)
      key = "#{args["table"]}--#{args["args"][primarykey]}"
      field = values.keys[0]
      value = values[field]
      return set([key, value])
    else
      ### Value is JSON
      key = args["table"]
      str = convert_json(values)
      return set([key, str])
    end
  end

  # @conv {"SELECT" => ["GET"]}
  ## args = {"key"=>tablename, "fields" => [],"where"=> [], "limit" =>number}
  def cassandra_select(args)
    results = []
    index = args["cond_keys"].index(args["primaryKey"])
    key = if index && args["cond_values"][index]
            "#{args["table"]}--#{args["cond_values"][index]}"
          else
            "#{args["table"]}"
          end
    if args["schema_fields"] == 2
      ### String (Key-Value)
      results = [get([key])]
    else
      ### String (Key-JSON)
      str_json = get([key])
      docs = []
      unless str_json.empty?
        docs = parse_json(str_json)
      end
      docs.each do |field, value|
        if args["fields"].include?(field)
          results.push(value)
        end
      end
    end
    results
  end

  # @conv {"UPDATE" =>["SET","GET"]}
  def cassandra_update(args)
    args["args"] = {}
    index = args["cond_keys"].index(args["primaryKey"])
    args["args"][args["cond_keys"][index]] = args["cond_values"][index]
    args["set"].each do |f, v|
      args["args"][f] = v
    end
    if args["schema_fields"] == 2
      ## Key-Value
      #key = "#{args["table"]}--#{args["cond_values"][index]}"
      return cassandra_insert(args)
    elsif args["schema_fields"] == args["set"].keys.size
      ## Key-JSON Full Update
      return cassandra_insert(args)
    else
      index = args["cond_keys"].index(args["primaryKey"])
      key = args["table"]
      ## Key-JSON Partical Update
      str_json = get([key])
      docs = parse_json(str_json)
      args["set"].each do |f, v|
        docs[f] = v
      end
      str = convert_json(docs)
      return set([key, str])
    end
  end

  # @conv {"DELETE" =>["DELETE"]}
  def cassandra_delete(args)
    index = args["cond_keys"].index(args["primaryKey"])
    if !index.nil? && !args["cond_values"][index].nil?
      key = "#{args["table"]}--#{args["cond_values"][index]}"
      return delete([key])
    end
    true
  end

  # @conv {"DROP" =>["GET","SET","DELETE"] }
  def cassandra_drop(args)
    keylist_ = keylist
    pattern = ""
    pattern = if args["type"] == "table"
                ".#{args["key"]}"
              elsif args["type"] == "keyspace"
                "#{args["key"]}."
              end
    deletes = []
    keylist_.each do |k|
      if k.include?(pattern)
        deletes.push(k)
      end
    end
    deletes.each do |dk|
      delete([dk])
    end
    true
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

  def cassandra_judge(doc, args)
    args["where"].each do |cond__|
      cond__ = cond__.split("=")
      fieldname = cond__[0]
      value = cond__[1]
      if doc[fieldname] != value
        return false
      end
    end
    true
  end

  def select_field(hash, args)
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

  def cassandra_serialize(array)
    array.join("__A__")
  end

  def cassandra_deserialize(str)
    str.split("__A__")
  end
end
