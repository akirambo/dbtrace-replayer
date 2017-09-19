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
  # collection => column family
  #  doc => {"fieldname"=> "value",....}
  # @conv {"INSERT" => ["INSERT"]}
  def cassandra_insert(args)
    docname = args["table"]
    doc = args["args"]
    primarykey = args["primaryKey"]
    doc["_id"] = doc[primarykey]
    doc.delete(primarykey)
    insert([[docname, doc]])
  end

  # @conv {"SELECT" => ["FIND"]}
  def cassandra_select(args)
    cond = {
      "key" => args["table"],
      "filter" => { "_id" => get_primarykey(args) },
      "projection" => nil,
    }
    if args["fields"] != ["*"]
      unless cond["projection"]
        cond["projection"] = {}
      end
      args["fields"].each do |field|
        cond["projection"][field] = 1
      end
    end
    result = []
    find(cond).each do |doc|
      doc.delete("_id")
      result = doc.values
    end
    result
  end

  # @conv {"UPDATE" => ["UPDATE"]}
  def cassandra_update(args)
    cond = {
      "key" => args["table"],
      "query" => { "_id" => get_primarykey(args) },
      "update" => { "$set" => args["set"] },
    }
    update(cond)
  end

  # @conv {"DELETE" => ["UPDATE"]}
  def cassandra_delete(args)
    cond = {
      "key" => args["table"],
      "query" => { "_id" => get_primarykey(args) },
      "update" => { "$unset" => {} },
    }
    args["fields"].each do |field|
      cond["update"]["$unset"][field] = 1
    end
    update(cond)
  end

  # @conv {"DROP" => [""]}
  def cassandra_drop(args)
    unless args.empty?
      @logger.warn("Unsupported CASSANDRA_DROP with #{args}.")
    end
    drop([])
  end

  #############
  ## PREPARE ##
  #############
  def prepare_cassandra(operand, args)
    ## PREPARE OPERATION & ARGS
    result = {}
    result["operand"] = "cassandra_#{operand}"
    result["args"] = @parser.exec(operand, args)
    result
  end

  def cassandra_judge(result, args)
    ## where
    args["where"].each do |cond__|
      cond__ = cond__.split("=")
      fieldname = cond__[0]
      value = cond__[1]
      if result[fieldname] != value
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

  def cassandra_serialize(hash)
    convert_json(hash)
  end

  def cassandra_deserialize(array)
    result = []
    array.each do |row|
      result.push(parse_json(row))
    end
    result
  end

  def get_primarykey(args)
    primarykey = args["primaryKey"]
    index = args["cond_keys"].index(primarykey)
    id = args["cond_values"][index]
    id
  end
end
