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


module MongoDB2RedisOperation
  MONGODB_NUMERIC_QUERY = ["$gt", "$gte", "$lt", "$lte"].freeze
  MONGODB_STRING_QUERY  = ["$eq", "$ne", "$in", "$nin"].freeze

  private

  # @conv {"INSERT" => ["SET"]}
  ## args[0] --> skip, args[1] = {_id => xx, value => xx, ...}
  def MONGODB_INSERT(args)
    v = "NG"
    if @options[:datamodel] == "DOCUMENT"
      ## Documents [SADD]
      if args[0] && args[0][0] && args[0][1][0]
        doc = {
          "key"  => args[0][0],
          "args" => args[0][1][0].to_json,
        }
        doc["args"].delete!("'")
        doc["args"].delete!(" ")
      end
      v = SADD(doc)
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    v
  end

  # @conv {"UPDATE" => ["SMEMBERS","QUERY@client","DEL","SADD"]}
  def MONGODB_UPDATE(args)
    results = []
    if @options[:datamodel] != "DOCUMENT"
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
      return "NG"
    end
    if args["update"].nil? || args["update"]["$set"].nil?
      @logger.error("Not Set update $set query @ mongodb2redis")
      return "NG"
    end
    ## Documents
    new_vals = args["update"]["$set"]
    data = GET([args["key"]])
    docs = eval("[" + data + "]")
    replace_flag = true
    docs.each_index do |index|
      if replace_flag
        doc = parse_json(docs[index])
        if args["query"].nil? || args["query"] == {} || mongodbQuery(doc, args["query"])
          new_vals.each do |k, v|
            doc[k.to_sym] = v.delete(" ")
          end
        end
        results.push(doc)
        unless args["multi"]
          replace_flag = false
        end
      else
        results.push(docs[index])
      end
    end
    SET([args["key"], results.to_json])
  end

  # @conv {"FIND" => ["GET,"QUERY@client"]}
  def MONGODB_FIND(args)
    results = []
    case @options[:datamodel]
    when "DOCUMENT" then
      ## Documents
      data = SMEMBERS([args["key"]], true)
      docs = []
      unless data.nil?
        docs = eval("[" + data + "]")
      end
      results = []
      if args["filter"].nil? || args["filter"] == {}
        results = docs
      else
        docs.each do |doc|
          if mongodbQuery(doc, args["filter"])
            results.push(doc)
          end
        end
      end
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
      return "NG"
    end
    results
  end

  # @conv {"DELETE" => ["SMEMBERS","QUERY@client","SREM"]}
  def MONGODB_DELETE(args)
    v = "NG"
    case @options[:datamodel]
    when "DOCUMENT"
      if args["filter"].size.zero?
        v = DEL([args["key"]])
      else
        data = GET([args["key"]])
        new_docs = []
        docs = eval("[" + data + "]")
        docs.each_index do |index|
          doc = parse_json(docs[index])
          unless mongodbQuery(doc, args["filter"])
            new_docs.push(convert_json(doc))
          end
        end
        if new_docs.size.zero?
          v = DEL(args["key"])
        else
          value = new_docs.to_json
          v = SET([args["key"], value])
        end
      end
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    v
  end

  # @conv {"FINDANDMODIFY" => ["undefined"]}
  def MONGODB_FINDANDMODIFY(args)
    @logger.debug("MONGODB_FINDANDMODIFY is not implemented [#{args}]")
    "NG"
  end

  # @conv {"COUNT" => ["SMEMBERS","QUERY@client","COUNT@client"]}
  def MONGODB_COUNT(args)
    count = 0
    case @options[:datamodel]
    when "DOCUMENT" then
      docs = SMEMBERS([args["key"]], true)
      monitor("client", "count")
      args["query"].each_key do |k|
        query = generate_query(k, args["query"][k])
        count__ = docs.scan(query).size
        if count__.zero?
          count = 0
          break
        elsif count.zero? || count > count__
          count = count__
        end
      end
      monitor("client", "count")
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    count
  end

  # @conv {"AGGREGATE" => ["SMEMBERS","ACCUMULATION@client"]}
  def MONGODB_AGGREGATE(args)
    @logger.debug("MONGODB_AGGREGATE")
    docs = SMEMBERS([args["key"]], true)
    result = {}
    params = @query_parser.getParameter(args)
    docs = eval("[" + docs + "]")
    firstflag = true
    key2realkey = nil
    docs.each do |doc|
      monitor("client", "match")
      flag = mongodbQuery(doc, args["match"])
      monitor("client", "match")
      if flag
        if firstflag
          key2realkey = @query_parser.createKey2RealKey(doc, params["cond"])
          firstflag = false
        end
        # create group key
        key = @query_parser.createGroupKey(doc, params["cond"])
        if result[key].nil?
          result[key] = {}
        end
        # do aggregation
        params["cond"].each do |k, v|
          monitor("client", "aggregate")
          result[key][k] = @query_processor.aggregation(result[key][k], doc, v, key2realkey)
          monitor("client", "aggregate")
        end
      end
    end
    result
  end
  # @conv {"MAPREDUCE" => ["undefined"]}
  def MONGODB_MAPREDUCE(args)
    @logger.warn("Unsupported MapReduce :#{args}")
    "NG"
  end

  ###################
  ## QUERY PROCESS ##
  ###################
  def mongodbQuery(doc, query)
    if query && query.class == Hash && !query.keys.empty?
      query.each do |key__, cond|
        key = key__.to_sym
        if doc[key]
          return_value = mongo_query_fieldmatching(doc[key], cond)
          if return_value != true
            return return_value
          end
        else
          return false
        end
        return true
      end
    end
    false
  end

  def mongo_query_fieldmatching(value, cond)
    if cond.kind_of?(Hash) && !@query_processor.query(cond, value)
      return false
    end
    case value.class.to_s
    when "String"
      ## Field Matching
      if value.delete("\"").delete(" ") != cond.delete(" ")
        return false
      end
    when "Float"
      return value == cond.to_f
    else
      if %w[Integer Fixnum].include?(value.class.to_s)
        return value == cond.to_i
      end
    end
    true
  end

  def generate_query(k, val)
    query = ""
    case val.class.to_s
    when "FalseClass" then
      query = '"' + k.split(".").last + '":false'
    when "TrueClass" then
      query = '"' + k.split(".").last + '":true'
    when "String" then
      query = '"' + k.split(".").last + '":"' + val + '"'
    when "Hash" then
      query = '"' + k.split(".").last + '":' + val.to_json
    else
      if %w[Float Integer Fixnum].include?(val.class.to_s)
        query = '"' + k.split(".").last + '":' + val.to_s
      end
    end
    query
  end

  #############
  ## PREPARE ##
  #############
  def prepare_mongodb(operand, args)
    result = { "operand" => "MONGODB_#{operand}", "args" => nil }
    result["args"] = @parser.exec(operand, args)
    result
  end
end
