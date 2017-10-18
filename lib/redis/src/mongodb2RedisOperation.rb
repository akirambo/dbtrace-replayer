
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

  # @conv {"insert" => ["set"]}
  ## args[0] --> skip, args[1] = {_id => xx, value => xx, ...}
  def mongodb_insert(args)
    v = "NG"
    if @option[:datamodel] == "DOCUMENT"
      if args[0] && args[0][0] && args[0][1][0]
        ## Documents [SADD]
        doc_args = args[0][1][0].to_json
        doc_args.delete!("'")
        doc_args.delete!(" ")
        doc = {
          "key"  => args[0][0],
          "args" => [doc_args],
        }
        v = sadd(doc)
      end
    elsif @option[:datamodel] == "KEYVALUE"
      v = set(mongodb_create_doc_for_keyvalue(args))
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@option[:datamodel]}")
    end
    v
  end

  # @conv {"update" => ["smembers","query@client","del","sadd"]}
  def mongodb_update(args)
    case @option[:datamodel]
    when "DOCUMENT"
      if args["update"].nil? || args["update"]["$set"].nil? && args["group"]
        @logger.error("Not Set update $set query @ mongodb2redis")
        return "NG"
      end
      ## Documents
      data = get([args["key"]])
      docs = eval("[" + data + "]")
      results = mongodb_create_result(docs, args)
      set([args["key"], results])
    when "KEYVALUE"
      set(mongodb_create_doc_for_keyvalue(args))
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@option[:datamodel]}")
      return "NG"
    end
    "OK"
  end

  def mongodb_create_doc_for_keyvalue(args)
    id = ""
    arg = ""
    args[0][1][0].each_key do |k|
      if k == @option[:key_of_keyvalue]
        id = args[0][1][0][k]
      else
        arg = args[0][1][0][k]
      end
    end
    [id, arg]
  end

  def mongodb_create_result(docs, args)
    results = []
    replace_flag = true
    docs.each_index do |index|
      if replace_flag
        doc = parse_json(docs[index])
        if args["query"].nil? || args["query"] == {} || mongodb_query(doc, args["query"])
          args["update"]["$set"].each do |k, v|
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
    results.to_json
  end

  # @conv {"find" => ["get,"query@client"]}
  def mongodb_find(args)
    results = []
    case @option[:datamodel]
    when "DOCUMENT" then
      results = mongodb_find_document(args)
    when "KEYVALUE"
      ## documents
      key = args["filter"][@option[:key_of_keyvalue]]
      results = get([key])
    else
      return "NG"
    end
    results
  end

  def mongodb_find_document(args)
    ## documents
    data = smembers([args["key"]], true)
    docs = []
    unless data.nil?
      docs = eval("[" + data + "]")
    end
    results = []
    if args["filter"].nil? || args["filter"] == {}
      results = docs
    else
      docs.each do |doc|
        if mongodb_query(doc, args["filter"])
          results.push(doc)
        end
      end
    end
    results
  end

  # @conv {"delete" => ["smembers","query@client","srem"]}
  def mongodb_delete(args)
    v = "NG"
    case @option[:datamodel]
    when "DOCUMENT"
      if args["filter"].size.zero?
        v = del([args["key"]])
      else
        new_docs = mongodb_delete_newdocs(args)
        if new_docs.size.zero?
          v = del(args["key"])
        else
          value = new_docs.to_json
          v = set([args["key"], value])
        end
      end
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@option[:datamodel]}")
    end
    v
  end

  def mongodb_delete_newdocs(args)
    new_docs = []
    data = get([args["key"]])
    docs = if data != "[]"
             eval("[" + data + "]")
           else
             []
           end
    docs.each_index do |index|
      doc = parse_json(docs[index])
      unless mongodb_query(doc, args["filter"])
        new_docs.push(convert_json(doc))
      end
    end
    new_docs
  end

  # @conv {"findandmodify" => ["undefined"]}
  def mongodb_findandmodify(args)
    @logger.debug("mongodb_findandmodify is not implemented [#{args}]")
    "NG"
  end

  # @conv {"count" => ["smembers","query@client","count@client"]}
  def mongodb_count(args)
    count = 0
    case @option[:datamodel]
    when "DOCUMENT" then
      docs = smembers([args["key"]], true)
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
      @logger.error("Unsupported Data Model @ mongodb2redis #{@option[:datamodel]}")
    end
    count
  end

  # @conv {"aggregate" => ["smembers","accumulation@client"]}
  def mongodb_aggregate(args)
    @logger.debug("mongodb_aggregate")
    docs = smembers([args["key"]], true)
    result = {}
    params = @query_parser.get_parameter(args)
    docs = eval("[" + docs + "]")
    firstflag = true
    key2realkey = nil
    docs.each do |doc|
      monitor("client", "match")
      flag = mongodb_query(doc, args["match"])
      monitor("client", "match")
      if flag
        if firstflag
          key2realkey = @query_parser.createkey2realkey(doc, params["cond"])
          firstflag = false
        end
        # create group key
        key = @query_parser.create_groupkey(doc, params["cond"])
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

  # @conv {"upsert" => ["undefined"]}
  def mongodb_upsert(args)
    @logger.warn("Unsupported Upsert :#{args} & Covert Insert ")
    mongodb_insert(args)
  end

  # @conv {"group" => ["undefined"]}
  def mongodb_group(args)
    @logger.warn("Unsupported Group :#{args}")
    "NG"
  end

  # @conv {"mapreduce" => ["undefined"]}
  def mongodb_mapreduce(args)
    @logger.warn("Unsupported MapReduce :#{args}")
    "NG"
  end

  ###################
  ## QUERY PROCESS ##
  ###################
  def mongodb_query(doc, query)
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
    if mongo_query_field_nonmatch(value, cond)
      return false
    elsif value.class.to_s == "Float"
      return value == cond.to_f
    elsif %w[Integer Fixnum].include?(value.class.to_s)
      return value == cond.to_i
    end
    true
  end

  def mongo_query_field_nonmatch(value, cond)
    if cond.is_a?(Hash) && !@query_processor.query(cond, value)
      return true
    elsif value.class.to_s == "String" &&
          value.delete("\"").delete(" ") != cond.delete(" ")
      ## Field Matching
      return true
    end
    false
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
    if args.include?("upsert: true")
      operand = "upsert"
    end
    result = { "operand" => "mongodb_#{operand}", "args" => nil }
    result["args"] = @parser.exec(operand, args)
    result
  end
end
