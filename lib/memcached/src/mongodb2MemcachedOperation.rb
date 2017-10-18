# coding: utf-8

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

module MongoDB2MemcachedOperation
  private

  # @conv {"insert" => ["get","set"]}
  def mongodb_insert(args)
    mongodb_insert_update(args, "insert")
  end

  # @conv {"update" => ["query@client","GET","REPLACE"]}
  def mongodb_update(args)
    mongodb_insert_update(args, "update")
  end

  def mongodb_group(args)
    @logger.warn("Unsupported Command Group")
    true
  end

  def mongodb_mapreduce(args)
    @logger.warn("Unsupported Command Mapreduce")
    true
  end

  def mongodb_insert_update(args, type)
    case @option[:datamodel]
    when "KEYVALUE" then
      return mongodb_process_keyvalue(args, type)
    when "DOCUMENT" then
      return mongodb_process_document(args, type)
    end
    false
  end

  def mongodb_process_keyvalue(args, type)
    if type == "insert"
      return mongodb_insert_keyvalue(args)
    elsif type == "update"
      return mongodb_update_keyvalue(args)
    end
    false
  end

  def mongodb_process_document(args, type)
    if type == "insert"
      return mongodb_insert_document(args)
    elsif type == "update"
      return mongodb_update_document(args)
    end
    false
  end

  # @conv {"find" => ["mongodb_query@client","GET"]}
  ## args = {"key"=>key, "filter"=>filter}
  def mongodb_find(args)
    case @option[:datamodel]
    when "KEYVALUE" then
      docs = get([args["key"] + args["filter"]["_id"]])
      return document_normalize(docs)
    when "DOCUMENT" then
      return mongodb_find_document(args)
    end
    []
  end

  # @conv {"delete" => ["mongodb_query@client","GET","DELETE","REPLACE"]}
  def mongodb_delete(args)
    case @option[:datamodel]
    when "KEYVALUE" then
      col = args["key"] + args["filter"]["_id"]
      return delete([col])
    when "DOCUMENT" then
      return mongodb_delete_document(args)
    end
    false
  end

  # @conv {"COUNT" => ["query@client","GET"]}
  def mongodb_count(args)
    case @option[:datamodel]
    when "KEYVALUE" then
      data = get([args["key"] + args["query"]["_id"]], false)
      unless data.size.zero?
        return 1
      end
    when "DOCUMENT" then
      return mongodb_count_document(args)
    end
    0
  end

  # @conv {"AGGREGATE" => ["query@client","accumulationclient","GET"]}
  ## args = {"key"=>key, "match"=>{"colname1"=>"STRING"}, "group"=>"{}", "unwind"=>"{}"}
  def mongodb_aggregate(args)
    result = {}
    match_duration = 0.0
    aggregate_duration = 0.0
    add_count(:match)
    add_count(:aggregate)
    data = get([args["key"]], false)
    docs = @utils.symbolhash2stringhash(eval(data))
    params = @query_parser.get_parameter(args)
    first_flag = true
    key2realkey = nil
    docs.each do |doc|
      start_time = Time.now
      match_flag = mongodb_query(doc, args["match"], "match")
      match_duration = start_time - Time.now
      if match_flag
        if first_flag
          key2realkey = @query_parser.createkey2realkey(doc, params["cond"], nil)
          first_flag = false
        end
        # create group key
        key = @query_parser.create_groupkey(doc, params["cond"])
        if key == ""
          next
        end
        if result[key].nil?
          result[key] = {}
        end
        # do aggregation
        params["cond"].each do |k, v|
          start_time = Time.now
          result[key][k] = @query_processor.aggregation(result[key][k], doc, v, key2realkey)
          aggregate_duration += Time.now - start_time
        end
      end
    end
    add_duration(match_duration, "client", "match")
    add_duration(aggregate_duration, "client", "aggregate")
    result
  end

  ###################
  ## QUERY PROCESS ##
  ###################
  ### Supported   :: Single Query
  def mongodb_query(doc, query, _)
    if query
      query = document_symbolize(query)
    end
    if query
      query.each do |key, cond|
        if doc[key]
          unless mongodb_query_has_doc(doc, key, cond)
            return false
          end
        else
          return false
        end
      end
    end
    true
  end

  def mongodb_query_has_doc(doc, key, cond)
    value = doc[key]
    if !cond.is_a?(Hash)
      value = doc[key].delete("\"")
      ## Field Matching
      tt = value.delete("\"").delete(" ")
      cond = cond.delete(" ")
      if tt != cond
        return false
      end
    else
      unless @query_processor.query(cond, value)
        return false
      end
    end
    true
  end

  def mongodb_replace(doc, args)
    hashed_doc = parse_json(doc)
    args["update"].each do |operation, values|
      case operation
      when "$set" then
        values.each do |key, value|
          hashed_doc[key] = value
        end
      else
        ## check colum_name
        unless values.is_a?(Hash)
          hashed_doc[operation] = values
        end
        # @logger.warn("Unsupported UPDATE Operation '#{operation}' !!")
      end
    end
    convert_json(hashed_doc)
  end

  #############
  ## PREPARE ##
  #############
  def prepare_mongodb(operand, args)
    result = { "operand" => "mongodb_#{operand.downcase}", "args" => nil }
    result["args"] = @parser.exec(operand.downcase, args)
    result
  end

  ####################
  ## Private Method ##
  ####################

  ## INSERT ##
  def mongodb_insert_keyvalue(args)
    args.each do |arg|
      if !arg[1].instance_of?(Array)
        key = arg[0] + arg[1]["_id"]
        value = ""
        arg[1].each do |k, v|
          if k != "_id"
            value = v
          end
        end
        unless set([key, value])
          return false
        end
      else
        arg[1].each do |kv|
          key = arg[0] + kv["_id"]
          value = ""
          kv.each do |k, v|
            if k != "_id"
              value = v
            end
          end
          unless set([key, value])
            return false
          end
        end
      end
    end
    true
  end

  def mongodb_insert_document(args)
    args.each do |insert_docs|
      ## Create New Data
      key = insert_docs[0]
      docs = @utils.stringhash2symbolhash(insert_docs[1])
      docs.each do |doc|
        doc[:_id].sub!(/ObjectId\(\'(\w+)\'\)/, '\1')
      end
      ## GET exists data
      predocs = mongodb_get_stored_document(key)
      if predocs && predocs.class == Array && !predocs.size.zero?
        docs.concat(predocs)
      end
      ## Commit
      unless set([key, docs.to_json])
        return false
      end
    end
    true
  end

  def mongodb_get_stored_document(key)
    predocs = ""
    predocs__ = get([key])
    if predocs__ && !predocs__.size.zero?
      predocs = document_normalize(predocs__)
    end
    predocs
  end

  ## UPDATE ##
  def mongodb_update_keyvalue(args)
    col = args["key"] + args["query"]["_id"]
    new_val = ""
    args["update"]["$set"].each do |_, v|
      new_val = v
    end
    replace([col, new_val])
  end

  def mongodb_update_document(args)
    results = []
    if args["update"] && args["update"]["$set"]
      new_vals = document_symbolize(args["update"]["$set"])
      results = mongodb_replace_doc(args, new_vals)
    else
      return false
    end
    value = results.to_json
    replace([args["key"], value])
  end

  def mongodb_replace_doc(args, new_vals)
    results = []
    data = get([args["key"]])
    docs = document_normalize(data)
    replace_flag = true
    docs.each_index do |index|
      if replace_flag
        doc = docs[index]
        if args["query"].nil? || args["query"] == {} || mongodb_query(doc, args["query"], "query")
          new_vals.each do |k, v|
            doc[k] = v
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
    results
  end

  ## FIND ##
  def mongodb_find_document(args)
    data = get([args["key"]])
    docs = []
    if data
      docs = document_normalize(data)
    end
    results = []
    if args["filter"].nil? || args["filter"] == {}
      docs.each do |doc|
        results.push(@utils.symbolhash2stringhash(doc))
      end
    else
      results = mongodb_find_filter(docs, args)
    end
    results
  end

  def mongodb_find_filter(docs, args)
    results = []
    docs.each_index do |index|
      doc = docs[index]
      if doc.class == Array
        doc.each_index do |idx|
          if mongodb_query(doc[idx], args["filter"], "filter")
            results.push(@utils.symbolhash2stringhash(doc[idx]))
          end
        end
      elsif mongodb_query(doc, args["filter"], "filter")
        results.push(@utils.symbolhash2stringhash(doc))
      end
    end
    results
  end

  ## DELETE ##
  def mongodb_delete_document(args)
    if args["filter"].size.zero?
      delete([args["key"]])
    else
      data = get([args["key"]])
      new_docs = []
      docs = document_normalize(data)
      docs.each_index do |index|
        unless mongodb_query(docs[index], args["filter"], "filter")
          new_docs.push(docs[index])
        end
      end
      if new_docs.empty?
        return delete(args["key"])
      else
        return replace([args["key"], new_docs.to_json])
      end
    end
  end

  ## COUNT ##
  def mongodb_count_document(args)
    count = 0
    docs__ = get([args["key"]])
    if !args["query"].keys.empty? && !docs__.size.zero?
      docs = document_normalize(docs__)
      docs.each do |doc|
        flag = true
        filters = document_symbolize(args["query"])
        filters.each do |key, value|
          if doc[key] != value
            flag = false
            next
          end
          if flag
            count += 1
          end
        end
      end
    end
    count
  end

  def document_symbolize(docs)
    if docs.class == Array
      symbol_docs = []
      docs.each do |doc__|
        doc = Hash[doc__.map { |k, v| [k.to_sym, v] }]
        symbol_docs.push(doc)
      end
    elsif docs.class == Hash
      symbol_docs = Hash[docs.map { |k, v| [k.to_sym, v] }]
    end
    symbol_docs
  end

  def document_normalize(data)
    docs = if data[0] == "["
             eval(data)
           else
             eval("[" + data + "]")
           end
    docs
  end
end
