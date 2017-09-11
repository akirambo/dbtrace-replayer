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

  # @conv {"INSERT" => ["GET","SET"]}
  def MONGODB_INSERT(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      return mongodbInsertKeyvalue(args)
    when "DOCUMENT" then
      return mongodbInsertDocument(args)
    end
    false
  end

  # @conv {"UPDATE" => ["query@client","GET","REPLACE"]}
  def MONGODB_UPDATE(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      return mongodbUpdateKeyvalue(args)
    when "DOCUMENT" then
      return mongodbUpdateDocument(args)
    end
    false
  end

  # @conv {"FIND" => ["mongodbQuery@client","GET"]}
  ## args = {"key"=>key, "filter"=>filter}
  def MONGODB_FIND(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      docs = GET([args["key"] + args["filter"]["_id"]])
      return documentNormalize(docs)
    when "DOCUMENT" then
      return mongodbFindDocument(args)
    end
    []
  end

  # @conv {"DELETE" => ["mongodbQuery@client","GET","DELETE","REPLACE"]}
  def MONGODB_DELETE(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      col = args["key"] + args["filter"]["_id"]
      return DELETE([col])
    when "DOCUMENT" then
      return mongodbDeleteDocument(args)
    end
    false
  end

  # @conv {"COUNT" => ["query@client","GET"]}
  def MONGODB_COUNT(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      data = GET([args["key"] + args["query"]["_id"]], false)
      unless data.size.zero?
        return 1
      end
    when "DOCUMENT" then
      return mongodbCountDocument(args)
    end
    0
  end

  # @conv {"AGGREGATE" => ["query@client","accumulationclient","GET"]}
  ## args = {"key"=>key, "match"=>{"colname1"=>"STRING"}, "group"=>"{}", "unwind"=>"{}"}
  def MONGODB_AGGREGATE(args)
    result = {}
    match_duration = 0.0
    aggregate_duration = 0.0
    add_count(:match)
    add_count(:aggregate)
    data = GET([args["key"]], false)
    docs = @utils.symbolhash2stringhash(eval(data))
    params = @queryParser.getParameter(args)
    first_flag = true
    key2realkey = nil
    docs.each do |doc|
      start_time = Time.now
      match_flag = mongodbQuery(doc, args["match"], "match")
      match_duration = start_time - Time.now
      if match_flag
        if first_flag
          key2realkey = @queryParser.createKey2RealKey(doc, params["cond"])
          first_flag = false
        end
        # create group key
        key = @queryParser.createGroupKey(doc, params["cond"])
        if result[key].nil?
          result[key] = {}
        end
        # do aggregation
        params["cond"].each do |k, v|
          start_time = Time.now
          result[key][k] = @queryProcessor.aggregation(result[key][k], doc, v, key2realkey)
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
  def mongodbQuery(doc, query, _)
    if query
      query = documentSymbolize(query)
      query.each do |key, cond|
        if doc[key]
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
            unless @queryProcessor.query(cond, value)
              return false
            end
          end
        else
          return false
        end
      end
    end
    true
  end

  def MONGODB_REPLACE(doc, args)
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
    result = { "operand" => "MONGODB_#{operand}", "args" => nil }
    result["args"] = @parser.exec(operand, args)
    result
  end

  ####################
  ## Private Method ##
  ####################

  ## INSERT ##
  def mongodbInsertKeyvalue(args)
    args.each do |arg|
      if !arg[1].instance_of?(Array)
        key = arg[0] + arg[1]["_id"]
        value = ""
        arg[1].each do |k, v|
          if k != "_id"
            value = v
          end
        end
        unless SET([key, value])
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
          unless SET([key, value])
            return false
          end
        end
      end
    end
    true
  end

  def mongodbInsertDocument(args)
    ## Create New Data
    key = args[0][0]
    docs = @utils.stringhash2symbolhash(args[0][1])
    docs.each do |doc|
      doc[:_id].sub!(/ObjectId\(\'(\w+)\'\)/, '\1')
    end
    ## GET exists data
    predocs = ""
    predocs__ = GET([key])
    if predocs__ && !predocs__.size.zero?
      predocs = documentNormalize(predocs__)
    end
    if predocs && predocs.class == Array && !predocs.size.zero?
      docs.concat(predocs)
    end
    ## Commit
    unless SET([key, docs.to_json])
      return false
    end
    true
  end

  ## UPDATE ##
  def mongodbUpdateKeyvalue(args)
    col = args["key"] + args["query"]["_id"]
    new_val = ""
    args["update"]["$set"].each do |_, v|
      new_val = v
    end
    REPLACE([col, new_val])
  end

  def mongodbUpdateDocument(args)
    results = []
    if args["update"] && args["update"]["$set"]
      new_vals = documentSymbolize(args["update"]["$set"])
      data = GET([args["key"]])
      docs = documentNormalize(data)
      replace_flag = true
      docs.each_index do |index|
        if replace_flag
          doc = docs[index]
          if args["query"].nil? || args["query"] == {} || mongodbQuery(doc, args["query"], "query")
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
    else
      return false
    end
    value = results.to_json
    REPLACE([args["key"], value])
  end

  ## FIND ##
  def mongodbFindDocument(args)
    data = GET([args["key"]])
    docs = []
    if data
      docs = documentNormalize(data)
    end
    results = []
    if args["filter"].nil? || args["filter"] == {}
      docs.each do |doc|
        results.push(@utils.symbolhash2stringhash(doc))
      end
    else
      docs.each_index do |index|
        doc = docs[index]
        if doc.class == Array
          doc.each_index do |idx|
            if mongodbQuery(doc[idx], args["filter"], "filter")
              results.push(@utils.symbolhash2stringhash(doc[idx]))
            end
          end
        elsif mongodbQuery(doc, args["filter"], "filter")
          results.push(@utils.symbolhash2stringhash(doc))
        end
      end
    end
  end

  ## DELETE ##
  def mongodbDeleteDocument(args)
    if args["filter"].size.zero?
      DELETE([args["key"]])
    else
      data = GET([args["key"]])
      new_docs = []
      docs = documentNormalize(data)
      docs.each_index do |index|
        unless mongodbQuery(docs[index], args["filter"], "filter")
          new_docs.push(docs[index])
        end
      end
      if new_docs.empty?
        return DELETE(args["key"])
      else
        return REPLACE([args["key"], new_docs.to_json])
      end
    end
  end

  ## COUNT ##
  def mongodbCountDocument(args)
    count = 0
    docs__ = GET([args["key"]])
    if !args["query"].keys.empty? && !docs__.size.zero?
      docs = documentNormalize(docs__)
      docs.each do |doc|
        flag = true
        filters = documentSymbolize(args["query"])
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

  def documentSymbolize(docs)
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

  def documentNormalize(data)
    docs = if data[0] == "["
             eval(data)
           else
             eval("[" + data + "]")
           end
    docs
  end
end
