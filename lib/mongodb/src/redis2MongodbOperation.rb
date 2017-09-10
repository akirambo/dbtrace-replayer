
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

module Redis2MongodbOperation
  private

  ############
  ## String ##
  ############

  # Data Structure [String] @ Mongodb
  #  Collection :: key
  #  Doc        :: "value" => value
  # @conv {"SET" => ["INSERT"]}
  def REDIS_SET(args)
    setString(args)
  end

  # @conv {"GET" => ["FIND"]}
  def REDIS_GET(args)
    getString(args[0])
  end

  # @conv {"SETNX" => ["INSERT"]}
  def REDIS_SETNX(args)
    REDIS_SET(args)
  end

  # @conv {"SETEX" => ["INSERT"]}
  def REDIS_SETEX(args)
    REDIS_SET(args)
  end

  # @conv {"PSETEX" => ["INSERT"]}
  def REDIS_PSETEX(args)
    REDIS_SET(args)
  end

  # @conv {"MSET" => ["INSERT"]}
  def REDIS_MSET(args)
    args.each do |key, value|
      flag = setString([key, value])
      unless flag
        return false
      end
    end
    true
  end

  # @conv {"MSETNX" => ["INSERT"]}
  def REDIS_MSETNX(args)
    REDIS_MSET(args)
  end

  # @conv {"MGET" => ["FIND"]}
  def REDIS_MGET(args)
    result = getStrings(args)
    result
  end

  # @conv {"INCR" => ["FIND","INSERT"]}
  def REDIS_INCR(args)
    str = getString(args[0])
    value = str.to_i + 1
    updateString(args[0], value)
  end

  # @conv {"INCRBY" => ["FIND","INSERT"]}
  def REDIS_INCRBY(args)
    str = getString(args[0])
    value = str.to_i + args[1].to_i
    updateString(args[0], value)
  end

  # @conv {"DECR" => ["FIND","INSERT"]}
  def REDIS_DECR(args)
    str = getString(args[0])
    value = str.to_i - 1
    updateString(args[0], value)
  end

  # @conv {"DECRBY" => ["FIND","INSERT"]}
  def REDIS_DECRBY(args)
    str = getString(args[0])
    value = str.to_i - args[1].to_i
    updateString(args[0], value)
  end

  # @conv {"APPEND" =>  ["FIND","INSERT"]}
  def REDIS_APPEND(args)
    str = getString(args[0])
    value = str + args[1]
    updateString(args[0], value)
  end

  # @conv {"GETSET" =>  ["FIND","INSERT"]}
  def REDIS_GETSET(args)
    str = getString(args[0])
    updateString(args[0], args[1])
    str
  end

  # @conv {"STRLEN" => ["FIND","LENGTH@client"]}
  def REDIS_STRLEN(args)
    getString(args[0]).size
  end

  # @conv {"DELETE" => ["DELETE"]}
  def REDIS_DEL(args__)
    args = {
      "key" => "testdb.col",
      "filter" => { "_id" => args__[0] },
    }
    DELETE(args)
  end

  def getStrings(keys)
    hash = {}
    hash["key"] = "testdb.col"
    hash["filter"] = { "_id" => { "$in" => keys } }
    values = FIND(hash)
    if !values || values.empty?
      return []
    end
    result = []
    values.each do |kv|
      result.push(kv["value"])
    end
    result
  end

  def getString(key)
    hash = {
      "key" => "testdb.col",
      "filter" => { "_id" => key },
    }
    values = FIND(hash)
    if !values || values.empty?
      return ""
    end
    values[0]["value"]
  end

  # collectionName:: test, doc : "_id" => args[0], "value" => args[1]
  def setString(args)
    INSERT([["testdb.col", { "_id" => args[0], "value" => args[1] }]])
  end

  def updateString(key, value)
    args = {
      "key" => "testdb.col",
      "update" => nil,
      "multi" => false,
      "query" => { "_id" => key },
    }
    args["update"] = { "value" => value }
    UPDATE(args)
  end
  ###########
  ## Lists ##
  ###########
  # Data Structure [List] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"key" => "list", "index" => num, "value" => value}

  # @conv {"LPUSH" => ["AGGREGATE","INSERT","UPDATE"]}
  def REDIS_LPUSH(args)
    updateIndex(args[0], "lpush")
    pushList(args[0], args[1], "lpush")
  end

  # @conv {"RPUSH" => ["AGGREGATE","INSERT"]}
  def REDIS_RPUSH(args)
    pushList(args[0], args[1], "rpush")
  end

  # @conv {"LLEN" => ["COUNT"]}
  def REDIS_LLEN(args)
    COUNT("key" => args[0])
  end

  # @conv {"LRANGE" => ["FIND"]}
  def REDIS_LRANGE(args_)
    args = build_ltrimtype_args(args_)
    result = []
    FIND(args).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"LTRIM" => ["DELETE","UPDATE"]}
  def REDIS_LTRIM(args_)
    args = build_ltrimtype_args(args_)
    v = DELETE(args)
    opt = { "start" => args_[1].to_i,
            "end" => args_[2].to_i }
    updateIndex(args["key"], "ltrim", opt)
    v
  end

  # @conv {"LINDEX" => ["FIND"]}
  def REDIS_LINDEX(args_)
    args = {
      "key" => args_[0],
      "filter" => { "index" => { "$eq" => args_[1].to_i } },
    }
    result = []
    FIND(args).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"LSET" => ["UPDATE"]}
  def REDIS_LSET(args)
    updateIndex(args[0], "lset", args[1])
    pushList(args[0], args[2], "lset", args[1])
  end

  # @conv {"LREM" => ["DELETE","UPDATE"]}
  def REDIS_LREM(args)
    ## get list (value == args[2])
    hash = {
      "key" => args[0],
      "filter" => { "value" => args[2] },
    }
    result = []
    str = FIND(hash)
    str.each do |doc|
      if result.size < args[1].to_i
        result.push(doc["index"])
      end
    end
    result.each do |index|
      ## delete element
      delListWithIndex(args[0], index)
      ## update
      updateIndex(args[0], "lrem", index)
    end
    true
  end

  # @conv {"LPOP" => ["FIND","DELETE","UPDATE"]}
  def REDIS_LPOP(args)
    v = getListWithIndex(args[0], 0)
    ## delete element
    delListWithIndex(args[0], 0)
    ## update
    updateIndex(args[0], "lpop", 0)
    v
  end

  # @conv {"RPOP" => ["DELETE","UPDATE"]}
  def REDIS_RPOP(args)
    index = getNewIndex(args[0], "max") - 1
    data = getListWithIndex(args[0], index)
    ## delete element
    delListWithIndex(args[0], index)
    data
  end

  # @conv {"RPOPLPUSH" => ["DELETE","UPDATE","INSERT","AGGEREGATE"]}
  def REDIS_RPOPLPUSH(args)
    data = REDIS_RPOP([args[0]])
    REDIS_LPUSH([args[1], data])
    data
  end

  ## Tools for LIST Structure.
  def pushList(key, value, opt, index = 0)
    case opt
    when "rpush" then
      index = getNewIndex(key, "max")
    when "lpush" then
      index = getNewIndex(key, "min")
    end
    # list = getList(key)
    doc = { "key" => "list", "value" => value, "index" => index.to_i }
    INSERT([[key, doc]])
  end

  def getList(key)
    hash = {}
    hash["key"] = key
    hash["filter"] = {}
    list = FIND(hash)
    list
  end

  def getListWithIndex(key, index)
    hash = {}
    hash["key"] = key
    hash["filter"] = { "index" => { "$eq" => index } }
    list = FIND(hash)
    if list && list.size == 1
      return list[0]["value"]
    end
    ""
  end

  def delListWithIndex(key, index)
    args = {
      "key" => key,
      "filter" => { "index" => index },
    }
    DELETE(args)
  end

  ## opt == max or min
  def getNewIndex(key, opt)
    args = {
      "key" => key,
      "match" => {},
      "accumulator_id" => "key",
      "accumulator_colname" => opt,
      "accumulator_target" => "$index",
      "accumulator" => "$#{opt}",
    }
    val = AGGREGATE(args)
    if val && !val.empty?
      if opt == "max"
        return val[0][opt].to_i + 1
      elsif opt == "min"
        return val[0][opt].to_i - 1
      end
    end
    0
  end

  ## type = lpush,ltrim
  def updateIndex(key, type, opt = nil)
    args = {
      "key" => key, "query" => {},
      "update" => {}, "multi" => true
    }
    flag = true
    case type
    when "lpush" then
      args["update"] = { "$inc" => { "index" => 1 } }
    when "ltrim" then
      ## opt["end"]
      args["query"] = { "index" => { "$gt" => opt["end"].to_i } }
      decr = (opt["end"].to_i - opt["start"].to_i + 1) * -1
      args["update"] = { "$inc" => { "index" => decr } }
    when "lset" then
      args["query"] = { "index" => { "$gte" => opt.to_i } }
      args["update"] = { "$inc" => { "index" => 1 } }
    when "lrem" then
      args["query"] = { "index" => { "$gte" => opt.to_i } }
      args["update"] = { "$inc" => { "index" => -1 } }
    when "lpop" then
      args["query"] = { "index" => { "$gte" => opt.to_i } }
      args["update"] = { "$inc" => { "index" => -1 } }
    else
      flag = false
      @logger.error(" #{type}")
    end
    if flag
      return UPDATE(args)
    end
    flag
  end

  #########
  ## Set ##
  #########
  # Data Structure [Set] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"value" => value}
  # @conv {"SADD" => ["INSERT"]}
  def REDIS_SADD(args, value = nil)
    r = false
    unless value
      r = pushSet(args[0], args[1])
    end
    r
  end

  # @conv {"SREM" => ["DELETE"]}
  def REDIS_SREM(args)
    delSet(args[0], args[1])
  end

  # @conv {"SISMEMBER" => ["COUNT"]}
  def REDIS_SISMEMBER(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1] },
    }
    COUNT(args) > 0
  end

  # @conv {"SPOP" => ["FIND","DELETE"]}
  def REDIS_SPOP(args)
    set = getSet(args[0])
    value = set.sample["value"]
    delSet(args[0], value)
    value
  end

  # @conv {"SMOVE" => ["DELETE","INSERT"]}
  def REDIS_SMOVE(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    delSet(srckey, member)
    ## ADD member to dstKey
    pushSet(dstkey, member)
  end

  # @conv {"REDIS_SADD" => ["COUNT"]}
  def REDIS_SCARD(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1] },
    }
    COUNT(args)
  end

  # @conv {"SINTER" => ["FIND"]}
  def REDIS_SINTER(args)
    gotten_set = {}
    args.each do |key|
      gotten_set[key] = getSet(key)
    end
    result = []
    args.each do |key|
      members = []
      gotten_set[key].each do |val|
        members.push(val["value"])
      end
      if result.empty?
        result = members
      else
        result &= members
      end
    end
    result
  end

  # @conv {"SINTERSTORE" => ["FIND","INSERT"]}
  def REDIS_SINTERSTORE(args)
    dstkey = args.shift
    members = REDIS_SINTER(args)
    members.each do |member|
      pushSet(dstkey, member)
    end
  end

  # @conv {"SMEMBERS" => ["FIND"]}
  def REDIS_SMEMBERS(args)
    result = []
    getSet(args[0]).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"SDIFF" => ["FIND"]}
  def REDIS_SDIFF(args)
    target_key = args.shift
    target_values = getSet(target_key)
    data_list = []
    args.each do |key|
      data_list.push(getSet(key))
    end
    data_list.each do |d|
      target_values -= d
    end
    results = []
    target_values.uniq.each do |val|
      results.push(val["value"])
    end
    results
  end

  # @conv {"SDIFFSTORE" => ["FIND","INSERT"]}
  def REDIS_SDIFFSTORE(args)
    dstkey = args.shift
    values = REDIS_SDIFF(args)
    values.each do |value|
      pushSet(dstkey, value)
    end
    true
  end

  # @conv {"SRANDMEMBER" => ["FIND"]}
  def REDIS_SRANDMEMBER(args)
    getSet(args[0]).sample
  end

  # @conv {"SUNION" => ["FIND"]}
  def REDIS_SUNION(args)
    value = []
    args.each do |key|
      members = getSet(key)
      value += members
    end
    results = []
    value.uniq.each do |val|
      results.push(val["value"])
    end
    results
  end

  # @conv {"SUNION" => ["FIND","INSERT"]}
  def REDIS_SUNIONSTORE(args)
    dstkey = args.shift
    values = REDIS_SUNION(args)
    values.each do |value|
      pushSet(dstkey, value)
    end
    true
  end

  ## Tools for SET Structure.
  def pushSet(key, value)
    doc = { "value" => value }
    INSERT([[key, doc]])
  end

  def getSet(key)
    FIND("key" => key, "filter" => {})
  end

  def delSet(key, value)
    args = {
      "key" => key,
      "filter" => { "value" => value },
    }
    DELETE(args)
  end

  #################
  ## Sorted Sets ##
  #################
  # Data Structure [Sorted Sets] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"value" => value, "score" => score}
  # @conv {"ZADD" => ["INSERT"]}
  ## args = [key, score, value]
  def REDIS_ZADD(args)
    pushSortedSet(args[0], args[1].to_i, args[2])
  end

  # @conv {"ZREM" => ["DELETE"]}
  ## args = [key, value]
  def REDIS_ZREM(args)
    delSet(args[0], args[1])
  end

  # @conv {"ZINCRBY" => ["UPDATE","INSERT"]}
  ## args = [key, inc, value]
  def REDIS_ZINCRBY(args__)
    args = {
      "key" => args__[0],
      "query" => {},
      "update" => {},
      "multi" => true,
    }
    args["query"] = { "value" => args__[2] }
    args["update"] = { "$inc" => { "score" => args__[1].to_i } }
    unless UPDATE(args)
      return REDIS_ZADD(args__)
    end
    true
  end

  # @conv {"ZRANK" => ["FIND","COUNT"]}
  ## args = [key, value]
  def REDIS_ZRANK(args, comparison = "$lte")
    # Get Value Score
    score = getScoreByValue(args[0], args[1])
    if score
      # Get Order ()
      hash = {
        "key" => args[0],
        "filter" => [],
      }
      hash["filter"] = { "score" => { comparison.to_s => score.to_i } }
      return COUNT(hash)
    end
    args[1]
  end

  # @conv {"ZREVRANK" => ["FIND","COUNT"]}
  ## args = [key, value]
  def REDIS_ZREVRANK(args)
    REDIS_ZRANK(args, "glte")
  end

  # @conv {"ZRANGE" => ["AGGERGATE"]}
  ## args = [key, start, end]
  def REDIS_ZRANGE(args, order = 1)
    hash = {
      "key" => args[0],
      "sortName" => "score",
      "sortOrder" => order,
      "limit" => args[2].to_i,
    }
    values = AGGREGATE(hash)
    @logger.debug(values)
    i = args[1].to_i
    results = []
    while i <= args[2].to_i && values[i]
      results.push(values[i]["value"])
      i += 1
    end
    results
  end

  # @conv {"ZREVRANGE" => ["AGGERGATE"]}
  ## args = [key, start, end]
  def REDIS_ZREVRANGE(args)
    REDIS_ZRANGE(args, -1)
  end

  # @conv {"ZRANGEBYSCORE" => ["FIND"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
      "sort" => nil,
    }
    hash["filter"] = { "score" => { "$gte" => args[1].to_i,
                                    "$lte" => args[2].to_i } }
    hash["filter"] = { "score" => 1 }
    FIND(hash).to_json
  end

  # @conv {"ZCOUNT" => ["COUNT"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
    }
    hash["filter"] = { "score" => { "$gte" => args[1].to_i,
                                    "$lte" => args[2].to_i } }
    COUNT(hash)
  end

  # @conv {"ZCARD" => ["COUNT"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
    }
    COUNT(hash)
  end

  # @conv {"ZSCORE" => ["FIND"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    hash = {
      "key" => args[0],
      "filter" => { "value" => args[1] },
    }
    FIND(hash).to_json
  end

  # @conv {"ZREMRANGEBYRANK" => ["AGGREGATE","DELETE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYRANK(args)
    ## GET TARGET VALUES
    values = REDIS_ZRANGE(args, 1)
    unless values.empty?
      hash = {
        "key" => args[0],
        "filter" => { "$or" => [] },
      }
      values.each do |val|
        hash["filter"]["$or"].push("value" => val)
      end
    end
    DELETE(hash)
  end

  # @conv {"ZREMRANGEBYSCORE" => ["DELETE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYSCORE(args)
    # DELETE VALEUS
    hash = build_ltrimtype_args(args)
    DELETE(hash)
  end

  # @conv {"ZUNIONSTORE" => ["FIND","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZUNIONSTORE(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index do |index|
      docs[index.to_s] = getSet(args["args"][index])
    end
    args["args"].each_index do |index|
      docs[index.to_s].each do |doc|
        unless data[doc["value"].to_s]
          data[doc["value"].to_s] = []
        end
        weight = get_weight(args["options"], index)
        data[doc["value"].to_s].push(doc["score"].to_i * weight)
      end
    end
    ## CREATE DOC
    aggregate = "SUM"
    if args["options"] && args["options"][:aggregate]
      aggregate = args["options"][:aggregate].upcase
    end
    docs = createDocsWithAggregate(args["key"], data, aggregate)
    INSERT(docs)
  end

  # @conv {"ZINTERSTORE" => ["FIND","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZINTERSTORE(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index do |index|
      docs[index.to_s] = getSet(args["args"][index])
    end
    args["args"].each_index do |index|
      docs[index.to_s].each do |doc|
        if !data[doc["value"].to_s] && index.zero?
          data[doc["value"].to_s] = []
        end
        if data[doc["value"].to_s]
          weight = get_weight(args["options"], index)
          data[doc["value"].to_s].push(doc["score"].to_i * weight)
        end
      end
    end
    ## CREATE DOC
    aggregate = "SUM"
    if args["options"] && args["options"][:aggregate]
      aggregate = args["options"][:aggregate].upcase
    end
    docs = createDocsWithAggregate(args["key"], data, aggregate)
    INSERT(docs)
  end
  
  ## Tools for Sorted Set Structure.
  def pushSortedSet(key, score, value)
    doc = {
      "score" => score,
      "value" => value,
    }
    INSERT([[key, doc]])
  end

  def getScoreByValue(key, value)
    hash = {}
    hash["key"] = key
    hash["filter"] = { "value" => value }
    score = FIND(hash)
    if score && !score.empty?
      return score[0]["score"]
    end
    nil
  end

  def createDocsWithAggregate(dstkey, data, aggregate)
    docs = []
    operand = aggregate.upcase
    if %w[SUM MAX MIN].include?(operand)
      data.each_key do |key|
        doc = { "value" => key }
        doc["score"] = case operand
                       when "SUM"
                         data[key].inject(:+)
                       when "MAX"
                         data[key].max
                       when "MIN"
                         data[key].min
                       end
        docs.push([dstkey, doc])
      end
    else
      @logger.error("Unsupported Aggregating Operation #{aggregate}")
    end
    docs
  end

  ############
  ## Hashes ##
  ############
  # Data Structure [Hashed] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"field" => field, "value" => value}
  # @conv {"HSET" => ["INSERT"]}
  ## args = [key, field, value]
  def REDIS_HSET(args)
    value = change_numeric_when_numeric(args[2])
    doc = { "redisKey" => args[0], "field" => args[1], "value" => value }
    INSERT([["test", doc]])
  end

  # @conv {"HGET" => ["FIND"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0], "field" => args[1] }
    docs = FIND(cond)
    docs.to_json
  end

  # @conv {"HMGET" => ["FIND"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def REDIS_HMGET(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "$or" => [], "redisKey" => args["key"] }
    args["args"].each do |arg|
      cond["filter"]["$or"].push("field" => arg)
    end
    FIND(cond).to_json
  end

  # @conv {"HMSET" => ["INSERT"]}
  ## args = {"key" => "__key__", "args"=> {field0=>member0,field1=>member1,...}}
  def REDIS_HMSET(args)
    docs = []
    json = {}
    args["args"].each do |field, value_|
      value = change_numeric_when_numeric(value_)
      json[field] = value
    end
    json["redisKey"] = args["key"]
    docs.push(["test", json])
    INSERT(docs)
  end

  # @conv {"HINCRBY" => ["UPDATE"]}
  ## args = [key, field, integer]
  def REDIS_HINCRBY(args__)
    args = {
      "key" => "test",
      "query" => {},
      "update" => {},
      "multi"  => true,
    }
    args["query"]  = { "redisKey" => args__[0], "field" => args__[1] }
    args["update"] = { "$inc" => { "value" => args__[2].to_i } }
    unless UPDATE(args)
      return REDIS_HSET(args__)
    end
    true
  end

  # @conv {"HEXISTS" => ["FIND"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0], "field" => args[1] }
    docs = FIND(cond)
    !docs.empty?
  end

  # @conv {"HDEL" => ["DELETE"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0], "field" => args[1] },
    }
    DELETE(cond)
  end

  # @conv {"HLEN" => ["COUNT"]}
  ## args = [key]
  def REDIS_HLEN(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
    }
    COUNT(cond)
  end

  # @conv {"HKEYS" => ["FIND"]}
  ## args = [key]
  def REDIS_HKEYS(args)
    cond = {
      "key" => "test",
      "filter" => nil,
      "projection" => { "redisKey" => args[0], "field" => 1 },
    }
    results = []
    FIND(cond).each do |doc|
      results.push(doc["field"])
    end
    results
  end

  # @conv {"HVALS" => ["FIND"]}
  ## args = [key]
  def REDIS_HVALS(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
      "projection" => { "value" => 1 },
    }
    results = []
    FIND(cond).each do |doc|
      results.push(doc["value"])
    end
    results
  end

  # @conv {"HGETALL" => ["FIND"]}
  def REDIS_HGETALL(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
    }
    v = FIND(cond)
    v.to_json
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["FLUSH"]}
  def REDIS_FLUSHALL(args)
    unless args.empty?
      @logger.warn("All data is flushed")
    end
    DROP(["testdb"])
  end

  #############
  ## PREPARE ##
  #############
  def prepare_redis(operand, args)
    result = {}
    result["operand"] = "REDIS_#{operand}"
    result["args"] = if %w[ZUNIONSTORE ZINTERSTORE].include?(operand)
                       @parser.extractZ_X_STORE_ARGS(args)
                     elsif %w[MSET MGET MSETNX].include?(operand)
                       @parser.args2hash(args)
                     elsif %w[HMGET].include?(operand)
                       @parser.args2key_args(args)
                     elsif %w[HMSET].include?(operand)
                       @parser.args2key_hash(args)
                     else
                       args
                     end
    result
  end

  ## Private Function
  def sortedArrayGetRange(start_index, end_index, members)
    result = []
    i = start_index
    if i == -1
      i = 0
    end
    if end_index == -1
      end_index = members.size - 1
    end
    while i <= end_index
      result.push(members[i])
      i += 1
    end
    result
  end

  def aggregateScore(operation, v0, v1, weight)
    score = 0
    if operation
      case operation.upcase
      when "SUM" then
        score = v0 + v1 * weight
      when "MAX" then
        score = [v0, v1 * weight].max
      when "MIN" then
        score = [v0, v1 * weight].min
      else
        @logger.error("Unsupported aggregation #{operation} @AGGREGATION_REDIS")
      end
    else
      score = v0 + v1 * weight
    end
    score
  end

  def build_ltrimtype_args(args_)
    args = {
      "key" => args_[0],
      "filter" => { "index" => { "$gte" => args_[1].to_i,
                                 "$lte" => args_[2].to_i } },
    }
    args
  end

  def get_weight(hash, index)
    weight = 1
    if hash && hash[:weights] && hash[:weights][index]
      weight = hash[:weights][index].to_i
    end
    return weight
  end
end
