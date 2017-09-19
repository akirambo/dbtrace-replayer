
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
  def redis_set(args)
    write_string(args)
  end

  # @conv {"GET" => ["find"]}
  def redis_get(args)
    get_string(args[0])
  end

  # @conv {"SETNX" => ["INSERT"]}
  def redis_setnx(args)
    redis_set(args)
  end

  # @conv {"SETEX" => ["INSERT"]}
  def redis_setex(args)
    redis_set(args)
  end

  # @conv {"PSETEX" => ["INSERT"]}
  def redis_psetex(args)
    redis_set(args)
  end

  # @conv {"MSET" => ["INSERT"]}
  def redis_mset(args)
    args.each do |key, value|
      flag = write_string([key, value])
      unless flag
        return false
      end
    end
    true
  end

  # @conv {"MSETNX" => ["INSERT"]}
  def redis_msetnx(args)
    redis_mset(args)
  end

  # @conv {"MGET" => ["find"]}
  def redis_mget(args)
    result = get_strings(args)
    result
  end

  # @conv {"INCR" => ["find","INSERT"]}
  def redis_incr(args)
    redis_incr_decr(args, "incr")
  end

  # @conv {"INCRBY" => ["find","INSERT"]}
  def redis_incrby(args)
    redis_incr_decr(args, "incrby")
  end

  # @conv {"DECR" => ["find","INSERT"]}
  def redis_decr(args)
    redis_incr_decr(args, "decr")
  end

  # @conv {"DECRBY" => ["find","INSERT"]}
  def redis_decrby(args)
    redis_incr_decr(args, "decrby")
  end

  def redis_incr_decr(args, type)
    str = get_string(args[0])
    value = case type
            when "incr"
              str.to_i + 1
            when "incrby"
              str.to_i + args[1].to_i
            when "decr"
              str.to_i - 1
            when "decrby"
              str.to_i - args[1].to_i
            end
    update_string(args[0], value)
  end

  # @conv {"APPEND" =>  ["find","INSERT"]}
  def redis_append(args)
    str = get_string(args[0])
    value = str + args[1]
    update_string(args[0], value)
  end

  # @conv {"GETSET" =>  ["find","INSERT"]}
  def redis_getset(args)
    str = get_string(args[0])
    update_string(args[0], args[1])
    str
  end

  # @conv {"STRLEN" => ["find","LENGTH@client"]}
  def redis_strlen(args)
    get_string(args[0]).size
  end

  # @conv {"delete" => ["delete"]}
  def redis_del(args__)
    args = {
      "key" => "testdb.col",
      "filter" => { "_id" => args__[0] },
    }
    delete(args)
  end

  def get_strings(keys)
    hash = {}
    hash["key"] = "testdb.col"
    hash["filter"] = { "_id" => { "$in" => keys } }
    values = find(hash)
    if !values || values.empty?
      return []
    end
    result = []
    values.each do |kv|
      result.push(kv["value"])
    end
    result
  end

  def get_string(key)
    hash = {
      "key" => "testdb.col",
      "filter" => { "_id" => key },
    }
    values = find(hash)
    if !values || values.empty?
      return ""
    end
    values[0]["value"]
  end

  # collectionName:: test, doc : "_id" => args[0], "value" => args[1]
  def write_string(args)
    insert([["testdb.col", { "_id" => args[0], "value" => args[1] }]])
  end

  def update_string(key, value)
    args = {
      "key" => "testdb.col",
      "update" => nil,
      "multi" => false,
      "query" => { "_id" => key },
    }
    args["update"] = { "value" => value }
    update(args)
  end
  ###########
  ## Lists ##
  ###########
  # Data Structure [List] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"key" => "list", "index" => num, "value" => value}

  # @conv {"LPUSH" => ["aggregate","INSERT","update"]}
  def redis_lpush(args)
    update_index(args[0], "lpush")
    push_list(args[0], args[1], "lpush")
  end

  # @conv {"RPUSH" => ["aggregate","INSERT"]}
  def redis_rpush(args)
    push_list(args[0], args[1], "rpush")
  end

  # @conv {"llen" => ["count"]}
  def redis_llen(args)
    count("key" => args[0])
  end

  # @conv {"LRANGE" => ["find"]}
  def redis_lrange(args_)
    args = build_ltrimtype_args(args_)
    result = []
    find(args).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"LTRIM" => ["delete","update"]}
  def redis_ltrim(args_)
    args = build_ltrimtype_args(args_)
    v = delete(args)
    opt = { "start" => args_[1].to_i,
            "end" => args_[2].to_i }
    update_index(args["key"], "ltrim", opt)
    v
  end

  # @conv {"LINDEX" => ["find"]}
  def redis_lindex(args_)
    args = {
      "key" => args_[0],
      "filter" => { "index" => { "$eq" => args_[1].to_i } },
    }
    result = []
    find(args).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"LSET" => ["update"]}
  def redis_lset(args)
    update_index(args[0], "lset", args[1])
    push_list(args[0], args[2], "lset", args[1])
  end

  # @conv {"LREM" => ["delete","update"]}
  def redis_lrem(args)
    ## get list (value == args[2])
    hash = {
      "key" => args[0],
      "filter" => { "value" => args[2] },
    }
    result = []
    str = find(hash)
    str.each do |doc|
      if result.size < args[1].to_i
        result.push(doc["index"])
      end
    end
    result.each do |index|
      ## delete element
      del_list_with_index(args[0], index)
      ## update
      update_index(args[0], "lrem", index)
    end
    true
  end

  # @conv {"LPOP" => ["find","delete","update"]}
  def redis_lpop(args)
    v = get_list_with_index(args[0], 0)
    ## delete element
    del_list_with_index(args[0], 0)
    ## update
    update_index(args[0], "lpop", 0)
    v
  end

  # @conv {"RPOP" => ["delete","update"]}
  def redis_rpop(args)
    index = get_new_index(args[0], "max") - 1
    data = get_list_with_index(args[0], index)
    ## delete element
    del_list_with_index(args[0], index)
    data
  end

  # @conv {"RPOPLPUSH" => ["delete","update","INSERT","AGGEREGATE"]}
  def redis_rpoplpush(args)
    data = redis_rpop([args[0]])
    redis_lpush([args[1], data])
    data
  end

  ## Tools for LIST Structure.
  def push_list(key, value, opt, index = 0)
    case opt
    when "rpush" then
      index = get_new_index(key, "max")
    when "lpush" then
      index = get_new_index(key, "min")
    end
    # list = get_list(key)
    doc = { "key" => "list", "value" => value, "index" => index.to_i }
    insert([[key, doc]])
  end

  def get_list(key)
    hash = {}
    hash["key"] = key
    hash["filter"] = {}
    list = find(hash)
    list
  end

  def get_list_with_index(key, index)
    hash = {}
    hash["key"] = key
    hash["filter"] = { "index" => { "$eq" => index } }
    list = find(hash)
    if list && list.size == 1
      return list[0]["value"]
    end
    ""
  end

  def del_list_with_index(key, index)
    args = {
      "key" => key,
      "filter" => { "index" => index },
    }
    delete(args)
  end

  ## opt == max or min
  def get_new_index(key, opt)
    args = {
      "key" => key,
      "match" => {},
      "accumulator_id" => "key",
      "accumulator_colname" => opt,
      "accumulator_target" => "$index",
      "accumulator" => "$#{opt}",
    }
    val = aggregate(args)
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
  def update_index(key, type, opt = nil)
    args = {
      "key" => key, "query" => {},
      "update" => {}, "multi" => true
    }
    flag = true
    if type == "lpush"
      args["update"] = { "$inc" => { "index" => 1 } }
    elsif type == "ltrim"
      ## opt["end"]
      args["query"] = { "index" => { "$gt" => opt["end"].to_i } }
      decr = (opt["end"].to_i - opt["start"].to_i + 1) * -1
      args["update"] = { "$inc" => { "index" => decr } }
    elsif %w[lrem lpop lset].include?(type)
      hash = update_lset_lrem_lpop(opt, type)
      args["query"] = hash["query"]
      args["update"] = hash["udpate"]
    else
      flag = false
      @logger.error(" #{type}")
    end
    if flag
      return update(args)
    end
    flag
  end

  def update_lset_lrem_lpop(opt, type)
    index = 1
    if type != "lset"
      index = -1
    end
    hash = {}
    hash["query"] = { "index" => { "$gte" => opt.to_i } }
    hash["update"] = { "$inc" => { "index" => index } }
    hash
  end

  #########
  ## Set ##
  #########
  # Data Structure [Set] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"value" => value}
  # @conv {"SADD" => ["INSERT"]}
  def redis_sadd(args, value = nil)
    r = false
    unless value
      r = push_set(args[0], args[1])
    end
    r
  end

  # @conv {"SREM" => ["delete"]}
  def redis_srem(args)
    del_set(args[0], args[1])
  end

  # @conv {"SISMEMBER" => ["COUNT"]}
  def redis_sismember(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1] },
    }
    count(args) > 0
  end

  # @conv {"spop" => ["find","delete"]}
  def redis_spop(args)
    set = get_set(args[0])
    value = set.sample["value"]
    del_set(args[0], value)
    value
  end

  # @conv {"SMOVE" => ["delete","INSERT"]}
  def redis_smove(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    del_set(srckey, member)
    ## ADD member to dstKey
    push_set(dstkey, member)
  end

  # @conv {"REDIS_SADD" => ["COUNT"]}
  def redis_scard(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1] },
    }
    count(args)
  end

  # @conv {"SINTER" => ["find"]}
  def redis_sinter(args)
    gotten_set = {}
    args.each do |key|
      gotten_set[key] = get_set(key)
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

  # @conv {"SINTERSTORE" => ["find","INSERT"]}
  def redis_sinterstore(args)
    dstkey = args.shift
    members = redis_sinter(args)
    members.each do |member|
      push_set(dstkey, member)
    end
  end

  # @conv {"SMEMBERS" => ["find"]}
  def redis_smembers(args)
    result = []
    get_set(args[0]).each do |val|
      result.push(val["value"])
    end
    result
  end

  # @conv {"SDIFF" => ["find"]}
  def redis_sdiff(args)
    target_key = args.shift
    target_values = get_set(target_key)
    data_list = []
    args.each do |key|
      data_list.push(get_set(key))
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

  # @conv {"SDIFFSTORE" => ["find","INSERT"]}
  def redis_sdiffstore(args)
    dstkey = args.shift
    values = redis_sdiff(args)
    values.each do |value|
      push_set(dstkey, value)
    end
    true
  end

  # @conv {"SRANDMEMBER" => ["find"]}
  def redis_srandmember(args)
    get_set(args[0]).sample
  end

  # @conv {"SUNION" => ["find"]}
  def redis_sunion(args)
    value = []
    args.each do |key|
      members = get_set(key)
      value += members
    end
    results = []
    value.uniq.each do |val|
      results.push(val["value"])
    end
    results
  end

  # @conv {"SUNION" => ["find","INSERT"]}
  def redis_sunionstore(args)
    dstkey = args.shift
    values = redis_sunion(args)
    values.each do |value|
      push_set(dstkey, value)
    end
    true
  end

  ## Tools for SET Structure.
  def push_set(key, value)
    doc = { "value" => value }
    insert([[key, doc]])
  end

  def get_set(key)
    find("key" => key, "filter" => {})
  end

  def del_set(key, value)
    args = {
      "key" => key,
      "filter" => { "value" => value },
    }
    delete(args)
  end

  #################
  ## Sorted Sets ##
  #################
  # Data Structure [Sorted Sets] @ Mongodb
  #  Collection :: key
  #  Doc        :: {"value" => value, "score" => score}
  # @conv {"ZADD" => ["INSERT"]}
  ## args = [key, score, value]
  def redis_zadd(args)
    push_sorted_set(args[0], args[1].to_i, args[2])
  end

  # @conv {"ZREM" => ["delete"]}
  ## args = [key, value]
  def redis_zrem(args)
    del_set(args[0], args[1])
  end

  # @conv {"ZINCRBY" => ["update","INSERT"]}
  ## args = [key, inc, value]
  def redis_zincrby(args__)
    args = {
      "key" => args__[0],
      "query" => {},
      "update" => {},
      "multi" => true,
    }
    args["query"] = { "value" => args__[2] }
    args["update"] = { "$inc" => { "score" => args__[1].to_i } }
    unless update(args)
      return redis_zadd(args__)
    end
    true
  end

  # @conv {"ZRANK" => ["find","COUNT"]}
  ## args = [key, value]
  def redis_zrank(args, comparison = "$lte")
    # Get Value Score
    score = get_score_by_value(args[0], args[1])
    if score
      # Get Order ()
      hash = {
        "key" => args[0],
        "filter" => [],
      }
      hash["filter"] = { "score" => { comparison.to_s => score.to_i } }
      return count(hash)
    end
    args[1]
  end

  # @conv {"ZREVRANK" => ["find","COUNT"]}
  ## args = [key, value]
  def redis_zrevrank(args)
    redis_zrank(args, "glte")
  end

  # @conv {"ZRANGE" => ["AGGERGATE"]}
  ## args = [key, start, end]
  def redis_zrange(args, order = 1)
    hash = {
      "key" => args[0],
      "sortName" => "score",
      "sortOrder" => order,
      "limit" => args[2].to_i,
    }
    values = aggregate(hash)
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
  def redis_zrevrange(args)
    redis_zrange(args, -1)
  end

  # @conv {"ZRANGEBYSCORE" => ["find"]}
  ## args = [key, min, max]
  def redis_zrangebyscore(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
      "sort" => nil,
    }
    hash["filter"] = { "score" => { "$gte" => args[1].to_i,
                                    "$lte" => args[2].to_i } }
    hash["filter"] = { "score" => 1 }
    find(hash).to_json
  end

  # @conv {"ZCOUNT" => ["COUNT"]}
  ## args = [key, min, max]
  def redis_zcount(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
    }
    hash["filter"] = { "score" => { "$gte" => args[1].to_i,
                                    "$lte" => args[2].to_i } }
    count(hash)
  end

  # @conv {"ZCARD" => ["COUNT"]}
  ## args = [key]
  def redis_zcard(args)
    hash = {
      "key" => args[0],
      "filter" => nil,
    }
    count(hash)
  end

  # @conv {"ZSCORE" => ["find"]}
  ## args = [key,member]
  def redis_zscore(args)
    hash = {
      "key" => args[0],
      "filter" => { "value" => args[1] },
    }
    find(hash).to_json
  end

  # @conv {"ZREMRANGEBYRANK" => ["aggregate","delete"]}
  ## args = [key,min,max]
  def redis_zremrangebyrank(args)
    ## GET TARGET VALUES
    values = redis_zrange(args, 1)
    unless values.empty?
      hash = {
        "key" => args[0],
        "filter" => { "$or" => [] },
      }
      values.each do |val|
        hash["filter"]["$or"].push("value" => val)
      end
    end
    delete(hash)
  end

  # @conv {"ZREMRANGEBYSCORE" => ["delete"]}
  ## args = [key,min,max]
  def redis_zremrangebyscore(args)
    # delete VALEUS
    hash = build_ltrimtype_args(args)
    delete(hash)
  end

  # @conv {"ZUNIONSTORE" => ["find","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "option" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def redis_zunionstore(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index do |index|
      docs[index.to_s] = get_set(args["args"][index])
    end
    args["args"].each_index do |index|
      docs[index.to_s].each do |doc|
        unless data[doc["value"].to_s]
          data[doc["value"].to_s] = []
        end
        weight = get_weight(args["option"], index)
        data[doc["value"].to_s].push(doc["score"].to_i * weight)
      end
    end
    ## CREATE DOC
    aggregate = "SUM"
    if args["option"] && args["option"][:aggregate]
      aggregate = args["option"][:aggregate].upcase
    end
    docs = create_docs_with_aggregate(args["key"], data, aggregate)
    insert(docs)
  end

  # @conv {"ZINTERSTORE" => ["find","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "option" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def redis_zinterstore(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index do |index|
      docs[index.to_s] = get_set(args["args"][index])
    end
    args["args"].each_index do |index|
      docs[index.to_s].each do |doc|
        if !data[doc["value"].to_s] && index.zero?
          data[doc["value"].to_s] = []
        end
        if data[doc["value"].to_s]
          weight = get_weight(args["option"], index)
          data[doc["value"].to_s].push(doc["score"].to_i * weight)
        end
      end
    end
    ## CREATE DOC
    aggregate = "SUM"
    if args["option"] && args["option"][:aggregate]
      aggregate = args["option"][:aggregate].upcase
    end
    docs = create_docs_with_aggregate(args["key"], data, aggregate)
    insert(docs)
  end

  ## Tools for Sorted Set Structure.
  def push_sorted_set(key, score, value)
    doc = {
      "score" => score,
      "value" => value,
    }
    insert([[key, doc]])
  end

  def get_score_by_value(key, value)
    hash = {}
    hash["key"] = key
    hash["filter"] = { "value" => value }
    score = find(hash)
    if score && !score.empty?
      return score[0]["score"]
    end
    nil
  end

  def create_docs_with_aggregate(dstkey, data, aggregate)
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
  def redis_hset(args)
    value = change_numeric_when_numeric(args[2])
    doc = { "redisKey" => args[0], "field" => args[1], "value" => value }
    insert([["test", doc]])
  end

  # @conv {"HGET" => ["find"]}
  ## args = [key, field]
  def redis_hget(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0], "field" => args[1] }
    docs = find(cond)
    docs.to_json
  end

  # @conv {"HMGET" => ["find"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def redis_hmget(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "$or" => [], "redisKey" => args["key"] }
    args["args"].each do |arg|
      cond["filter"]["$or"].push("field" => arg)
    end
    find(cond).to_json
  end

  # @conv {"HMSET" => ["INSERT"]}
  ## args = {"key" => "__key__", "args"=> {field0=>member0,field1=>member1,...}}
  def redis_hmset(args)
    docs = []
    json = {}
    args["args"].each do |field, value_|
      value = change_numeric_when_numeric(value_)
      json[field] = value
    end
    json["redisKey"] = args["key"]
    docs.push(["test", json])
    insert(docs)
  end

  # @conv {"HINCRBY" => ["update"]}
  ## args = [key, field, integer]
  def redis_hincrby(args__)
    args = {
      "key" => "test",
      "query" => {},
      "update" => {},
      "multi"  => true,
    }
    args["query"]  = { "redisKey" => args__[0], "field" => args__[1] }
    args["update"] = { "$inc" => { "value" => args__[2].to_i } }
    unless update(args)
      return redis_hset(args__)
    end
    true
  end

  # @conv {"HEXISTS" => ["find"]}
  ## args = [key, field]
  def redis_hexists(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0], "field" => args[1] }
    docs = find(cond)
    !docs.empty?
  end

  # @conv {"HDEL" => ["delete"]}
  ## args = [key, field]
  def redis_hdel(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0], "field" => args[1] },
    }
    delete(cond)
  end

  # @conv {"HLEN" => ["COUNT"]}
  ## args = [key]
  def redis_hlen(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
    }
    count(cond)
  end

  # @conv {"HKEYS" => ["find"]}
  ## args = [key]
  def redis_hkeys(args)
    cond = {
      "key" => "test",
      "filter" => nil,
      "projection" => { "redisKey" => args[0], "field" => 1 },
    }
    results = []
    find(cond).each do |doc|
      results.push(doc["field"])
    end
    results
  end

  # @conv {"HVALS" => ["find"]}
  ## args = [key]
  def redis_hvals(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
      "projection" => { "value" => 1 },
    }
    results = []
    find(cond).each do |doc|
      results.push(doc["value"])
    end
    results
  end

  # @conv {"HGETALL" => ["find"]}
  def redis_hgetall(args)
    cond = {
      "key" => "test",
      "filter" => { "redisKey" => args[0] },
    }
    v = find(cond)
    v.to_json
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["FLUSH"]}
  def redis_flushall(args)
    unless args.empty?
      @logger.warn("All data is flushed")
    end
    drop(["testdb"])
  end

  #############
  ## PREPARE ##
  #############
  def prepare_redis(operand, args)
    result = {}
    result["operand"] = "redis_#{operand}"
    result["args"] = if %w[zunionstore zinterstore].include?(operand)
                       @parser.extract_z_x_store_args(args)
                     elsif %w[mset mget msetnx].include?(operand)
                       @parser.args2hash(args)
                     elsif %w[hmget].include?(operand)
                       @parser.args2key_args(args)
                     elsif %w[hmset].include?(operand)
                       @parser.args2key_hash(args)
                     else
                       args
                     end
    result
  end

  ## Private Function
  def sorted_array_get_range(start_index, end_index, members)
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

  def aggregate_score(operation, v0, v1, weight)
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
    weight
  end
end
