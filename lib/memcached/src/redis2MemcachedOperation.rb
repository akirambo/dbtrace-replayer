
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

module Redis2MemcachedOperation
  private

  ############
  ## String ##
  ############
  # @conv {"SET" => ["SET"]}
  def REDIS_SET(args)
    SET(args)
  end

  # @conv {"GET" => ["GET"]}
  def REDIS_GET(args)
    GET(args)
  end

  # @conv {"SETNX" => ["ADD"]}
  def REDIS_SETNX(args)
    ADD(args)
  end

  # @conv {"SETEX" => ["SET"]}
  def REDIS_SETEX(args)
    SET(args)
  end

  # @conv {"PSETEX" => ["SET"]}
  def REDIS_PSETEX(args)
    args[2] = (args[2].to_f / 1000).to_i + 1
    SET(args)
  end

  # @conv {"MSET" => ["SET"]}
  def REDIS_MSET(args)
    args.each do |key, value|
      unless SET([key, value])
        return false
      end
    end
    true
  end

  # @conv {"MGET" => ["GET"]}
  def REDIS_MGET(args)
    result = []
    args.each do |key|
      result.push(GET([key]))
    end
    result
  end

  # @conv {"MSETNX" => ["ADD"]}
  def REDIS_MSETNX(args)
    args.each do |key, value|
      unless ADD([key, value])
        return false
      end
    end
    true
  end

  # @conv {"INCR" => ["INCR"]}
  def REDIS_INCR(args)
    args[0].gsub!(":", "__colon__")
    INCR([args[0], 1])
  end

  # @conv {"INCRBY" => ["INCR"]}
  def REDIS_INCRBY(args)
    args[0].gsub!(":", "__colon__")
    INCR(args)
  end

  # @conv {"DECR" => ["DECR"]}
  def REDIS_DECR(args)
    args[0].gsub!(":", "__colon__")
    DECR([args[0], 1])
  end

  # @conv {"DECRBY" => ["DECRBY"]}
  def REDIS_DECRBY(args)
    args[0].gsub!(":", "__colon__")
    DECR([args[0], args[1]])
  end

  # @conv {"APPEND" => ["APPEND"]}
  def REDIS_APPEND(args)
    APPEND(args)
  end

  # @conv {"GETSET" => ["REPLACE"]}
  def REDIS_GETSET(args)
    old = GET(args)
    REPLACE(args)
    old
  end

  # @conv {"STRLEN" => ["GET","LENGTH@client"]}
  def REDIS_STRLEN(args)
    GET(args).size
  end

  ###########
  ## Lists ##
  ###########
  # @conv {"LPUSH" => ["GET","SET"]}
  def REDIS_LPUSH(args)
    key = args[0]
    value = args[1]
    exist_data = GET([key])
    unless exist_data.empty?
      value = value + "," + exist_data
    end
    SET([key, value])
  end

  # @conv {"RPUSH" => ["GET","SET"]}
  def REDIS_RPUSH(args)
    key = args[0]
    value = args[1]
    exist_data = GET([key])
    unless exist_data.size.zero?
      value = exist_data + "," + value
    end
    SET([key, value])
  end

  # @conv {"LPOP" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LPOP(args)
    data = GET(args)
    value = redisDeserialize(data)
    str = value.shift
    value = redisSerialize(value)
    if !value.empty?
      SET([args[0], value])
    else
      DELETE([args[0]])
    end
    str
  end

  # @conv {"RPOP" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_RPOP(args)
    data  = GET(args)
    value = redisDeserialize(data)
    data  = value.pop
    value = redisSerialize(value)
    if !value.empty?
      SET([args[0], value])
    else
      DELETE([args[0]])
    end
    data
  end

  # @conv {"LRANGE" => ["redisDeserialize@client","extractElementsFromList@client","GET"]}
  def REDIS_LRANGE(args)
    data = GET([args[0]])
    list = redisDeserialize(data)
    value = extractElementsFromList(list, args[1], args[2])
    value
  end

  # @conv {"LREM" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LREM(args)
    data = GET([args[0]])
    value = redisDeserialize(data)
    result = []
    count = args[1].to_i
    value.each do |elem|
      if elem == args[2] && !count.zero?
        # skip
        count -= 1
      else
        result.push(elem)
      end
    end
    result = redisSerialize(result)
    SET([args[0], result])
  end

  # @conv {"LINDEX" => ["redisDeserialize@client","extractElementsFromList@client","GET","SET"]}
  def REDIS_LINDEX(args)
    data = GET([args[0]])
    value = redisDeserialize(data)[args[1].to_i]
    value
  end

  # @conv {"RPOPLPUSH" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_RPOPLPUSH(args)
    data = REDIS_RPOP([args[0]])
    REDIS_LPUSH([args[1], data])
    data
  end

  # @conv {"LSET" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LSET(args)
    data = GET([args[0]])
    value = redisDeserialize(data)
    value[args[1].to_i] = args[2]
    value = redisSerialize(value)
    SET([args[0], value])
  end

  # @conv {"LTRIM" => ["redisDeserialize@client","extractElementsFromList@client","redisSerialize@client","GET","SET"]}
  def REDIS_LTRIM(args)
    data = REDIS_LRANGE(args)
    value = redisSerialize(data)
    SET([args[0], value])
  end

  # @conv {"LLEN" => ["redisDeserialize@client","GET"]}
  def REDIS_LLEN(args)
    data = GET([args[0]])
    redisDeserialize(data).size
  end

  #########
  ## Set ##
  #########
  # @conv {"SRANDMEMBER" => ["redisDeserialize@client","GET"]}
  def REDIS_SRANDMEMBER(args)
    key = args[0]
    data = GET([key])
    members = redisDeserialize(data)
    members.sample
  end

  # @conv {"SMEMBERS" => ["redisDeserialize@client","GET"]}
  def REDIS_SMEMBERS(args)
    key = args[0]
    value = GET([key])
    redisDeserialize(value)
  end

  # @conv {"SDIFF" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SDIFF(args)
    common = []
    members_array = []
    data_array = []
    args.each do |key|
      data_array.push(GET([key]))
    end
    data_array.each do |data|
      members = redisDeserialize(data)
      members_array += members
      common = if common.empty?
                 members
               else
                 common & members
               end
    end
    value = members_array.uniq - common.uniq
    redisSerialize(value)
  end

  # @conv {"SDIFFSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SDIFFSTORE(args)
    dstkey = args.shift
    value = REDIS_SDIFF(args)
    SET([dstkey, value])
  end

  # @conv {"SINTER" => ["redisDeserialize@client","GET"]}
  def REDIS_SINTER(args)
    result = []
    data_array = []
    args.each do |key|
      data_array.push(GET([key]))
    end
    data_array.each do |data|
      members = redisDeserialize(data)
      result = if result.empty?
                 members
               else
                 result & members
               end
    end
    redisSerialize(result)
  end

  # @conv {"SINTERSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SINTERSTORE(args)
    dstkey = args.shift
    result = REDIS_SINTER(args)
    SET([dstkey, result])
  end

  # @conv {"SUNION" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SUNION(args)
    value = []
    args.each do |key|
      members = redisDeserialize(GET([key]))
      value += members
    end
    redisSerialize(value.uniq)
  end

  # @conv {"SUNION" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SUNIONSTORE(args)
    dstkey = args.shift
    result = REDIS_SUNION(args)
    SET([dstkey, result])
  end

  # @conv {"SISMEMBER" => ["redisDeserialize@client","GET"]}
  def REDIS_SISMEMBER(args)
    target = args.pop
    value = REDIS_SMEMBERS(args)
    value.include?(target)
  end

  # @conv {"SREM" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SREM(args)
    key = args[0]
    value = args[1]
    data = GET([key])
    members = redisDeserialize(data)
    members.delete(value)
    unless members.empty?
      result = redisSerialize(members)
      return REPLACE([key, result])
    end
    DELETE([key])
  end

  # @conv {"SMOVE" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SMOVE(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    srcdata = GET([srckey])
    dstdata = GET([dstkey])
    ## REMOVE member from srtKey
    members = redisDeserialize(srcdata)
    members.delete(member)
    srcresult = redisSerialize(members)
    ## ADD member to dstKey
    members = redisDeserialize(dstdata)
    members.push(member)
    dstresult = redisSerialize(members)
    (REPLACE([srckey, srcresult]) && REPLACE([dstkey, dstresult]))
  end

  # @conv {"SCARD" => ["GET","redisDeserialize@client"]}
  def REDIS_SCARD(args)
    members = GET([args[0]])
    redisDeserialize(members).size
  end

  # @conv {"SADD" => ["redisSerialize@client","SET","REPLACE"]}
  def REDIS_SADD(args)
    key = args.shift
    existing_data = GET([key])
    value = redisSerialize(args)
    if !existing_data.empty?
      return REPLACE([key, "#{existing_data},#{value}"])
    else
      return SET([key, value])
    end
  end

  # @conv {"SPOP" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SPOP(args)
    key = args[0]
    data = GET([key])
    ## GET
    members = redisDeserialize(data)
    popedmember = members.sample
    members.delete(popedmember)
    result = redisSerialize(members)
    if !result.empty?
      REPLACE([key, result])
    else
      DELETE([key])
    end
    popedmember
  end

  #################
  ## Sorted Sets ##
  #################
  # @conv {"ZADD" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore@client","GET","SET"]}
  ## args = [key, score, member]
  def REDIS_ZADD(args)
    ## hash { member => score}
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash[args[2]] = args[1].to_i
    result = redisSerializeWithScore(hash)
    SET([args[0], result])
  end

  # @conv {"ZREM" => ["redisDeserializeWithScore@client","redisSerializeWithScore@client","GET","SET"]}
  ## args = [key, member]
  def REDIS_ZREM(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash.delete(args[1])
    result = redisSerializeWithScore(hash, false)
    unless result.empty?
      return SET([args[0], result])
    end
    DELETE([args[0]])
  end

  # @conv {"ZINCRBY" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore@client","GET","SET"]}
  ## args = [key, score, member]
  def REDIS_ZINCRBY(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash[args[2]] += args[1].to_i
    result = redisSerializeWithScore(hash, true)
    SET([args[0], result])
  end

  # @conv {"ZRANK" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, member]
  def REDIS_ZRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash.keys.index(args[1])
  end

  # @conv {"ZREVRANK" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, member]
  def REDIS_ZREVRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash.keys.reverse.index(args[1])
  end

  # @conv {"ZRANGE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, start, end]
  def REDIS_ZRANGE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    sortedArrayGetRange(args[1].to_i, args[2].to_i, hash.keys)
  end

  # @conv {"ZREVRANGE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, start, end]
  def REDIS_ZREVRANGE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    sortedArrayGetRange(args[1].to_i, args[2].to_i, hash.keys.reverse)
  end

  # @conv {"ZRANGEBYSCORE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    selected = hash.select { |_, score| score >= args[1].to_i && score <= args[2].to_i }
    selected.keys
  end

  # @conv {"ZCOUNT" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    selected = REDIS_ZRANGEBYSCORE(args)
    selected.size
  end

  # @conv {"ZCARD" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    data = GET(args)
    hash = redisDeserializeWithScore(data)
    hash.keys.size
  end

  # @conv {"ZSCORE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash[args[1]].to_i
  end

  # @conv {"ZREMRANGEBYSCORE" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore","GET","SET"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    selected = hash.select { |_, score| score < args[1].to_i || score > args[2].to_i }
    result = redisSerializeWithScore(selected)
    SET([args[0], result])
  end

  # @conv {"ZREMRANGEBYRANK" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore","GET","SET"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    result = {}
    i = 0
    hash.each do |member, score|
      if i < args[1].to_i || i > args[2].to_i
        result[member] = score
      end
      i += 1
    end
    value = redisSerializeWithScore(result)
    SET([args[0], value])
  end

  # @conv {"ZUNIONSTORE" => ["deserizeWithScore@client","redisSerializeWithScore@client","mergeWithOption@client","GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZUNIONSTORE(args)
    result = {}
    data = {}
    args["args"].each_index do |index|
      key = args["args"][index]
      data[key] = GET([key])
    end
    args["args"].each_index do |index|
      key = args["args"][index]
      weight = 1.0
      if args["options"] &&
         args["options"][:weights] &&
         args["options"][:weights][index]
        weight = args["options"][:weights][index].to_f
      end
      if !result.keys.empty?
        result.merge!(redisDeserializeWithScore(data[key])) do |_, v0, v1|
          aggregateScore(args["options"][:aggregate], v0.to_f, v1.to_f, weight)
        end
      else
        result = redisDeserializeWithScore(data[key])
      end
    end
    value = redisSerializeWithScore(result)
    SET([args["key"], value])
  end

  # @conv {"ZINTERSTORE" => ["deserizeWithScore@client","redisSerializeWithScore@client","diffWithOption@client""GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZINTERSTORE(args)
    result = {}
    data   = {}
    args["args"].each_index do |index|
      key = args["args"][index]
      data[key] = GET([key])
    end
    args["args"].each_index do |index|
      key = args["args"][index]
      weight = 1.0
      if args["options"] &&
         args["options"][:weights] &&
         args["options"][:weights][index]
        weight = args["options"][:weights][index].to_f
      end
      if !result.keys.empty?
        new_hash = redisDeserializeWithScore(data[key])
        keys = result.keys & new_hash.keys
        tmp__ = {}
        keys.each do |key_|
          tmp__[key_] = aggregateScore(args["options"][:aggregate], result[key_].to_f, new_hash[key_].to_f, weight)
        end
      else
        result = redisDeserializeWithScore(data[key])
      end
    end
    value = redisSerializeWithScore(result)
    SET([args["key"], value])
  end

  ############
  ## Hashes ##
  ############
  # @conv {"HSET" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field, value]
  def REDIS_HSET(args)
    data = GET([args[0]])
    hash = {}
    unless data.empty?
      hash = redisDeserializeHash(data)
    end
    hash[args[1]] = args[2]
    result = redisSerializeHash(hash)
    SET([args[0], result])
  end

  # @conv {"HGET" => ["redisDeserializeHash@client","GET"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash[args[1]]
  end

  # @conv {"HMGET" => ["redisDeserializeHash@client","GET"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def REDIS_HMGET(args)
    data = GET([args["key"]])
    hash = redisDeserializeHash(data)
    result = []
    args["args"].each do |field|
      result.push(hash[field])
    end
    result
  end

  # @conv {"HMSET" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = {"key" => key, "args"=> {field0=>member0,field1=>member1,...}}
  def REDIS_HMSET(args)
    args["key"].gsub!(":", "__colon__")
    ### keyname, asyncable
    data = GET([args["key"]])
    hash = {}
    unless data.empty?
      hash = redisDeserializeHash(data)
    end
    args["args"].each do |field, member|
      hash[field] = member
    end
    result = redisSerializeHash(hash)
    SET([args["key"], result])
  end

  # @conv {"HINCRBY" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field, integer]
  def REDIS_HINCRBY(args)
    data = GET([args[0]])
    hash = {}
    unless data.empty?
      hash = redisDeserializeHash(data)
    end
    hash[args[1]] = hash[args[1]].to_i + args[2].to_i
    result = redisSerializeHash(hash)
    SET([args[0], result])
  end

  # @conv {"HEXISTS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.key?(args[1])
  end

  # @conv {"HDEL" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.delete(args[1])
    if hash.keys.empty?
      return DELETE([args[0]])
    end
    result = redisSerializeHash(hash)
    SET([args[0], result])
  end

  # @conv {"HLEN" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HLEN(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.keys.size
  end

  # @conv {"HKEYS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HKEYS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.keys
  end

  # @conv {"HVALS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HVALS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.values
  end

  # @conv {"HGETALL" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HGETALL(args)
    data = GET([args[0]])
    redisDeserializeHash(data)
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["FLUSH"]}
  def REDIS_FLUSHALL(args)
    FLUSH(args)
  end

  # @conv {"DEL" => ["DELETE"]}
  def REDIS_DEL(args)
    DELETE(args)
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
  def extractElementsFromList(list, start_pos, end_pos)
    result = []
    start_pos = start_pos.to_i
    end_pos = end_pos.to_i
    i = start_pos
    while i <= end_pos
      result.push(list[i])
      i += 1
    end
    result
  end

  def redisSerialize(array)
    array.join(",")
  end

  def redisDeserialize(str)
    str.split(",")
  end

  # hash {member => score}
  def redisSerializeWithScore(hash, sort = true)
    result = []
    ## sortByScore
    if sort
      hash.sort { |(_, v1), (_, v2)| v1 <=> v2 }
    end
    ## build
    hash.each do |member, score|
      result.push("#{score}_#{member}")
    end
    result.join(",")
  end

  def redisDeserializeWithScore(str)
    result = {}
    if !str.nil? && !str.empty?
      str_with_score = str.split(",")
      str_with_score.each do |value|
        ## val = [score, member]
        val = value.split("_")
        result[val[1]] = val[0].to_i
      end
    end
    result
  end

  def redisSerializeHash(hash)
    result = []
    ## build
    hash.each do |field, member|
      result.push("#{field}__H__#{member}")
    end
    result.join(",")
  end

  def redisDeserializeHash(str)
    result = {}
    if !str.nil? && !str.empty?
      str_with_score = str.split(",")
      str_with_score.each do |value|
        ## val = [score, member]
        val = value.split("__H__")
        result[val[0]] = val[1]
      end
    end
    result
  end

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
    score = case operation.upcase
            when "SUM" then
              v0 + v1 * weight
            when "MAX" then
              [v0, v1 * weight].max
            when "MIN" then
              [v0, v1 * weight].min
            else
              v0 + v1 * weight
            end
    score
  end
end
