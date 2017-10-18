
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
  # @conv {"set" => ["set"]}
  def redis_set(args)
    set(args)
  end

  # @conv {"get" => ["get"]}
  def redis_get(args)
    get(args)
  end

  # @conv {"setnx" => ["add"]}
  def redis_setnx(args)
    add(args)
  end

  # @conv {"setex" => ["set"]}
  def redis_setex(args)
    set(args)
  end

  # @conv {"psetex" => ["set"]}
  def redis_psetex(args)
    args[2] = (args[2].to_f / 1000).to_i + 1
    set(args)
  end

  # @conv {"mset" => ["set"]}
  def redis_mset(args)
    args.each do |key, value|
      unless set([key, value])
        return false
      end
    end
    true
  end

  # @conv {"mget" => ["get"]}
  def redis_mget(args)
    result = []
    args.each do |key|
      result.push(get([key]))
    end
    result
  end

  # @conv {"msetnx" => ["add"]}
  def redis_msetnx(args)
    args.each do |key, value|
      unless add([key, value])
        return false
      end
    end
    true
  end

  # @conv {"incr" => ["incr"]}
  def redis_incr(args)
    args[0].gsub!(":", "__colon__")
    incr([args[0], 1])
  end

  # @conv {"incrby" => ["incr"]}
  def redis_incrby(args)
    args[0].gsub!(":", "__colon__")
    incr(args)
  end

  # @conv {"decr" => ["decr"]}
  def redis_decr(args)
    args[0].gsub!(":", "__colon__")
    decr([args[0], 1])
  end

  # @conv {"decrby" => ["decrby"]}
  def redis_decrby(args)
    args[0].gsub!(":", "__colon__")
    decr([args[0], args[1]])
  end

  # @conv {"append" => ["append"]}
  def redis_append(args)
    append(args)
  end

  # @conv {"getset" => ["replace"]}
  def redis_getset(args)
    old = get(args)
    replace(args)
    old
  end

  # @conv {"strlen" => ["get","length@client"]}
  def redis_strlen(args)
    get(args).size
  end

  ###########
  ## lists ##
  ###########
  # @conv {"lpush" => ["get","set"]}
  def redis_lpush(args)
    key = args[0]
    value = args[1]
    exist_data = get([key])
    unless exist_data.empty?
      value = value + "," + exist_data
    end
    set([key, value])
  end

  # @conv {"rpush" => ["get","set"]}
  def redis_rpush(args)
    key = args[0]
    value = args[1]
    exist_data = get([key])
    unless exist_data.size.zero?
      value = exist_data + "," + value
    end
    set([key, value])
  end

  # @conv {"lpop" => ["redisdeserialize@client","redisserialize@client","get","set"]}
  def redis_lpop(args)
    redis_lrpop(args, "l")
  end

  # @conv {"rpop" => ["redisdeserialize@client","redisserialize@client","get","set"]}
  def redis_rpop(args)
    redis_lrpop(args, "r")
  end

  def redis_lrpop(args, type)
    data = get(args)
    value = redis_deserialize(data)
    data = if type == "l"
             value.shift
           elsif type == "r"
             value.pop
           end
    value = redis_serialize(value)
    if !value.empty?
      set([args[0], value])
    else
      delete([args[0]])
    end
    data
  end

  # @conv {"lrange" => ["redis_deserialize@client","extract_elements_fromlist@client","GET"]}
  def redis_lrange(args)
    data = get([args[0]])
    list = redis_deserialize(data)
    value = extract_elements_fromlist(list, args[1], args[2])
    value
  end

  # @conv {"lrem" => ["redisdeserialize@client","redis_serialize@client","GET","SET"]}
  def redis_lrem(args)
    data = get([args[0]])
    value = redis_deserialize(data)
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
    result = redis_serialize(result)
    set([args[0], result])
  end

  # @conv {"lindex" => ["redis_deserialize@client","extract_elements_fromlist@client","GET","SET"]}
  def redis_lindex(args)
    data = get([args[0]])
    value = redis_deserialize(data)[args[1].to_i]
    value
  end

  # @conv {"rpoplpush" => ["redis_deserialize@client","redis_serialize@client","GET","SET"]}
  def redis_rpoplpush(args)
    data = redis_rpop([args[0]])
    redis_lpush([args[1], data])
    data
  end

  # @conv {"lset" => ["redis_deserialize@client","redis_serialize@client","GET","SET"]}
  def redis_lset(args)
    data = get([args[0]])
    value = redis_deserialize(data)
    value[args[1].to_i] = args[2]
    value = redis_serialize(value)
    set([args[0], value])
  end

  # @conv {"ltrim" => ["redis_deserialize@client","extract_elements_fromlist@client","redis_serialize@client","GET","SET"]}
  def redis_ltrim(args)
    data = redis_lrange(args)
    value = redis_serialize(data)
    set([args[0], value])
  end

  # @conv {"llen" => ["redis_deserialize@client","GET"]}
  def redis_llen(args)
    data = get([args[0]])
    redis_deserialize(data).size
  end

  #########
  ## Set ##
  #########
  # @conv {"srandmember" => ["redisdeserialize@client","get"]}
  def redis_srandmember(args)
    key = args[0]
    data = get([key])
    members = redis_deserialize(data)
    members.sample
  end

  # @conv {"smembers" => ["redis_deserialize@client","GET"]}
  def redis_smembers(args)
    key = args[0]
    value = get([key])
    redis_deserialize(value)
  end

  # @conv {"sdiff" => ["redisdeserialize@client","redis_serialize@client","GET"]}
  def redis_sdiff(args)
    common = []
    members_array = []
    data_array = []
    args.each do |key|
      data_array.push(get([key]))
    end
    data_array.each do |data|
      members = redis_deserialize(data)
      members_array += members
      common = if common.empty?
                 members
               else
                 common & members
               end
    end
    value = members_array.uniq - common.uniq
    redis_serialize(value)
  end

  # @conv {"sdiffstore" => ["redisdeserialize@client","redisserialize@client","get","set"]}
  def redis_sdiffstore(args)
    dstkey = args.shift
    value = redis_sdiff(args)
    set([dstkey, value])
  end

  # @conv {"sinter" => ["redis_deserialize@client","GET"]}
  def redis_sinter(args)
    result = []
    data_array = []
    args.each do |key|
      data_array.push(get([key]))
    end
    data_array.each do |data|
      members = redis_deserialize(data)
      result = if result.empty?
                 members
               else
                 result & members
               end
    end
    redis_serialize(result)
  end

  # @conv {"sinterstore" => ["redis_deserialize@client","redis_serialize@client","GET","SET"]}
  def redis_sinterstore(args)
    dstkey = args.shift
    result = redis_sinter(args)
    set([dstkey, result])
  end

  # @conv {"sunion" => ["redis_deserialize@client","redis_serialize@client","GET"]}
  def redis_sunion(args)
    value = []
    args.each do |key|
      members = redis_deserialize(get([key]))
      value += members
    end
    redis_serialize(value.uniq)
  end

  # @conv {"sunion" => ["redis_deserialize@client","redis_serialize@client","GET","SET"]}
  def redis_sunionstore(args)
    dstkey = args.shift
    result = redis_sunion(args)
    set([dstkey, result])
  end

  # @conv {"sismember" => ["redis_deserialize@client","GET"]}
  def redis_sismember(args)
    target = args.pop
    value = redis_smembers(args)
    value.include?(target)
  end

  # @conv {"srem" => ["redis_deserialize@client","redis_serialize@client","GET","REPLACE"]}
  def redis_srem(args)
    key = args[0]
    value = args[1]
    data = get([key])
    members = redis_deserialize(data)
    members.delete(value)
    unless members.empty?
      result = redis_serialize(members)
      return replace([key, result])
    end
    delete([key])
  end

  # @conv {"smove" => ["redis_deserialize@client","redis_serialize@client","GET","REPLACE"]}
  def redis_smove(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    srcdata = get([srckey])
    dstdata = get([dstkey])
    ## REMOVE member from srtKey
    members = redis_deserialize(srcdata)
    members.delete(member)
    srcresult = redis_serialize(members)
    ## ADD member to dstKey
    members = redis_deserialize(dstdata)
    members.push(member)
    dstresult = redis_serialize(members)
    (replace([srckey, srcresult]) && replace([dstkey, dstresult]))
  end

  # @conv {"scard" => ["get","redis_deserialize@client"]}
  def redis_scard(args)
    members = get([args[0]])
    redis_deserialize(members).size
  end

  # @conv {"sadd" => ["redis_serialize@client","SET","REPLACE"]}
  def redis_sadd(args)
    key = args.shift
    existing_data = get([key])
    value = redis_serialize(args)
    if !existing_data.empty?
      return replace([key, "#{existing_data},#{value}"])
    else
      return set([key, value])
    end
  end

  # @conv {"spop" => ["redis_deserialize@client","redis_serialize@client","GET","REPLACE"]}
  def redis_spop(args)
    key = args[0]
    data = get([key])
    ## GET
    members = redis_deserialize(data)
    popedmember = members.sample
    members.delete(popedmember)
    result = redis_serialize(members)
    if !result.empty?
      replace([key, result])
    else
      delete([key])
    end
    popedmember
  end

  #################
  ## Sorted Sets ##
  #################
  # @conv {"zadd" => ["redis_deserialize_withscore@client","sortByScore@client","redis_serialize_withscore@client","GET","SET"]}
  ## args = [key, score, member]
  def redis_zadd(args)
    ## hash { member => score}
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    hash[args[2]] = args[1].to_i
    result = redis_serialize_withscore(hash)
    set([args[0], result])
  end

  # @conv {"zrem" => ["redis_deserialize_withscore@client","redis_serialize_withscore@client","GET","SET"]}
  ## args = [key, member]
  def redis_zrem(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    hash.delete(args[1])
    result = redis_serialize_withscore(hash, false)
    unless result.empty?
      return set([args[0], result])
    end
    delete([args[0]])
  end

  # @conv {"zincrby" => ["redis_deserialize_withscore@client","sortByScore@client","redis_serialize_withscore@client","GET","SET"]}
  ## args = [key, score, member]
  def redis_zincrby(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    if hash[args[2]].nil?
      hash[args[2]] = 0
    end
    hash[args[2]] += args[1].to_i
    result = redis_serialize_withscore(hash, true)
    set([args[0], result])
  end

  # @conv {"zrank" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, member]
  def redis_zrank(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    hash.keys.index(args[1])
  end

  # @conv {"zrevrank" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, member]
  def redis_zrevrank(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    hash.keys.reverse.index(args[1])
  end

  # @conv {"zrange" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, start, end]
  def redis_zrange(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    sorted_array_get_range(args[1].to_i, args[2].to_i, hash.keys)
  end

  # @conv {"zrevrange" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, start, end]
  def redis_zrevrange(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    sorted_array_get_range(args[1].to_i, args[2].to_i, hash.keys.reverse)
  end

  # @conv {"zrangebyscore" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, min, max]
  def redis_zrangebyscore(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    selected = hash.select { |_, score| score >= args[1].to_i && score <= args[2].to_i }
    selected.keys
  end

  # @conv {"zcount" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key, min, max]
  def redis_zcount(args)
    selected = redis_zrangebyscore(args)
    selected.size
  end

  # @conv {"zcard" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key]
  def redis_zcard(args)
    data = get(args)
    hash = redis_deserialize_withscore(data)
    hash.keys.size
  end

  # @conv {"zscore" => ["redis_deserialize_withscore@client","GET"]}
  ## args = [key,member]
  def redis_zscore(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    hash[args[1]].to_i
  end

  # @conv {"zremrangebyscore" => ["redis_deserialize_withscore@client","sortByScore@client","redis_serialize_withscore","GET","SET"]}
  ## args = [key,min,max]
  def redis_zremrangebyscore(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    selected = hash.select { |_, score| score < args[1].to_i || score > args[2].to_i }
    result = redis_serialize_withscore(selected)
    set([args[0], result])
  end

  # @conv {"zremrangebyrank" => ["redis_deserialize_withscore@client","sortByScore@client","redis_serialize_withscore","GET","SET"]}
  ## args = [key,min,max]
  def redis_zremrangebyrank(args)
    data = get([args[0]])
    hash = redis_deserialize_withscore(data)
    result = {}
    i = 0
    hash.each do |member, score|
      if i < args[1].to_i || i > args[2].to_i
        result[member] = score
      end
      i += 1
    end
    value = redis_serialize_withscore(result)
    set([args[0], value])
  end

  # @conv {"zunionstore" => ["deserizeWithScore@client","redis_serialize_withscore@client","mergeWithOption@client","GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "option" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def redis_zunionstore(args)
    result = {}
    data = redis_get_stored_data(args)
    args["args"].each_index do |index|
      key = args["args"][index]
      weight = redis_get_weight(args, index)
      if !result.keys.empty?
        tmp = redis_deserialize_withscore(data[key])
        tmp.each do |k, v|
          result[k] = if result[k].nil? || args["option"][:aggregate].nil?
                        v
                      else
                        result[k] = aggregate_score(args["option"][:aggregate],
                                                    result[k], v.to_f, weight)
                      end
        end
      else
        result = redis_deserialize_withscore(data[key])
        result.each_key do |k|
          result[k] *= weight
        end
      end
    end
    value = redis_serialize_withscore(result)
    set([args["key"], value])
  end

  # @conv {"zinterstore" => ["deserizeWithScore@client","redis_serialize_withscore@client","diffWithOption@client""GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "option" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def redis_zinterstore(args)
    result = {}
    data = redis_get_stored_data(args)
    args["args"].each_index do |index|
      key = args["args"][index]
      weight = redis_get_weight(args, index)
      if !result.keys.empty?
        new_hash = redis_deserialize_withscore(data[key])
        keys = result.keys & new_hash.keys
        tmp__ = {}
        keys.each do |key_|
          tmp__[key_] = if args["option"][:aggregate].nil?
                          result[key_].to_f + new_hash[key_].to_f
                        else
                          aggregate_score(args["option"][:aggregate], result[key_].to_f, new_hash[key_].to_f, weight)
                        end
        end
      else
        result = redis_deserialize_withscore(data[key])
      end
    end
    value = redis_serialize_withscore(result)
    set([args["key"], value])
  end

  def redis_get_stored_data(args)
    data = {}
    args["args"].each_index do |index|
      key = args["args"][index]
      data[key] = get([key])
    end
    data
  end

  def redis_get_weight(args, index)
    weight = 1.0
    if args["option"] &&
       args["option"][:weights] &&
       args["option"][:weights][index]
      weight = args["option"][:weights][index].to_f
    end
    weight
  end

  ############
  ## Hashes ##
  ############
  # @conv {"hset" => ["redis_deserialize_hash@client","redis_serialize_hash@client","GET","SET"]}
  ## args = [key, field, value]
  def redis_hset(args)
    data = get([args[0]])
    hash = {}
    unless data.empty?
      hash = redis_deserialize_hash(data)
    end
    hash[args[1]] = args[2]
    result = redis_serialize_hash(hash)
    set([args[0], result])
  end

  # @conv {"hget" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key, field]
  def redis_hget(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash[args[1]]
  end

  # @conv {"hmget" => ["redis_deserialize_hash@client","GET"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def redis_hmget(args)
    data = get([args["key"]])
    hash = redis_deserialize_hash(data)
    result = []
    args["args"].each do |field|
      result.push(hash[field])
    end
    result
  end

  # @conv {"hmset" => ["redis_deserialize_hash@client","redis_serialize_hash@client","GET","SET"]}
  ## args = {"key" => key, "args"=> {field0=>member0,field1=>member1,...}}
  def redis_hmset(args)
    args["key"].gsub!(":", "__colon__")
    ### keyname, asyncable
    data = get([args["key"]])
    hash = {}
    unless data.empty?
      hash = redis_deserialize_hash(data)
    end
    args["args"].each do |field, member|
      hash[field] = member
    end
    result = redis_serialize_hash(hash)
    set([args["key"], result])
  end

  # @conv {"hincrby" => ["redis_deserialize_hash@client","redis_serialize_hash@client","GET","SET"]}
  ## args = [key, field, integer]
  def redis_hincrby(args)
    data = get([args[0]])
    hash = {}
    unless data.empty?
      hash = redis_deserialize_hash(data)
    end
    hash[args[1]] = hash[args[1]].to_i + args[2].to_i
    result = redis_serialize_hash(hash)
    set([args[0], result])
  end

  # @conv {"hexists" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key, field]
  def redis_hexists(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash.key?(args[1])
  end

  # @conv {"hdel" => ["redis_deserialize_hash@client","redis_serialize_hash@client","GET","SET"]}
  ## args = [key, field]
  def redis_hdel(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash.delete(args[1])
    if hash.keys.empty?
      return delete([args[0]])
    end
    result = redis_serialize_hash(hash)
    set([args[0], result])
  end

  # @conv {"hlen" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key]
  def redis_hlen(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash.keys.size
  end

  # @conv {"hkeys" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key]
  def redis_hkeys(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash.keys
  end

  # @conv {"hvals" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key]
  def redis_hvals(args)
    data = get([args[0]])
    hash = redis_deserialize_hash(data)
    hash.values
  end

  # @conv {"hgetall" => ["redis_deserialize_hash@client","GET"]}
  ## args = [key]
  def redis_hgetall(args)
    data = get([args[0]])
    redis_deserialize_hash(data)
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"flushall" => ["flush"]}
  def redis_flushall(args)
    flush(args)
  end

  # @conv {"del" => ["delete"]}
  def redis_del(args)
    delete(args)
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
  def extract_elements_fromlist(list, start_pos, end_pos)
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

  def redis_serialize(array)
    array.join(",")
  end

  def redis_deserialize(str)
    str.split(",")
  end

  # hash {member => score}
  def redis_serialize_withscore(hash, sort = true)
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

  def redis_deserialize_withscore(str)
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

  def redis_serialize_hash(hash)
    result = []
    ## build
    hash.each do |field, member|
      result.push("#{field}__H__#{member}")
    end
    result.join(",")
  end

  def redis_deserialize_hash(str)
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
