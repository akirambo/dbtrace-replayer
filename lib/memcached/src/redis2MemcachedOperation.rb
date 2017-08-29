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
    return SET(args)
  end
  # @conv {"GET" => ["GET"]}
  def REDIS_GET(args)
    return GET(args)
  end
  # @conv {"SETNX" => ["ADD"]}
  def REDIS_SETNX(args)
    return ADD(args)
  end
  # @conv {"SETEX" => ["SET"]}
  def REDIS_SETEX(args)
    return SET(args)
  end
  # @conv {"PSETEX" => ["SET"]}
  def REDIS_PSETEX(args)
    args[2] = (args[2].to_f / 1000).to_i + 1
    return SET(args)
  end
  # @conv {"MSET" => ["SET"]}
  def REDIS_MSET(args)
    args.each{|key,value|
      if(!SET([key,value]))then
        return false
      end
    }
    return true
  end
  # @conv {"MGET" => ["GET"]}
  def REDIS_MGET(args)
    result = []
    args.each{|key|
      result.push(GET([key]))
    }
    return result
  end
  # @conv {"MSETNX" => ["ADD"]}
  def REDIS_MSETNX(args)
    args.each{|key,value|
      if(!ADD([key,value]))then
        return false
      end
    }
    return true
  end
  # @conv {"INCR" => ["INCR"]}
  def REDIS_INCR(args)
    args[0].gsub!(":","__colon__")
    return INCR([args[0],1])
  end
  # @conv {"INCRBY" => ["INCR"]}
  def REDIS_INCRBY(args)
    args[0].gsub!(":","__colon__")
    return INCR(args)
  end
  # @conv {"DECR" => ["DECR"]}
  def REDIS_DECR(args)
    args[0].gsub!(":","__colon__")
    return DECR([args[0],1])
  end
  # @conv {"DECRBY" => ["DECRBY"]}
  def REDIS_DECRBY(args)
    args[0].gsub!(":","__colon__")
    return DECR([args[0],args[1]])
  end
  # @conv {"APPEND" => ["APPEND"]}
  def REDIS_APPEND(args)
    return APPEND(args)
  end
  # @conv {"GETSET" => ["REPLACE"]}
  def REDIS_GETSET(args)
    old = GET(args)
    REPLACE(args)
    return old
  end
  # @conv {"STRLEN" => ["GET","LENGTH@client"]}
  def REDIS_STRLEN(args)
    return GET(args).size
  end
  ###########
  ## Lists ##
  ###########
  # @conv {"LPUSH" => ["GET","SET"]}
  def REDIS_LPUSH(args)
    key = args[0]
    value = args[1]
    existData = GET([key])
    if(existData.size != 0)then
      value = value + ","+existData 
    end
    return SET([key,value])
  end
  # @conv {"RPUSH" => ["GET","SET"]}
  def REDIS_RPUSH(args)
    key = args[0]
    value = args[1]
    existData = GET([key])
    if(existData.size != 0)then
      value = existData +"," + value
    end
    return SET([key,value])
  end
  # @conv {"LPOP" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LPOP(args)
    data = GET(args)
    value = redisDeserialize(data)
    str = value.shift
    value = redisSerialize(value)
    if(value.size > 0)then
      SET([args[0],value])
    else
      DELETE([args[0]])
    end
    return str
  end
  # @conv {"RPOP" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_RPOP(args)
    data  = GET(args)
    value = redisDeserialize(data)
    data  = value.pop
    value = redisSerialize(value)
    if(value.size > 0)then
      SET([args[0],value])
    else
      DELETE([args[0]])
    end
    return data
  end
  # @conv {"LRANGE" => ["redisDeserialize@client","extractElementsFromList@client","GET"]}
  def REDIS_LRANGE(args)
    data = GET([args[0]])
    list = redisDeserialize(data)
    value = extractElementsFromList(list,args[1],args[2])
    return value
  end
  # @conv {"LREM" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LREM(args)
    data = GET([args[0]])
    value = redisDeserialize(data)
    result = []
    count = args[1].to_i
    value.each{|elem|
      if(elem == args[2] and count != 0)then
        # skip
        count -= 1
      else
        result.push(elem)
      end
    }
    result = redisSerialize(result)
    return SET([args[0],result])
  end
  # @conv {"LINDEX" => ["redisDeserialize@client","extractElementsFromList@client","GET","SET"]}
  def REDIS_LINDEX(args)
    data = GET([args[0]])
    value = redisDeserialize(data)[args[1].to_i]
    return value
  end
  # @conv {"RPOPLPUSH" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_RPOPLPUSH(args)
    data = REDIS_RPOP([args[0]])
    REDIS_LPUSH([args[1],data])
    return data
  end
  # @conv {"LSET" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_LSET(args)
    data = GET([args[0]])
    value = redisDeserialize(data)
    value[args[1].to_i] = args[2]
    value = redisSerialize(value)
    return SET([args[0],value])
  end
  # @conv {"LTRIM" => ["redisDeserialize@client","extractElementsFromList@client","redisSerialize@client","GET","SET"]}
  def REDIS_LTRIM(args)
    data = REDIS_LRANGE(args)
    value = redisSerialize(data)
    return SET([args[0],value])
  end
  # @conv {"LLEN" => ["redisDeserialize@client","GET"]}
  def REDIS_LLEN(args)
    data = GET([args[0]])
    len = redisDeserialize(data).size
    return len
  end
  #########
  ## Set ##
  #########
  # @conv {"SRANDMEMBER" => ["redisDeserialize@client","GET"]}
  def REDIS_SRANDMEMBER(args)
    key = args[0]
    data = GET([key])
    members     = redisDeserialize(data)
    popedMember = members.sample
    return popedMember
  end 
  # @conv {"SMEMBERS" => ["redisDeserialize@client","GET"]}  
  def REDIS_SMEMBERS(args)
    key = args[0]
    value = GET([key])
    return redisDeserialize(value)
  end
  # @conv {"SDIFF" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SDIFF(args)
    common = []
    membersArray = []
    dataArray = []
    args.each{|key|
      dataArray.push(GET([key]))
    }
    dataArray.each{|data|
      members = redisDeserialize(data)
      membersArray += members
      if(common.size == 0)then
        common = members
      else
        common = common & members
      end
    }
    value = membersArray.uniq - common.uniq
    result = redisSerialize(value)
    return result
  end
  # @conv {"SDIFFSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SDIFFSTORE(args)
    dstKey = args.shift
    value = REDIS_SDIFF(args)
    return SET([dstKey,value])
  end
  # @conv {"SINTER" => ["redisDeserialize@client","GET"]}
  def REDIS_SINTER(args)
    result = []
    dataArray = []
    args.each{|key|
      dataArray.push(GET([key]))
    }
    dataArray.each{|data|
      members = redisDeserialize(data)
      if(result.size == 0)then
        result = members
      else
        result = result & members
      end
    }
    value = redisSerialize(result)
    return value
  end
  # @conv {"SINTERSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SINTERSTORE(args)
    result = []
    dstKey = args.shift
    result = REDIS_SINTER(args)
    return SET([dstKey,result])
  end
  # @conv {"SUNION" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SUNION(args)
    value = []
    dataArray = []
    args.each{|key|
      members = redisDeserialize(GET([key]))
      value += members
    }
    result = redisSerialize(value.uniq)
    return result
  end
  # @conv {"SUNION" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SUNIONSTORE(args)
    result = []
    dstKey = args.shift
    result = REDIS_SUNION(args)
    return SET([dstKey,result])
  end
  # @conv {"SISMEMBER" => ["redisDeserialize@client","GET"]}
  def REDIS_SISMEMBER(args)
    target = args.pop
    value = REDIS_SMEMBERS(args)
    return value.include?(target)
  end
  # @conv {"SREM" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SREM(args)
    key   = args[0]
    value = args[1]
    data = GET([key])
    members = redisDeserialize(data)
    members.delete(value)
    if(members.size > 0)then
      result = redisSerialize(members)
      return REPLACE([key,result])
    end
    return DELETE([key])
  end
  # @conv {"SMOVE" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SMOVE(args)
    srcKey = args[0]
    dstKey = args[1]
    member = args[2]
    srcData = GET([srcKey])
    dstData = GET([dstKey])
    ## REMOVE member from srtKey
    members = redisDeserialize(srcData)
    members.delete(member)
    srcResult = redisSerialize(members)
    ## ADD member to dstKey
    members = redisDeserialize(dstData)
    members.push(member)
    dstResult = redisSerialize(members)
    return (REPLACE([srcKey,srcResult]) and REPLACE([dstKey,dstResult]))
  end
  # @conv {"SCARD" => ["GET","redisDeserialize@client"]}
  def REDIS_SCARD(args)
    members = GET([args[0]])
    data = redisDeserialize(members).size
    return data
  end
  # @conv {"SADD" => ["redisSerialize@client","SET","REPLACE"]}
  def REDIS_SADD(args)
    key = args.shift
    existingData = GET([key])
    value = redisSerialize(args)
    if(existingData.size > 0)then
      return REPLACE([key,"#{existingData},#{value}"])
    else
      return SET([key,value])
    end
  end
  # @conv {"SPOP" => ["redisDeserialize@client","redisSerialize@client","GET","REPLACE"]}
  def REDIS_SPOP(args)
    key = args[0]
    data = GET([key])
    ## GET
    members = redisDeserialize(data)
    popedMember = members.sample
    members.delete(popedMember)
    result = redisSerialize(members)
    if(result.size > 0)then
      REPLACE([key,result])
    else
      DELETE([key])
    end
    return popedMember
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
    return SET([args[0],result])
  end
  # @conv {"ZREM" => ["redisDeserializeWithScore@client","redisSerializeWithScore@client","GET","SET"]}
  ## args = [key, member]
  def REDIS_ZREM(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash.delete(args[1])
    result = redisSerializeWithScore(hash,false)
    if(result.size > 0)then
      return SET([args[0],result])
    end
    return DELETE([args[0]])
  end
  # @conv {"ZINCRBY" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore@client","GET","SET"]}
  ## args = [key, score, member]
  def REDIS_ZINCRBY(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    hash[args[2]] += args[1].to_i
    result = redisSerializeWithScore(hash,true)
    return SET([args[0],result])
  end
  # @conv {"ZRANK" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, member]
  def REDIS_ZRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    return (hash.keys()).index(args[1])
  end
  # @conv {"ZREVRANK" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, member]
  def REDIS_ZREVRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    return (hash.keys()).reverse.index(args[1])
  end
  # @conv {"ZRANGE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, start, end]
  def REDIS_ZRANGE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    return sortedArrayGetRange(args[1].to_i,args[2].to_i,hash.keys())
  end
  # @conv {"ZREVRANGE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, start, end]
  def REDIS_ZREVRANGE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    return sortedArrayGetRange(args[1].to_i,args[2].to_i,hash.keys().reverse)
  end
  # @conv {"ZRANGEBYSCORE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    selected = hash.select{|member,score| score >= args[1].to_i and score <= args[2].to_i}
    return selected.keys
  end
  # @conv {"ZCOUNT" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    selected = REDIS_ZRANGEBYSCORE(args)
    return selected.size
  end
  # @conv {"ZCARD" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    data = GET(args)
    hash = redisDeserializeWithScore(data)
    return hash.keys.size
  end
  # @conv {"ZSCORE" => ["redisDeserializeWithScore@client","GET"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    return hash[args[1]].to_i
  end
  # @conv {"ZREMRANGEBYSCORE" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore","GET","SET"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYSCORE(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    selected = hash.select{|member,score| score < args[1].to_i or score > args[2].to_i}
    result = redisSerializeWithScore(selected)
    return SET([args[0],result])
  end
  # @conv {"ZREMRANGEBYRANK" => ["redisDeserializeWithScore@client","sortByScore@client","redisSerializeWithScore","GET","SET"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYRANK(args)
    data = GET([args[0]])
    hash = redisDeserializeWithScore(data)
    result = {}
    i = 0
    hash.each{|member,score| 
      if(i < args[1].to_i or i > args[2].to_i)then
        result[member] = score
      end
      i += 1
    }
    value = redisSerializeWithScore(result)
    return SET([args[0],value])
  end
  # @conv {"ZUNIONSTORE" => ["deserizeWithScore@client","redisSerializeWithScore@client","mergeWithOption@client","GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZUNIONSTORE(args)
    result = {}
    data = {}
    args["args"].each_index{|index|
      key = args["args"][index]
      data[key] = GET([key])
    }
    args["args"].each_index{|index|
      key = args["args"][index]
      weight = 1.0
      if(args["options"] and 
          args["options"][:weights] and 
          args["options"][:weights][index])then
        weight = args["options"][:weights][index].to_f
      end
      if(result.keys.size > 0)then
        result.merge!(redisDeserializeWithScore(data[key])){|_key, v0, v1|
          aggregateScore(args["options"][:aggregate], v0.to_f, v1.to_f, weight)
        }
      else
        result = redisDeserializeWithScore(data[key])
      end
    }
    value = redisSerializeWithScore(result)
    return SET([args["key"],value])
  end
  # @conv {"ZINTERSTORE" => ["deserizeWithScore@client","redisSerializeWithScore@client","diffWithOption@client""GET","SET"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}

  def REDIS_ZINTERSTORE(args)
    result = {}
    data   = {}
    args["args"].each_index{|index|
      key = args["args"][index]
      data[key] = GET([key])
    }
    args["args"].each_index{|index|
      key = args["args"][index]
      weight = 1.0
      if(args["options"] and 
          args["options"][:weights] and 
          args["options"][:weights][index])then
        weight = args["options"][:weights][index].to_f
      end
      if(result.keys.size > 0)then
        newHash = redisDeserializeWithScore(data[key])
        keys = result.keys & newHash.keys
        __tmp__ = {}
        keys.each{|key|
          __tmp__[key] = aggregateScore(args["options"][:aggregate], result[key].to_f, newHash[key].to_f,weight)
        }
      else
        result = redisDeserializeWithScore(data[key])
      end
    }
    value = redisSerializeWithScore(result)
    return SET([args["key"],value])
  end
  ############
  ## Hashes ##
  ############
  # @conv {"HSET" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field, value]
  def REDIS_HSET(args)
    data = GET([args[0]])
    hash = {}
    if(data.size > 0)then
      hash = redisDeserializeHash(data)
    end
    hash[args[1]] = args[2]
    result = redisSerializeHash(hash)
    return SET([args[0],result])
  end
  # @conv {"HGET" => ["redisDeserializeHash@client","GET"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    return hash[args[1]]
  end
  # @conv {"HMGET" => ["redisDeserializeHash@client","GET"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def REDIS_HMGET(args)
    data = GET([args["key"]])
    hash = redisDeserializeHash(data)
    result = []
    args["args"].each{|field|
      result.push(hash[field])
    }
    return result
  end
  # @conv {"HMSET" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = {"key" => key, "args"=> {field0=>member0,field1=>member1,...}}
  def REDIS_HMSET(args)
    args["key"].gsub!(":","__colon__")
    ### keyname, asyncable
    data = GET([args["key"]])
    hash = {}
    if(data.size > 0)then
      hash = redisDeserializeHash(data)
    end
    args["args"].each{|field,member|
      hash[field] = member
    }
    result = redisSerializeHash(hash)
    return SET([args["key"],result])
  end
  # @conv {"HINCRBY" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field, integer]
  def REDIS_HINCRBY(args)
    data = GET([args[0]])
    hash = {}
    if(data.size > 0)then
      hash = redisDeserializeHash(data)
    end
    hash[args[1]] = hash[args[1]].to_i + args[2].to_i
    result = redisSerializeHash(hash)
    return SET([args[0],result])
  end
  # @conv {"HEXISTS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    return hash.key?(args[1])
  end
  # @conv {"HDEL" => ["redisDeserializeHash@client","redisSerializeHash@client","GET","SET"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    hash.delete(args[1])
    if(hash.keys.size == 0)then
      return DELETE([args[0]])
    end
    result = redisSerializeHash(hash)
    return SET([args[0],result])
  end
  # @conv {"HLEN" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HLEN(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    return  hash.keys.size
  end
  # @conv {"HKEYS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HKEYS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    return hash.keys
  end
  # @conv {"HVALS" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HVALS(args)
    data = GET([args[0]])
    hash = redisDeserializeHash(data)
    return hash.values
  end
  # @conv {"HGETALL" => ["redisDeserializeHash@client","GET"]}
  ## args = [key]
  def REDIS_HGETALL(args)
    data = GET([args[0]])
    return redisDeserializeHash(data)
  end
  
  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["FLUSH"]}
  def REDIS_FLUSHALL(args)
    return FLUSH(args)
  end
  # @conv {"DEL" => ["DELETE"]}
  def REDIS_DEL(args)
    return DELETE(args)
  end
  #############
  ## PREPARE ##
  #############
  def prepare_REDIS(operand,args)
    result = {}
    result["operand"] = "REDIS_#{operand}"
    if(["ZUNIONSTORE","ZINTERSTORE"].include?(operand))then
      result["args"] = @parser.extractZ_X_STORE_ARGS(args)
    elsif(["MSET","MGET","MSETNX"].include?(operand))then
      result["args"] = @parser.args2hash(args)
    elsif(["HMGET"].include?(operand))then
      result["args"] = @parser.args2key_args(args)
    elsif(["HMSET"].include?(operand))then
      result["args"] = @parser.args2key_hash(args)
    else
      result["args"] = args
    end
    return result
  end

  ## Private Function
  def extractElementsFromList(list,startPos,endPos)
    result = []
    startPos = startPos.to_i
    endPos   = endPos.to_i
    i = startPos
    while (i <= endPos) do
      result.push(list[i])
      i += 1;
    end
    return result
  end
  def redisSerialize(array)
    return array.join(",")
  end
  def redisDeserialize(str)
    return str.split(",")
  end
  # hash {member => score}
  def redisSerializeWithScore(hash,sort=true)
    result = []
    ## sortByScore
    if(sort)then
      hash.sort{|(k1,v1),(k2,v2)| v1 <=> v2}
    end
    ## build
    hash.each{|member,score|
      result.push("#{score.to_s}_#{member}")
    }
    return result.join(",")
  end
  def redisDeserializeWithScore(str)
    result = {}
    if(str and str.size > 0)then
      strWithScore = str.split(",")
      strWithScore.each{|value|
        ## val = [score, member]
        val = value.split("_")
        result[val[1]] = val[0].to_i
      }
    end
    return result
  end
  def redisSerializeHash(hash)
    result = []
    ## build
    hash.each{|field,member|
      result.push("#{field}__H__#{member}")
    }
    return result.join(",")
  end
  def redisDeserializeHash(str)
    result = {}
    if(str and str.size > 0)then
      strWithScore = str.split(",")
      strWithScore.each{|value|
        ## val = [score, member]
        val = value.split("__H__")
        result[val[0]] = val[1]
      }
    end
    return result
  end
  def sortedArrayGetRange(startIndex,endIndex,members)
    result = []
    i = startIndex
    if(i == -1)then
      i = 0
    end
    if(endIndex == -1)then
      endIndex = members.size - 1
    end
    while (i <= endIndex) do
      result.push(members[i])
      i += 1
    end

    return result
  end
  def aggregateScore(operation, v0,v1,weight)
    score = 0
    case operation.upcase
    when "SUM" then
      score = v0 + v1*weight
    when "MAX" then
      score = [v0,v1*weight].max
    when "MIN" then
      score = [v0,v1*weight].min
    else
      score = v0 + v1*weight
    end
    return score
  end
end

