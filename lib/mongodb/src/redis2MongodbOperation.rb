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
=begin
 Data Structure [String] @ Mongodb
    Collection :: key
    Doc        :: "value" => value
=end
  # @conv {"SET" => ["INSERT"]}
  def REDIS_SET(args)
    return setString(args)
  end
  # @conv {"GET" => ["FIND"]}
  def REDIS_GET(args)
    return getString(args[0])
  end
  # @conv {"SETNX" => ["INSERT"]}
  def REDIS_SETNX(args)
    return REDIS_SET(args)
  end
  # @conv {"SETEX" => ["INSERT"]}
  def REDIS_SETEX(args)
    return REDIS_SET(args)
  end
  # @conv {"PSETEX" => ["INSERT"]}
  def REDIS_PSETEX(args)
    return REDIS_SET(args)
  end
  # @conv {"MSET" => ["INSERT"]}
  def REDIS_MSET(args)
    args.each{|key,value|
      flag = setString([key,value])
      if(!flag)then
        return false
      end
    }
    return true
  end
  # @conv {"MSETNX" => ["INSERT"]}
  def REDIS_MSETNX(args)
    return REDIS_MSET(args)
  end
  # @conv {"MGET" => ["FIND"]}
  def REDIS_MGET(args)
    result = []
    result = getStrings(args)
    return result
  end
  
  # @conv {"INCR" => ["FIND","INSERT"]}
  def REDIS_INCR(args)
    str = getString(args[0])
    value = str.to_i + 1
    return updateString(args[0],value)
  end
  # @conv {"INCRBY" => ["FIND","INSERT"]}
  def REDIS_INCRBY(args)
    str = getString(args[0])
    value = str.to_i + args[1].to_i
    return updateString(args[0],value)
  end
  # @conv {"DECR" => ["FIND","INSERT"]}
  def REDIS_DECR(args)
    str = getString(args[0])
    value = str.to_i - 1
    return updateString(args[0],value)
  end
  # @conv {"DECRBY" => ["FIND","INSERT"]}
  def REDIS_DECRBY(args)
    str = getString(args[0])
    value = str.to_i - args[1].to_i
    return updateString(args[0],value)
  end

  # @conv {"APPEND" =>  ["FIND","INSERT"]}
  def REDIS_APPEND(args)
    str = getString(args[0])
    value = str + args[1]
    return updateString(args[0],value)
  end
  # @conv {"GETSET" =>  ["FIND","INSERT"]}
  def REDIS_GETSET(args)
    str = getString(args[0])
    updateString(args[0],args[1])
    return str
  end
  # @conv {"STRLEN" => ["FIND","LENGTH@client"]}
  def REDIS_STRLEN(args)
    return getString(args[0]).size
  end
  # @conv {"DELETE" => ["DELETE"]}
  def REDIS_DEL(args__)
    args = {
      "key" => "testdb.col",
      "filter" => {"_id" => args__[0]}
    }
    return DELETE(args)
  end
  def getStrings(keys)
    hash = {}
    hash["key"] = "testdb.col"
    hash["filter"] = { "_id" => { "$in" => keys}}
    values = FIND(hash)
    if(!values or values.size == 0)then
      return []
    end
    result = []
    values.each{|kv|
      result.push(kv["value"])
    }
    return result
  end
  def getString(key)
    hash = {}
    hash["key"] = "testdb.col"
    hash["filter"] = {"_id" => key}
    values = FIND(hash)
    if(!values or values.size == 0)then
      return ""
    end
    return values[0]["value"]
  end
  # collectionName:: test, doc : "_id" => args[0], "value" => args[1]
  def setString(args)
    return INSERT([["testdb.col",{"_id" => args[0], "value" => args[1]}]])
  end
  def updateString(key,value)
    args = {
      "key" => "testdb.col",
      "update" => nil,
      "multi" => false,
      "query" => {"_id" => key}
    }
    args["update"] = {"value" => value}
    return UPDATE(args)
  end
  ###########
  ## Lists ##
  ###########
=begin
 Data Structure [List] @ Mongodb
    Collection :: key
    Doc        :: {"key" => "list", "index" => num, "value" => value}
=end

  # @conv {"LPUSH" => ["AGGREGATE","INSERT","UPDATE"]}
  def REDIS_LPUSH(args)
    updateIndex(args[0],"lpush")
    return pushList(args[0],args[1],"lpush")
  
  end
  # @conv {"RPUSH" => ["AGGREGATE","INSERT"]}
  def REDIS_RPUSH(args)
    return pushList(args[0],args[1],"rpush")
  end
  # @conv {"LLEN" => ["COUNT"]}
  def REDIS_LLEN(args)
    COUNT({"key" => args[0]})
  end
  # @conv {"LRANGE" => ["FIND"]}
  def REDIS_LRANGE(args_)
    args = {
      "key" => args_[0],
      "filter"  => {"index" => { "$gte" => args_[1].to_i, "$lte" => args_[2].to_i}}
    }
    result = []
    FIND(args).each{|val|
      result.push(val["value"])
    }
    return result
  end
  # @conv {"LTRIM" => ["DELETE","UPDATE"]}
  def REDIS_LTRIM(args_)
    args = {
      "key" => args_[0],
      "filter"  => {"index" => { "$gte" => args_[1].to_i, "$lte" => args_[2].to_i}}
    }
    v = DELETE(args)
    opt = {"start" => args_[1].to_i, "end" => args_[2].to_i}
    updateIndex(args["key"],"ltrim",opt)
    return v
  end
  # @conv {"LINDEX" => ["FIND"]}
  def REDIS_LINDEX(args_)
    args = {
      "key" => args_[0],
      "filter"  => {"index" => { "$eq" => args_[1].to_i}}
    }
    result = []
    FIND(args).each{|val|
      result.push(val["value"])
    }
    return result
  end
  # @conv {"LSET" => ["UPDATE"]}
  def REDIS_LSET(args)
    updateIndex(args[0],"lset",args[1])
    return pushList(args[0],args[2],"lset",args[1])
  end
  # @conv {"LREM" => ["DELETE","UPDATE"]}
  def REDIS_LREM(args)
    ## get list (value == args[2])
    hash = {
      "key" => args[0],
      "filter" => {"value" => args[2]}
    }
    result = []
    str = FIND(hash)
    str.each{|doc|
      if(result.size < args[1].to_i)then
        result.push(doc["index"])
      end
    }
    result.each{|index|
      ## delete element
      delListWithIndex(args[0],index)
      ## update 
      updateIndex(args[0],"lrem",index)
    }
    return true
  end
  # @conv {"LPOP" => ["FIND","DELETE","UPDATE"]}
  def REDIS_LPOP(args)
    v = getListWithIndex(args[0],0)
    ## delete element
    delListWithIndex(args[0],0)
    ## update 
    updateIndex(args[0],"lpop",0)
    return v
  end
  # @conv {"RPOP" => ["DELETE","UPDATE"]}
  def REDIS_RPOP(args)
    index = getNewIndex(args[0],"max") - 1
    data = getListWithIndex(args[0],index)
    ## delete element
    delListWithIndex(args[0],index)
    return data
  end
  # @conv {"RPOPLPUSH" => ["DELETE","UPDATE","INSERT","AGGEREGATE"]}
  def REDIS_RPOPLPUSH(args)
    data = REDIS_RPOP([args[0]])
    REDIS_LPUSH([args[1],data])
    return data
  end
  ## Tools for LIST Structure.
  def pushList(key,value,opt,index=0)
    case opt
    when "rpush" then
      index = getNewIndex(key,"max")
    when "lpush" then
      index =  getNewIndex(key,"min")
    when "lset" then
      ## do nothing
    end
    list = getList(key)
    doc = {"key" => "list", "value" => value, "index" => index.to_i}
    return INSERT([[key,doc]])
  end
  def getList(key)
    hash = {}
    hash["key"] = key
    hash["filter"] = {}
    list = FIND(hash)
    return list
  end
  def getListWithIndex(key,index)
    hash = {}
    hash["key"] = key
    hash["filter"] = {"index" => {"$eq" => index}}
    list = FIND(hash)
    if(list and list.size == 1)then
      return list[0]["value"]
    end
    return ""
  end
  def delListWithIndex(key,index)
    args = {
      "key" => key,
      "filter" => {"index" => index}
    }
    DELETE(args)
  end
  ## opt == max or min
  def getNewIndex(key,opt)
    args = {
      "key" => key,
      "match" => {},
      "accumulator_id" => "key",
      "accumulator_colname" => opt,
      "accumulator_target" => "$index",
      "accumulator" => "$#{opt}"
    }
    val = AGGREGATE(args)
    if(val and val.size > 0)then
      if(opt == "max")then
        return val[0][opt].to_i + 1
      elsif(opt == "min")then
        return val[0][opt].to_i - 1
      end
    end
    return 0
  end
  ## type = lpush,ltrim
  def updateIndex(key,type,opt=nil)
    args = {
        "key" => key,
        "query" => {},
        "update" => {},
        "multi"  => true
      }
    flag = true
    case type 
    when "lpush" then
      args["update"] = {"$inc" => {"index" => 1}}
    when "ltrim" then
      ## opt["end"]  
      args["query"] = {"index" => {"$gt" => opt["end"].to_i}}
      decr = (opt["end"].to_i - opt["start"].to_i + 1)*-1
      args["update"] = {"$inc" => {"index" => decr}}
    when "lset" then
      args["query"] = {"index" => {"$gte" => opt.to_i}}
      args["update"] = {"$inc" => {"index" => 1}}
    when "lrem" then
      args["query"] = {"index" => {"$gte" => opt.to_i}}
      args["update"] = {"$inc" => {"index" => -1}}
    when "lpop" then
      args["query"] = {"index" => {"$gte" => opt.to_i}}
      args["update"] = {"$inc" => {"index" => -1}}
    else
      flag = false
      @logger.error(" #{type}")
    end
    if(flag)then
      return UPDATE(args)
    end
    return flag
  end
  #########
  ## Set ##
  #########
=begin
 Data Structure [Set] @ Mongodb
    Collection :: key
    Doc        :: {"value" => value}
=end
  # @conv {"SADD" => ["INSERT"]}
  def REDIS_SADD(args, value=nil)
    r = false
    if(!value)then
      r = pushSet(args[0],args[1])
    end
    return r
  end
  # @conv {"SREM" => ["DELETE"]}
  def REDIS_SREM(args)
    delSet(args[0],args[1])
  end
  # @conv {"SISMEMBER" => ["COUNT"]}
  def REDIS_SISMEMBER(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1]}
    }
    return COUNT(args) > 0
  end

  # @conv {"SPOP" => ["FIND","DELETE"]}
  def REDIS_SPOP(args)
    set = getSet(args[0])
    value = set.sample["value"]
    delSet(args[0],value)
    return value
  end
  # @conv {"SMOVE" => ["DELETE","INSERT"]}
  def REDIS_SMOVE(args)
    srcKey = args[0]
    dstKey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    delSet(srcKey,member)
    ## ADD member to dstKey
    pushSet(dstKey,member)
  end
  # @conv {"REDIS_SADD" => ["COUNT"]}
  def REDIS_SCARD(args__)
    args = {
      "key" => args__[0],
      "filter" => { "value" => args__[1]}
    }
    return COUNT(args) 
  end
  # @conv {"SINTER" => ["FIND"]}
  def REDIS_SINTER(args)
    gotSet = {}
    gottenSet = {}
    args.each{|key|
      gottenSet[key] = getSet(key)
    }
    result = []
    args.each{|key|
      members = []
      gottenSet[key].each{|val|
        members.push(val["value"])
      }
      if(result.size == 0)then
        result = members
      else
        result = result & members
      end
    }
    return result
  end
  # @conv {"SINTERSTORE" => ["FIND","INSERT"]}
  def REDIS_SINTERSTORE(args)
    dstKey = args.shift
    members = REDIS_SINTER(args)
    members.each{|member|
      pushSet(dstKey,member)
    }
  end
  # @conv {"SMEMBERS" => ["FIND"]}
  def REDIS_SMEMBERS(args)
    result = []
    getSet(args[0]).each{|val|
      result.push(val["value"])
    }
    return result
  end
  
  # @conv {"SDIFF" => ["FIND"]}
  def REDIS_SDIFF(args)
    targetKey    = args.shift
    targetValues = getSet(targetKey)
    dataList = []
    args.each{|key|
      dataList.push(getSet(key))
    }
    dataList.each{|d|
      targetValues -= d
    }
    results = []
    targetValues.uniq.each{|val|
      results.push(val["value"])
    }
    return results
  end
  # @conv {"SDIFFSTORE" => ["FIND","INSERT"]}
  def REDIS_SDIFFSTORE(args)
    dstKey = args.shift
    values = REDIS_SDIFF(args)
    values.each{|value|
      pushSet(dstKey,value)
    }
    return true
  end
  # @conv {"SRANDMEMBER" => ["FIND"]}
  def REDIS_SRANDMEMBER(args)
    return getSet(args[0]).sample
  end 
  
  # @conv {"SUNION" => ["FIND"]}
  def REDIS_SUNION(args)
    value = []
    args.each{|key|
      members = getSet(key)
      value += members
    }
    results = []
    value.uniq.each{|val|
      results.push(val["value"])
    }
    return results
  end
  # @conv {"SUNION" => ["FIND","INSERT"]}
  def REDIS_SUNIONSTORE(args)
    result = []
    dstKey = args.shift
    values = REDIS_SUNION(args)
    values.each{|value|
      pushSet(dstKey,value)
    }
    return true
  end
  
  ## Tools for SET Structure.
  def pushSet(key,value)
    doc = {"value" => value}
    return INSERT([[key,doc]])
  end
  def getSet(key)
    return FIND({"key"=>key, "filter"=>{}})
  end
  def delSet(key,value)
    args = {
      "key" => key,
      "filter" => {"value" => value}
    }
    return DELETE(args)
  end

  #################
  ## Sorted Sets ##
  #################
=begin
 Data Structure [Sorted Sets] @ Mongodb
    Collection :: key
    Doc        :: {"value" => value, "score" => score}
=end

  # @conv {"ZADD" => ["INSERT"]}
  ## args = [key, score, value]
  def REDIS_ZADD(args)
    return pushSortedSet(args[0],args[1].to_i,args[2])
  end
  # @conv {"ZREM" => ["DELETE"]}
  ## args = [key, value]
  def REDIS_ZREM(args)
    return delSet(args[0],args[1])
  end
  # @conv {"ZINCRBY" => ["UPDATE","INSERT"]}
  ## args = [key, inc, value]
  def REDIS_ZINCRBY(args__)
    args = {
      "key" => args__[0],
      "query" => {},
      "update" => {},
      "multi"  => true
    }
    args["query"]  = {"value" => args__[2]}
    args["update"] = {"$inc" => {"score" => args__[1].to_i}}
    if(!UPDATE(args))then
      return REDIS_ZADD(args__)
    end
    return true
  end
  # @conv {"ZRANK" => ["FIND","COUNT"]}
  ## args = [key, value]
  def REDIS_ZRANK(args,comparison="$lte")
    # Get Value Score
    if(score = getScoreByValue(args[0],args[1]))then
      # Get Order ()
      hash = {
        "key" => args[0],
        "filter" => []
      }
      hash["filter"] = { "score" => {"#{comparison}" => score.to_i}}
      return COUNT(hash)
    else
      return args[1]
    end
  end
  # @conv {"ZREVRANK" => ["FIND","COUNT"]}
  ## args = [key, value]
  def REDIS_ZREVRANK(args)
    return REDIS_ZRANK(args,comparison="glte")
  end
  # @conv {"ZRANGE" => ["AGGERGATE"]}
  ## args = [key, start, end]
  def REDIS_ZRANGE(args, order=1)
    hash = {
      "key"  => args[0],
      "sortName" => "score",
      "sortOrder" => order,
      "limit" => args[2].to_i
    }
    values = AGGREGATE(hash)
    @logger.debug(values)
    i = args[1].to_i
    results = []
    while(i <= args[2].to_i and values[i]) do
      results.push(values[i]["value"])
      i += 1
    end
    return results
  end
  # @conv {"ZREVRANGE" => ["AGGERGATE"]}
  ## args = [key, start, end]
  def REDIS_ZREVRANGE(args)
    REDIS_ZRANGE(args,-1)
  end
  # @conv {"ZRANGEBYSCORE" => ["FIND"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    hash = {
      "key"  => args[0],
      "filter" => nil,
      "sort" => nil
    }
    hash["filter"] = { "score" => {"$gte" => args[1].to_i, "$lte" => args[2].to_i }}
    hash["filter"] = { "score" => 1}
    result = []
    result = FIND(hash).to_json
    return result
  end
  # @conv {"ZCOUNT" => ["COUNT"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    hash = {
      "key"  => args[0],
      "filter" => nil
    }
    hash["filter"] = { "score" => {"$gte" => args[1].to_i, "$lte" => args[2].to_i }}
    COUNT(hash)
  end
  # @conv {"ZCARD" => ["COUNT"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    hash = {
      "key"  => args[0],
      "filter" => nil
    }
    COUNT(hash)
  end
  # @conv {"ZSCORE" => ["FIND"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    hash = {
      "key"  => args[0],
      "filter" => { "value" => args[1]}
    }
    return FIND(hash).to_json
  end
  # @conv {"ZREMRANGEBYRANK" => ["AGGREGATE","DELETE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYRANK(args)
    ## GET TARGET VALUES
    values = REDIS_ZRANGE(args,1)
    if(values.size > 0)then
      hash = {
        "key" => args[0],
        "filter" => { "$or" => []}
      }
      values.each{|val|
        hash["filter"]["$or"].push({"value" => val})
      }
    end
    return DELETE(hash)
  end
  # @conv {"ZREMRANGEBYSCORE" => ["DELETE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYSCORE(args)
    # DELETE VALEUS
    hash = {
      "key" => args[0],
      "filter" => { "score" => {"$gte" => args[1].to_i , "$lte" => args[2].to_i}}
    }
    return DELETE(hash)
  end
  # @conv {"ZUNIONSTORE" => ["FIND","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZUNIONSTORE(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index{|index|
      docs[index.to_s] = getSet(args["args"][index])
    }
    args["args"].each_index{|index|
      docs[index.to_s].each{|doc|
        if(!data[doc["value"].to_s])then
          data[doc["value"].to_s] = []
        end
        weight = 1
        if(args["options"] and 
            args["options"][:weights] and
            args["options"][:weights][index])then
          weight = args["options"][:weights][index].to_i
        end
        data[doc["value"].to_s].push(doc["score"].to_i * weight)
      }
    }
    ## CREATE DOC
    aggregate = "SUM"
    if(args["options"] and 
        args["options"][:aggregate])then
      aggregate = args["options"][:aggregate].upcase
    end
    docs = createDocsWithAggregate(args["key"],data,aggregate)
    return INSERT(docs)
  end
  # @conv {"ZINTERSTORE" => ["FIND","INSERT"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZINTERSTORE(args)
    ## GET DATA
    data = {} ## value => score
    docs = {}
    args["args"].each_index{|index|
      docs[index.to_s] = getSet(args["args"][index])
    }
    args["args"].each_index{|index|
      docs[index.to_s].each{|doc|
        if(!data[doc["value"].to_s] and index == 0)then
          data[doc["value"].to_s] = []
        end
        if(data[doc["value"].to_s])then
          weight = 1
          if(args["options"] and 
              args["options"][:weights] and
              args["options"][:weights][index])then
            weight = args["options"][:weights][index].to_i
          end
          data[doc["value"].to_s].push(doc["score"].to_i * weight)
        end
      }
    }
    ## CREATE DOC
    aggregate = "SUM"
    if(args["options"] and 
        args["options"][:aggregate])then
      aggregate = args["options"][:aggregate].upcase
    end
    docs = createDocsWithAggregate(args["key"],data,aggregate)
    INSERT(docs)
  end
  
  ## Tools for Sorted Set Structure.
  def pushSortedSet(key,score,value)
    doc = {
      "score" => score,
      "value" => value
    }
    return INSERT([[key,doc]])
  end
  def getScoreByValue(key,value)
    hash = {}
    hash["key"] = key
    hash["filter"] = { "value" => value}
    score = FIND(hash)
    if(score and score.size > 0)then
      return score[0]["score"]
    end
    return nil
  end
  def createDocsWithAggregate(dstKey,data,aggregate)
    docs = []
    case aggregate.upcase()
    when "SUM" then
      data.each_key{|key|
        doc = {"value" => key, "score" => data[key].inject(:+)}
        docs.push([dstKey,doc])
      }
    when "MAX" then
      data.each_key{|key|
        doc = {"value" => key, "score" => data[key].max}
        docs.push([dstKey,doc])
      }
    when "MIN" then
      data.each_key{|key|
        doc = {"value" => key, "score" => data[key].min}
        docs.push([dstKey,doc])
      }
    else
      @logger.error("Unsupported Aggregating Operation #{aggregate}")
    end
    return docs
  end

  ############
  ## Hashes ##
  ############
=begin
 Data Structure [Hashed] @ Mongodb
    Collection :: key
    Doc        :: {"field" => field, "value" => value}
=end

  # @conv {"HSET" => ["INSERT"]}
  ## args = [key, field, value]
  def REDIS_HSET(args)
    value = changeNumericWhenNumeric(args[2])
    doc = {"redisKey" => args[0], "field" => args[1], "value" => value}
    INSERT([["test",doc]])
  end
  # @conv {"HGET" => ["FIND"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0], "field" => args[1]}
    docs = FIND(cond)
    return docs.to_json
  end
  # @conv {"HMGET" => ["FIND"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def REDIS_HMGET(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "$or" => [],"redisKey" => args["key"]}
    args["args"].each{|arg|
      cond["filter"]["$or"].push("field" => arg)
    }
    return FIND(cond).to_json
  end

  # @conv {"HMSET" => ["INSERT"]}
  ## args = {"key" => "__key__", "args"=> {field0=>member0,field1=>member1,...}}
  def REDIS_HMSET(args)
    docs = []
    json = {}
    args["args"].each{|field,value_|
      value = changeNumericWhenNumeric(value_)
      json[field] = value
    }
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
      "multi"  => true
    }
    args["query"]  = {"redisKey" => args__[0],"field" => args__[1]}
    args["update"] = {"$inc" => {"value" => args__[2].to_i}}
    if(!UPDATE(args))then
      return REDIS_HSET(args__)
    end
    return true
  end

  # @conv {"HEXISTS" => ["FIND"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    cond = {}
    cond["key"] = "test"
    cond["filter"] = { "redisKey" => args[0],"field" => args[1]}
    docs = FIND(cond)
    return docs.size > 0
  end

  # @conv {"HDEL" => ["DELETE"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    cond = {
      "key" => "test",
      "filter" => {"redisKey" => args[0], "field" => args[1]}
    }
    return DELETE(cond)
  end
  # @conv {"HLEN" => ["COUNT"]}
  ## args = [key]
  def REDIS_HLEN(args)
    cond = {
      "key"  => "test",
      "filter" => {"redisKey" => args[0]}
    }
    return COUNT(cond)
  end
  # @conv {"HKEYS" => ["FIND"]}
  ## args = [key]
  def REDIS_HKEYS(args)
    cond = {
      "key"  => "test",
      "filter" => nil,
      "projection" => {"redisKey" => args[0], "field" => 1}
    }
    results = []
    FIND(cond).each{|doc|
      results.push(doc["field"])
    }
    return results
  end
  # @conv {"HVALS" => ["FIND"]}
  ## args = [key]
  def REDIS_HVALS(args)
    cond = {
      "key"  => "test",
      "filter" => {"redisKey" => args[0]},
      "projection" => {"value" => 1}
    }
    results = []
    FIND(cond).each{|doc|
      results.push(doc["value"])
    }
    return results
  end
  # @conv {"HGETALL" => ["FIND"]}
  def REDIS_HGETALL(args)
    cond = {
      "key"  => "test",
      "filter" => {"redisKey" => args[0]}
    }
    v = FIND(cond)
    #puts doc["field"] doc["value"]
    return v.to_json
  end
  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["FLUSH"]}
  def REDIS_FLUSHALL(args)
    DROP(["testdb"])
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
      result["args"]    = args
    end
    return result
  end

  ## Private Function
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
    if(operation)then
      case operation.upcase
      when "SUM" then
        score = v0 + v1*weight
      when "MAX" then
        score = [v0,v1*weight].max
      when "MIN" then
        score = [v0,v1*weight].min
      else
        @logger.error("Unsupported aggregation #{operation} @AGGREGATION_REDIS")
      end
    else
      score = v0 + v1*weight
    end
    return score
  end
  
end

