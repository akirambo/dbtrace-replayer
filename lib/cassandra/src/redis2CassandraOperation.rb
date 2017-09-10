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

module Redis2CassandraOperation
  private
  ############
  ## String ##
  ############
  # @conv {"SET" => ["INSERT"]}
  def REDIS_SET(args, cond ={}, onTime=false)
    table = @options[:keyspace]+"."+@options[:columnfamily]
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}','#{args[1]}')"
    r = true
    if(cond["ttl"])then
      command += " USING TTL #{cond["ttl"]}"
    end
    begin
      DIRECT_EXECUTER(command + ";", onTime)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      r = false
    end
    return r
  end
  # @conv {"GET" => ["SELECT"]}
  def REDIS_GET(args,onTime=false)
    table = @options[:keyspace]+"."+@options[:columnfamily]
    command = "SELECT value FROM #{table}"
    if(args.size > 0)then
      command += " WHERE key = '#{args[0]}' ;"
    end
    begin
      value = DIRECT_EXECUTER(command,onTime)
      if(value["value"] and value["value"][0])then
        return value["value"][0]
      else
        return ""
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    return ""
  end
  # @conv {"SETNX" => ["INSERT","SELECT"]}
  def REDIS_SETNX(args)
    if(REDIS_GET(args,false) == "")then
      return REDIS_SET(args)
    end
    return false
  end
  # @conv {"SETEX" => ["INSERT"]}
  def REDIS_SETEX(args)
    args[2] = (args[2].to_f / 1000).to_i + 1
    return REDIS_SET(args,{"ttl" => args[2]})
  end
  # @conv {"PSETEX" => ["INSERT"]}
  def REDIS_PSETEX(args)
    args[2] = (args[2].to_f / 1000000).to_i + 1
    return REDIS_SET(args,{"ttl" => args[2]})
  end
  # @conv {"MSET" => ["INSERT"]}
  def REDIS_MSET(args)
    args.each{|key,value|
      r = REDIS_SET([key,value])
      if(r == false)then
        return false
      end
    }
    return true
  end
  # @conv {"MGET" => ["SELECT"]}
  def REDIS_MGET(args)
    result = []
    args.each{|key,value|
      result.push(REDIS_GET([key],false))
    }
    return result
  end
  # @conv {"MSETNX" => ["INSERT"]}
  def REDIS_MSETNX(args)
    args.each{|key,value|
      r = REDIS_SETNX([key,value])
      if(!r)then
        return false
      end
    }
    return true
  end
  # @conv {"INCR" => ["SELECT","INSERT"]}
  def REDIS_INCR(args)
    value = REDIS_GET([args[0]],false).to_i + 1
    return REDIS_SET([args[0],value])
  end
  # @conv {"INCRBY" => ["SELECT","INSERT"]}
  def REDIS_INCRBY(args)
    value = REDIS_GET([args[0]],false).to_i + args[1].to_i
    return REDIS_SET([args[0],value])
  end
  # @conv {"DECR" => ["SELECT","INSERT"]}
  def REDIS_DECR(args)
    value = REDIS_GET([args[0]],false).to_i - 1
    return REDIS_SET([args[0],value])
  end
  # @conv {"DECRBY" => ["SELECT","INSERT"]}
  def REDIS_DECRBY(args)
    value = REDIS_GET([args[0]],false).to_i - args[1].to_i
    return REDIS_SET([args[0],value])
  end
  # @conv {"APPEND" => ["SELECT","INSERT"]}
  def REDIS_APPEND(args)
    value = REDIS_GET([args[0]],false).to_s + args[1]
    return REDIS_SET([args[0],value])
  end
  # @conv {"GETSET" => ["SELECT","INSERT"]}
  def REDIS_GETSET(args)
    val = REDIS_GET([args[0]])
    REDIS_SET(args)
    return val
  end
  # @conv {"STRLEN" => ["SELECT","LENGTH@client"]}
  def REDIS_STRLEN(args)
    return REDIS_GET([args[0]],false).size
  end
  # @conv {"DEL" => ["DELETE"]}
  def REDIS_DEL(args, onTime=false)
    table = @options[:keyspace]+"."+@options[:columnfamily]
    command = "DELETE FROM #{table} WHERE key = '#{args[0]}';"
    r = true
    begin
      DIRECT_EXECUTER(command,onTime)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      r = false
    end
    return r
  end
  ###########
  ## Lists ##
  ###########
  # @conv {"LPUSH" => [UPDATE"]}
  def REDIS_LPUSH(args)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    command = "UPDATE #{table} SET value = ['#{args[1]}'] + value WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  # @conv {"RPUSH" => ["UPDATE"]}
  def REDIS_RPUSH(args)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    command = "UPDATE #{table} SET value = value + ['#{args[1]}'] WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  # @conv {"LPOP" => ["SELECT","DELETE"]}
  def REDIS_LPOP(args)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    ### GET 
    values = redis_lget(args)
    ### DELETE
    command = "DELETE value[0] FROM #{table} WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return values.first
  end
  # @conv {"RPOP" => ["SELECT","DELETE"]}
  def REDIS_RPOP(args,stdout=true)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    ### GET 
    values = redis_lget(args)
    ### DELETE
    command = "DELETE value[#{values.size - 1}] FROM #{table} WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return values.last
  end
  # @conv {"LRANGE" => ["SELECT"]}
  def REDIS_LRANGE(args)
    list = redis_lget(args)
    first = args[1].to_i
    last  = args[2].to_i
    if(first < 0)then
      first = 0
    elsif(first > 0)then
      first -= 1
    end
    if(last < 0)then
      last = list.size - 1
    elsif(last > 0)then
      last -= 1
    end
    result = []
    list.each_index{|index|
      if(index >= first and index <= last)then
        result.push(list[index])
      end
    }
    return result
  end
  # @conv {"LREM" => ["UPDATE","SELECT"]}
  def REDIS_LREM(args)
    count = args[1].to_i
    value = args[2]
    values = redis_lget(args)
    if(count == 0)then
      values.delete(value)
      return redis_lreset(args,values)
    elsif(count > 0)then
      newValues = []
      values.each_index{|index|
        if(values[index] == value and count > 0)then
          count -= 1
        else
          newValues.push(values[index])
        end
      }
      return redis_lreset(args,newValues)
    elsif(count < 0)then
      count *= -1
      newValues = []
      values.reverse!
      values.each_index{|index|
        if(values[index] == value and count > 0)then
          count -= 1
        else
          newValues.push(values[index])
        end
      }
      newValues.reverse!
      return redis_lreset(args,newValues)
    end
  end
  # @conv {"LINDEX" => ["SELECT"]}
  def REDIS_LINDEX(args)
    value = redis_lget(args)
    return value[args[1].to_i]
  end
  # @conv {"RPOPLPUSH" =>  ["SELECT","DELETE","UPDATE"]}
  def REDIS_RPOPLPUSH(args)
    value = REDIS_RPOP(args)
    REDIS_LPUSH(args)
    return value
  end
  # @conv {"LSET" => ["UPDATE"]}
  def REDIS_LSET(args)
    index = args[1]
    value = args[2]
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    command = "UPDATE #{table} SET value[#{index}] = '#{value}'"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      result = false
    end
    return result
  end
  # @conv {"LTRIM" => ["SELECT","INSERT"]
  def REDIS_LTRIM(args)
    newData = redis_lget(args) - REDIS_LRANGE(args)
    return redis_lreset(args,newData)
  end
  # @conv {"LLEN" => ["SELECT","length@client"]}
  def REDIS_LLEN(args)
    list = redis_lget(args)
    return list.size
  end
  def redis_lreset(args,value)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    command = "UPDATE #{table} SET value = ['#{value.join("','")}']"
    command += " WHERE key = '#{args[0]}'"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  def redis_lget(args)
    ## tablename = "list"
    table = @options[:keyspace]+".list"
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    data = []
    begin
      result = DIRECT_EXECUTER(command)
      data = eval(result)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
    end
    return data
  end

  #########
  ## Set ##
  #########
  # @conv {"SADD" => ["SELECT","INSERT"]}
  def REDIS_SADD(args)
    table = @options[:keyspace]+".array"
    array = REDIS_SMEMBERS(args)
    if(args[1].class == Array)then
      args[1].each{|e| array.push(e)}
    else
      array.push(args[1])
    end
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  # @conv {"SREM" => ["SELECT","INSERT"]}
  def REDIS_SREM(args)
    table = @options[:keyspace]+".array"
    array = REDIS_SMEMBERS(args)
    array.delete(args[1])
    begin
      command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    return true
  end

  # @conv {"SMEMBERS" => ["SELECT"]}
  def REDIS_SMEMBERS(args)
    ## tablename = "array"
    table = @options[:keyspace]+".array"
    command = "SELECT value FROM #{table}"
    value = []
    begin
      values = DIRECT_SELECT(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      values = []
    end
    if(values.size > 0)then
      rows = values.split("\n")
      rows.each{|row|
        key   = row.split(",")[0]
        if(key == args[0])then
          value = row.sub("#{key},","").sub("{","").sub("}","").gsub("'","").split(",")
          return value
        end
      }
    end
    return value
  end
  # @conv {"SISMEMBER" => ["SELECT"]}
  def REDIS_SISMEMBER(args)
    array = REDIS_SMEMBERS(args)
    return array.include?(args[1])
  end

  # @conv {"SRANDMEMBER" => ["SELECT"]}
  def REDIS_SRANDMEMBER(args)
    array = REDIS_SMEMBERS(args)
    return array.sample
  end 
  
  # @conv {"SPOP" => ["SELECT","INSERT"]}
  def REDIS_SPOP(args)
    table = @options[:keyspace]+".array"
    array = REDIS_SMEMBERS(args)
    array.pop
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  
  # @conv {"SMOVE" => ["SELECT"]}
  def REDIS_SMOVE(args)
    srcKey = args[0]
    dstKey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    REDIS_SREM([srcKey,member])
    ## ADD member to dstKey
    return REDIS_SADD([dstKey,member])
  end

  # @conv {"SCARD" => ["SELECT"]}
  def REDIS_SCARD(args)
    return REDIS_SMEMBERS(args).size
  end

  # @conv {"SDIFF" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SDIFF(args)
    common = []
    membersArray = []
    args.each{|key|
      members = REDIS_SMEMBERS([key])
      membersArray += members
      if(common.size == 0)then
        common = members
      else
        common = common & members
      end
    }
    value = membersArray.uniq - common.uniq
    return value
  end
  # @conv {"SDIFFSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SDIFFSTORE(args)
    dstKey = args.shift
    value = REDIS_SDIFF(args)
    return REDIS_SADD([dstKey,value])
  end
  # @conv {"SINTER" => ["SELECT"]}
  def REDIS_SINTER(args)
    result = []
    args.each{|key|
      members = REDIS_SMEMBERS([key])
      if(result.size == 0)then
        result = members
      else
        result = result & members
      end
    }
    return result
  end
  # @conv {"SINTERSTORE" => ["SELECT","INSERT"]}
  def REDIS_SINTERSTORE(args)
   result = []
    dstKey = args.shift
    result = REDIS_SINTER(args)
    REDIS_SADD([dstKey,result])
  end
  # @conv {"SUNION" => ["SELECT"]}
  def REDIS_SUNION(args)
    value = []
    args.each{|key|
      members = REDIS_SMEMBERS([key])
      value += members
    }
    return value
  end
  # @conv {"SUNION" => ["SELECT","INSERT"]}
  def REDIS_SUNIONSTORE(args)
    result = []
    dstKey = args.shift
    result = REDIS_SUNION(args)
    return REDIS_SADD([dstKey,result])
  end

  #################
  ## Sorted Sets ##
  #################
  # @conv {"ZADD" => ["UPDATE"]}
  ## args = [key, score, member]
  def REDIS_ZADD(args,hash=nil)
    ## hash { member => score}
    ## tablename = "sarray"
    table = @options[:keyspace]+".sarray"
    command = "UPDATE #{table} SET value = "
    if(hash)then
      command += hash.to_json.gsub('"',"'")
    else
      command += "value + {'#{args[2]}':#{args[1].to_f}}"
    end
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  # @conv {"ZREM" => ["DELETE"]}
  ## args = [key, member]
  def REDIS_ZREM(args)
    ## tablename = "sarray"
    table = @options[:keyspace]+".sarray"
    command  = "DELETE value['#{args[1]}'] FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    return true
  end
  # @conv {"ZINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, score, member]
  def REDIS_ZINCRBY(args)
    value = redis_zget(args)
    value[args[2].to_sym] += args[1].to_f
    return REDIS_ZADD([args[0],value[args[2].to_sym],args[2]])
  end
  # @conv {"ZRANK" => ["SELECT"]}
  ## args = [key, member]
  def REDIS_ZRANK(args)
    value = redis_zget(args)
    data = Hash[value.sort_by{|_,v| v}]
    return data.keys().find_index(args[1].to_sym) + 1
  end
  # @conv {"ZREVRANK" => ["SELECT"]}
  ## args = [key, member]
  def REDIS_ZREVRANK(args)
     value = redis_zget(args)
    data = Hash[value.sort_by{|_,v| -v}]
    return data.keys().find_index(args[1].to_sym) + 1
  end
  # @conv {"ZRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def REDIS_ZRANGE(args)
    first = args[1].to_i - 1
    last  = args[2].to_i - 1
    value = redis_zget(args)
    data = Hash[value.sort_by{|_,v| v}].keys()
    if(last < 0)then
      last = data.size
    end
    result = []
    data.each_index{|index|
      if(index >= first and index <= last)then
        result.push(data[index].to_s)
      end
    }
    return result
  end
  # @conv {"ZREVRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def REDIS_ZREVRANGE(args)
    first = args[1].to_i - 1
    last  = args[2].to_i - 1 
    value = redis_zget(args)
    data = Hash[value.sort_by{|_,v| -v}].keys()
    result = []
    if(last < 0)then
      last = data.size
    end
    data.each_index{|index|
      if(index >= first and index <= last)then
        result.push(data[index].to_s)
      end
    }
    return result
  end
  
   # @conv {"ZRANGEBYSCORE" => ["SELECT"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    min = args[1].to_i 
    max = args[2].to_i
    value = redis_zget(args)
    result = []
    v = value.sort_by{|_,v| v}
    v.each{|member,score|
      if(score.to_i >= min and score.to_i <= max)then
        result.push(member.to_s)
      end
    }
    return result
  end
  # @conv {"ZCOUNT" => ["SELECT","count@client"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    selected = REDIS_ZRANGEBYSCORE(args)
    return selected.size
  end
  # @conv {"ZCARD" => ["SELECT","count@client"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    selected = redis_zget(args)
    return selected.keys.size
  end
  # @conv {"ZSCORE" => ["SELECT"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    value = redis_zget(args)
    return value[args[1].to_sym]
  end
   # @conv {"ZREMRANGEBYSCORE" => ["SELECT","UPDATE"]}
   ## args = [key,min,max]
   def REDIS_ZREMRANGEBYSCORE(args)
     all   = redis_zget(args)
     remKey = REDIS_ZRANGEBYSCORE(args)
     remKey.each{|rkey|
       all.delete(rkey.to_sym)
     }
     return REDIS_ZADD(args, all)
   end
   # @conv {"ZREMRANGEBYRANK" => ["SELECT","UPDATE"]}
   ## args = [key,min,max]
   def REDIS_ZREMRANGEBYRANK(args)
     all  = redis_zget(args)
     min  = args[1].to_i - 1
     max  = args[2].to_i - 1
     if(max < 0)then
       max = all.keys.size - 1
     end
     keys = all.keys
     keys.each_index{|index|
       key = keys[index]
       if(min <= index and max >= index)then
         all.delete(keys[index].to_sym)
       end
     }
     return REDIS_ZADD(args, all)
   end
   # @conv {"ZUNIONSTORE" => ["SELECT","UPDATE"]}
   ## args {"key"      => dstKey, 
   ##       "args"     => [srcKey0,srcKey1,...], 
   ##       "options"  => {:weights => [1,2,...],:aggregete => SUM/MAX/MIN}
   def REDIS_ZUNIONSTORE(args)
     data = {} ## value => score
     args["args"].each_index{|index|
       result = redis_zget([args["args"][index]])
       result.each{|hash|
         value = hash[0]
         score = hash[1]
         if(!data[value])then
           data[value] = []
         end
         weight = 1
         if(args["options"] and 
             args["options"][:weights] and
             args["options"][:weights][index])then
           weight = args["options"][:weights][index].to_i
         end
         data[value].push(score.to_f * weight)
       }
     }
     ## UNION
     if(data.keys.size > 0)then
       aggregate = "SUM"
       if(args["options"] and 
           args["options"][:aggregate])then
         aggregate = args["options"][:aggregate].upcase
       end
       hash = createDocWithAggregate(data,aggregate)
       return REDIS_ZADD([args["key"]], hash)
     end
     return false
   end
   # @conv {"ZINTERSTORE" => ["SELECT","UPDATE"]}
   ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
   def REDIS_ZINTERSTORE(args)
    data = {} ## value => score
    args["args"].each_index{|index|
      redis_zget([args["args"][index]]).each{|hash|
         value = hash[0]
         score = hash[1]
        if(!data[value] and index == 0)then
          data[value] = []
        end
        if(data[value])then
          weight = 1
          if(args["options"] and 
              args["options"][:weights] and
              args["options"][:weights][index])then
            weight = args["options"][:weights][index].to_i
          end
          data[value].push(score.to_f * weight)
        end
      }
    }
    if(data.keys.size > 0)then
      ## UNION
      aggregate = "SUM"
      if(args["options"] and 
          args["options"][:aggregate])then
        aggregate = args["options"][:aggregate].upcase
      end
      hash = createDocWithAggregate(data,aggregate)
      return REDIS_ZADD([args["key"]], hash)
    end
     return false
  end

   def redis_zget(args)
     ## tablename = "sarray"
    table = @options[:keyspace]+".sarray"
    command  = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = DIRECT_SELECT(command)
      if(result)then
        return eval(result)
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    return {}
  end
  def createDocWithAggregate(data,aggregate)
    doc = {}
    case aggregate
    when "SUM" then
      data.each_key{|key|
        doc[key] = data[key].inject(:+)
      }
    when "MAX" then
      data.each_key{|key|
        doc[key] = data[key].max
      }
    when "MIN" then
      data.each_key{|key|
        doc[key] = data[key].min
      }
    else
      @logger.error("Unsupported Aggregating Operation #{aggregate}")
    end
    return doc
  end
  ############
  ## Hashes ##
  ############
  # @conv {"HSET" => ["UPDATE"]}
  ## args = [key, field, value]
  def REDIS_HSET(args,hash=nil)
    ## hash {field => value}
    ## tablename = "hash"
    table = @options[:keyspace]+".hash"
    command  = "UPDATE #{table} SET value = "
    if(hash)then
      command += "value + " + hash.to_json.gsub('"',"'")
    else
      command += "value + {'#{args[1]}':'#{args[2]}'}"
    end
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end    
    return true
  end
  # @conv {"HGET" => ["SELECT"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    ## tablename = "hash"
    table = @options[:keyspace]+".hash"
    command  = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    begin
      value = DIRECT_EXECUTER(command)
      data = eval(value)
      if(args[1])then
        return data[args[1].to_sym]
      end
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return nil
    end
  end
  # @conv {"HMGET" => ["SELECT"]}
  ## args = {"key" => key, "args"=> [field0,field1,...]]
  def REDIS_HMGET(args)
    ## tablename = "hash"
    table = @options[:keyspace]+".hash"
    command  = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args["key"]}';"
    result = []
    begin
      value = DIRECT_EXECUTER(command)
      data = eval(value)
      if(args["args"])then
        args["args"].each{|field|
          result.push(data[field.to_sym])
        }
      end
      return result
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return []
    end
  end
  # @conv {"HMSET" => ["UPDATE"]}
  ## args = {"key" => key, "args"=> {field0=>member0,field1=>member1,...}}
  def REDIS_HMSET(args)
    REDIS_HSET([args["key"]],args["args"])
  end
  # @conv {"HINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, field, integer]
  def REDIS_HINCRBY(args)
    hash = REDIS_HGET(args)
    value =  hash.to_i + args[2].to_i
    return REDIS_HSET([args[0],args[1],value])
  end
  # @conv {"HEXISTS" => ["SELECT"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    value = REDIS_HGET(args)
    if(value)then
      return  true
    end
    return false
  end
  # @conv {"HDEL" => ["DELETE"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    ## tablename = "hash"
    table = @options[:keyspace]+".hash"
    command  = "DELETE value['#{args[1]}'] FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end    
    return true
  end
  # @conv {"HLEN" => ["SELECT"]}
  ## args = [key]
  def REDIS_HLEN(args)
    return @schemas[args[0]].fields.size
  end
  # @conv {"HKEYS" => ["SELECT"]
  ## args = [key]
  def REDIS_HKEYS(args)
    return @schemas[args[0]].fields
  end
  # @conv {"HVALS" => ["SELECT"]}
  ## args = [key]
  def REDIS_HVALS(args)
    hash = REDIS_HGETALL({"key"=>args[0]})
    return hash.values
  end
  # @conv {"HGETALL" => ["SELECT"]}
  ## args = [key]
  def REDIS_HGETALL(args)
    hash = {}
    keys = REDIS_HKEYS([args["key"]])
    args["args"] = keys
    values = REDIS_HMGET(args)
    keys.each_index{|index|
      hash[keys[index]] = values[index]
    }
    return hash 
  end
  
  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["reset@client"]}
  def REDIS_FLUSHALL
    queries = []
    queries.push("drop keyspace if exists #{@options[:keyspace]};")
    queries.push("create keyspace #{@options[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3};")
    @schemas.each{|k,s|
      queries.push(s.createQuery)
    }
    queries.each{|query|
      begin
        DIRECT_EXECUTER(query)
      rescue => e
        @logger.error(query)
        @logger.error(e.message)
        return false
      end
    }
    return true
  end
  
  #############
  ## PREPARE ##
  #############
  def prepare_redis(operand,args)
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
end

