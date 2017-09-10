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
  def REDIS_SET(args, cond = {}, onTime = false)
    table = @options[:keyspace] + "." + @options[:columnfamily]
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}','#{args[1]}')"
    r = true
    if cond["ttl"]
      command += " USING TTL #{cond["ttl"]}"
    end
    begin
      DIRECT_EXECUTER(command + ";", onTime)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      r = false
    end
    r
  end

  # @conv {"GET" => ["SELECT"]}
  def REDIS_GET(args, onTime = false)
    table = @options[:keyspace] + "." + @options[:columnfamily]
    command = "SELECT value FROM #{table}"
    unless args.empty?
      command += " WHERE key = '#{args[0]}' ;"
    end
    begin
      value = DIRECT_EXECUTER(command, onTime)
      if value["value"] && value["value"][0]
        return value["value"][0]
      else
        return ""
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    ""
  end

  # @conv {"SETNX" => ["INSERT","SELECT"]}
  def REDIS_SETNX(args)
    if REDIS_GET(args, false) == ""
      return REDIS_SET(args)
    end
    false
  end

  # @conv {"SETEX" => ["INSERT"]}
  def REDIS_SETEX(args)
    args[2] = (args[2].to_f / 1_000).to_i + 1
    hash = { "ttl" => args[2] }
    REDIS_SET(args, hash)
  end

  # @conv {"PSETEX" => ["INSERT"]}
  def REDIS_PSETEX(args)
    args[2] = (args[2].to_f / 1_000_000).to_i + 1
    hash = { "ttl" => args[2] }
    REDIS_SET(args, hash)
  end

  # @conv {"MSET" => ["INSERT"]}
  def REDIS_MSET(args)
    args.each do |key, value|
      r = REDIS_SET([key, value])
      unless r
        return false
      end
    end
    true
  end

  # @conv {"MGET" => ["SELECT"]}
  def REDIS_MGET(args)
    result = []
    args.each do |key, _|
      result.push(REDIS_GET([key], false))
    end
    result
  end

  # @conv {"MSETNX" => ["INSERT"]}
  def REDIS_MSETNX(args)
    args.each do |key, value|
      r = REDIS_SETNX([key, value])
      unless r
        return false
      end
    end
    true
  end

  # @conv {"INCR" => ["SELECT","INSERT"]}
  def REDIS_INCR(args)
    value = REDIS_GET([args[0]], false).to_i + 1
    REDIS_SET([args[0], value])
  end

  # @conv {"INCRBY" => ["SELECT","INSERT"]}
  def REDIS_INCRBY(args)
    value = REDIS_GET([args[0]], false).to_i + args[1].to_i
    REDIS_SET([args[0], value])
  end

  # @conv {"DECR" => ["SELECT","INSERT"]}
  def REDIS_DECR(args)
    value = REDIS_GET([args[0]], false).to_i - 1
    REDIS_SET([args[0], value])
  end

  # @conv {"DECRBY" => ["SELECT","INSERT"]}
  def REDIS_DECRBY(args)
    value = REDIS_GET([args[0]], false).to_i - args[1].to_i
    REDIS_SET([args[0], value])
  end

  # @conv {"APPEND" => ["SELECT","INSERT"]}
  def REDIS_APPEND(args)
    value = REDIS_GET([args[0]], false).to_s + args[1]
    REDIS_SET([args[0], value])
  end

  # @conv {"GETSET" => ["SELECT","INSERT"]}
  def REDIS_GETSET(args)
    val = REDIS_GET([args[0]])
    REDIS_SET(args)
    val
  end

  # @conv {"STRLEN" => ["SELECT","LENGTH@client"]}
  def REDIS_STRLEN(args)
    REDIS_GET([args[0]], false).size
  end

  # @conv {"DEL" => ["DELETE"]}
  def REDIS_DEL(args, onTime = false)
    table = @options[:keyspace] + "." + @options[:columnfamily]
    command = "DELETE FROM #{table} WHERE key = '#{args[0]}';"
    r = true
    begin
      DIRECT_EXECUTER(command, onTime)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      r = false
    end
    r
  end

  ###########
  ## Lists ##
  ###########
  # @conv {"LPUSH" => [UPDATE"]}
  def REDIS_LPUSH(args)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
    command = "UPDATE #{table} SET value = ['#{args[1]}'] + value WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"RPUSH" => ["UPDATE"]}
  def REDIS_RPUSH(args)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
    command = "UPDATE #{table} SET value = value + ['#{args[1]}'] WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"LPOP" => ["SELECT","DELETE"]}
  def REDIS_LPOP(args)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
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
    values.first
  end

  # @conv {"RPOP" => ["SELECT","DELETE"]}
  def REDIS_RPOP(args, stdout = false)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
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
    if stdout
      @logger.info(values)
    end
    values.last
  end

  # @conv {"LRANGE" => ["SELECT"]}
  def REDIS_LRANGE(args)
    list = redis_lget(args)
    first = update_first(args[1].to_i)
    last = update_last(args[2].to_i, list)
    result = []
    list.each_index do |index|
      if index >= first && index <= last
        result.push(list[index])
      end
    end
    result
  end

  def update_first(first)
    if first < 0
      first = 0
    elsif first > 0
      first -= 1
    end
    first
  end

  def update_last(last, list)
    if last < 0
      last = list.size - 1
    elsif last > 0
      last -= 1
    end
    last
  end

  # @conv {"LREM" => ["UPDATE","SELECT"]}
  def REDIS_LREM(args)
    count = args[1].to_i
    value = args[2]
    values = redis_lget(args)
    if count.zero?
      values.delete(value)
      return redis_lreset(args, values)
    elsif count > 0
      return lrem_count_more_than_zero(count, args, value, values)
    elsif count < 0
      return lrem_count_less_than_zero(count, args, value, values)
    end
  end

  def lrem_count_more_than_zero(count, args, value, values)
    new_values = []
    values.each_index do |index|
      if values[index] == value && count > 0
        count -= 1
      else
        new_values.push(values[index])
      end
    end
    redis_lreset(args, new_values)
  end

  def lrem_count_less_than_zero(count, args, value, values)
    count *= -1
    new_values = []
    values.reverse!
    values.each_index do |index|
      if values[index] == value && count > 0
        count -= 1
      else
        new_values.push(values[index])
      end
    end
    new_values.reverse!
    redis_lreset(args, new_values)
  end

  # @conv {"LINDEX" => ["SELECT"]}
  def REDIS_LINDEX(args)
    value = redis_lget(args)
    value[args[1].to_i]
  end

  # @conv {"RPOPLPUSH" =>  ["SELECT","DELETE","UPDATE"]}
  def REDIS_RPOPLPUSH(args)
    value = REDIS_RPOP(args)
    REDIS_LPUSH(args)
    value
  end

  # @conv {"LSET" => ["UPDATE"]}
  def REDIS_LSET(args)
    index = args[1]
    value = args[2]
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
    command = "UPDATE #{table} SET value[#{index}] = '#{value}'"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      result = false
    end
    result
  end

  # @conv {"LTRIM" => ["SELECT","INSERT"]
  def REDIS_LTRIM(args)
    new_data = redis_lget(args) - REDIS_LRANGE(args)
    redis_lreset(args, new_data)
  end

  # @conv {"LLEN" => ["SELECT","length@client"]}
  def REDIS_LLEN(args)
    list = redis_lget(args)
    list.size
  end

  def redis_lreset(args, value)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
    command = "UPDATE #{table} SET value = ['#{value.join("','")}']"
    command += " WHERE key = '#{args[0]}'"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  def redis_lget(args)
    ## tablename = "list"
    table = @options[:keyspace] + ".list"
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    data = []
    begin
      result = DIRECT_EXECUTER(command)
      data = eval(result)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
    end
    data
  end

  #########
  ## Set ##
  #########
  # @conv {"SADD" => ["SELECT","INSERT"]}
  def REDIS_SADD(args)
    table = @options[:keyspace] + ".array"
    array = REDIS_SMEMBERS(args)
    if args[1].class == Array
      args[1].each do |e|
        array.push(e)
      end
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
    true
  end

  # @conv {"SREM" => ["SELECT","INSERT"]}
  def REDIS_SREM(args)
    table = @options[:keyspace] + ".array"
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
    true
  end

  # @conv {"SMEMBERS" => ["SELECT"]}
  def REDIS_SMEMBERS(args)
    ## tablename = "array"
    table = @options[:keyspace] + ".array"
    command = "SELECT value FROM #{table}"
    value = []
    begin
      values = DIRECT_SELECT(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      values = []
    end
    unless values.empty?
      rows = values.split("\n")
      rows.each do |row|
        key = row.split(",")[0]
        if key == args[0]
          value = row.sub("#{key},", "").sub("{", "").sub("}", "").delete("'").split(",")
          return value
        end
      end
    end
    value
  end

  # @conv {"SISMEMBER" => ["SELECT"]}
  def REDIS_SISMEMBER(args)
    array = REDIS_SMEMBERS(args)
    array.include?(args[1])
  end

  # @conv {"SRANDMEMBER" => ["SELECT"]}
  def REDIS_SRANDMEMBER(args)
    array = REDIS_SMEMBERS(args)
    array.sample
  end

  # @conv {"SPOP" => ["SELECT","INSERT"]}
  def REDIS_SPOP(args)
    table = @options[:keyspace] + ".array"
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
    true
  end

  # @conv {"SMOVE" => ["SELECT"]}
  def REDIS_SMOVE(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    REDIS_SREM([srckey, member])
    ## ADD member to dstKey
    REDIS_SADD([dstkey, member])
  end

  # @conv {"SCARD" => ["SELECT"]}
  def REDIS_SCARD(args)
    REDIS_SMEMBERS(args).size
  end

  # @conv {"SDIFF" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def REDIS_SDIFF(args)
    common = []
    members_array = []
    args.each do |key|
      members = REDIS_SMEMBERS([key])
      members_array += members
      if common.size.zero?
        common = members
      else
        common &= members
      end
    end
    value = members_array.uniq - common.uniq
    value
  end

  # @conv {"SDIFFSTORE" => ["redisDeserialize@client","redisSerialize@client","GET","SET"]}
  def REDIS_SDIFFSTORE(args)
    dstkey = args.shift
    value = REDIS_SDIFF(args)
    REDIS_SADD([dstkey, value])
  end

  # @conv {"SINTER" => ["SELECT"]}
  def REDIS_SINTER(args)
    result = []
    args.each do |key|
      members = REDIS_SMEMBERS([key])
      result = if result.size.zero?
                 members
               else
                 result & members
               end
    end
    result
  end

  # @conv {"SINTERSTORE" => ["SELECT","INSERT"]}
  def REDIS_SINTERSTORE(args)
    dstkey = args.shift
    result = REDIS_SINTER(args)
    REDIS_SADD([dstkey, result])
  end

  # @conv {"SUNION" => ["SELECT"]}
  def REDIS_SUNION(args)
    value = []
    args.each do |key|
      members = REDIS_SMEMBERS([key])
      value += members
    end
    value
  end

  # @conv {"SUNION" => ["SELECT","INSERT"]}
  def REDIS_SUNIONSTORE(args)
    dstkey = args.shift
    result = REDIS_SUNION(args)
    REDIS_SADD([dstkey, result])
  end

  #################
  ## Sorted Sets ##
  #################
  # @conv {"ZADD" => ["UPDATE"]}
  ## args = [key, score, member]
  def REDIS_ZADD(args, hash = nil)
    ## hash { member => score}
    ## tablename = "sarray"
    table = @options[:keyspace] + ".sarray"
    command = "UPDATE #{table} SET value = "
    command += if hash
                 hash.to_json.tr('"', "'")
               else
                 "value + {'#{args[2]}':#{args[1].to_f}}"
               end
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"ZREM" => ["DELETE"]}
  ## args = [key, member]
  def REDIS_ZREM(args)
    ## tablename = "sarray"
    table = @options[:keyspace] + ".sarray"
    command = "DELETE value['#{args[1]}'] FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"ZINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, score, member]
  def REDIS_ZINCRBY(args)
    value = redis_zget(args)
    value[args[2].to_sym] += args[1].to_f
    REDIS_ZADD([args[0], value[args[2].to_sym], args[2]])
  end

  # @conv {"ZRANK" => ["SELECT"]}
  ## args = [key, member]
  def REDIS_ZRANK(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| v }]
    data.keys.find_index(args[1].to_sym) + 1
  end

  # @conv {"ZREVRANK" => ["SELECT"]}
  ## args = [key, member]
  def REDIS_ZREVRANK(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| -v }]
    data.keys.find_index(args[1].to_sym) + 1
  end

  # @conv {"ZRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def REDIS_ZRANGE(args)
    first = args[1].to_i - 1
    last  = args[2].to_i - 1
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| v }].keys
    if last < 0
      last = data.size
    end
    result = []
    data.each_index do |index|
      if index >= first && index <= last
        result.push(data[index].to_s)
      end
    end
    result
  end

  # @conv {"ZREVRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def REDIS_ZREVRANGE(args)
    first = args[1].to_i - 1
    last  = args[2].to_i - 1
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| -v }].keys
    result = []
    if last < 0
      last = data.size
    end
    data.each_index do |index|
      if index >= first && index <= last
        result.push(data[index].to_s)
      end
    end
    result
  end

  # @conv {"ZRANGEBYSCORE" => ["SELECT"]}
  ## args = [key, min, max]
  def REDIS_ZRANGEBYSCORE(args)
    min = args[1].to_i
    max = args[2].to_i
    value = redis_zget(args)
    result = []
    v = value.sort_by { |_, v2| v2 }
    v.each do |member, score|
      if score.to_i >= min && score.to_i <= max
        result.push(member.to_s)
      end
    end
    result
  end

  # @conv {"ZCOUNT" => ["SELECT","count@client"]}
  ## args = [key, min, max]
  def REDIS_ZCOUNT(args)
    selected = REDIS_ZRANGEBYSCORE(args)
    selected.size
  end

  # @conv {"ZCARD" => ["SELECT","count@client"]}
  ## args = [key]
  def REDIS_ZCARD(args)
    selected = redis_zget(args)
    selected.keys.size
  end

  # @conv {"ZSCORE" => ["SELECT"]}
  ## args = [key,member]
  def REDIS_ZSCORE(args)
    value = redis_zget(args)
    value[args[1].to_sym]
  end

  # @conv {"ZREMRANGEBYSCORE" => ["SELECT","UPDATE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYSCORE(args)
    all = redis_zget(args)
    remKey = REDIS_ZRANGEBYSCORE(args)
    remKey.each do |rkey|
      all.delete(rkey.to_sym)
    end
    REDIS_ZADD(args, all)
  end

  # @conv {"ZREMRANGEBYRANK" => ["SELECT","UPDATE"]}
  ## args = [key,min,max]
  def REDIS_ZREMRANGEBYRANK(args)
    all = redis_zget(args)
    min = args[1].to_i - 1
    max = args[2].to_i - 1
    if max < 0
      max = all.keys.size - 1
    end
    keys = all.keys
    keys.each_index do |index|
      if min <= index && max >= index
        all.delete(keys[index].to_sym)
      end
    end
    REDIS_ZADD(args, all)
  end

  # @conv {"ZUNIONSTORE" => ["SELECT","UPDATE"]}
  ## args {"key"      => dstKey,
  ##       "args"     => [srcKey0,srcKey1,...],
  ##       "options"  => {:weights => [1,2,...],:aggregete => SUM/MAX/MIN}
  def REDIS_ZUNIONSTORE(args)
    data = {} ## value => score
    args["args"].each_index do |index|
      result = redis_zget([args["args"][index]])
      result.each do |hash|
        value = hash[0]
        score = hash[1]
        unless data[value]
          data[value] = []
        end
        weight = 1
        if check_options(args, index, "weights")
          weight = args["options"][:weights][index].to_i
        end
        data[value].push(score.to_f * weight)
      end
    end
    ## UNION
    unless data.keys.empty?
      aggregate = "SUM"
      if check_options(args, nil, "aggregate")
        aggregate = args["options"][:aggregate].upcase
      end
      hash = createDocWithAggregate(data, aggregate)
      return REDIS_ZADD([args["key"]], hash)
    end
    false
  end

  # @conv {"ZINTERSTORE" => ["SELECT","UPDATE"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "options" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def REDIS_ZINTERSTORE(args)
    data = create_zinterstore(args)
    unless data.keys.empty?
      ## UNION
      aggregate = "SUM"
      if check_options(args, nil, "aggregate")
        aggregate = args["options"][:aggregate].upcase
      end
      hash = createDocWithAggregate(data, aggregate)
      return REDIS_ZADD([args["key"]], hash)
    end
    false
  end

  def check_options(args, index, condition)
    case condition
    when "weights"
      return args["options"] &&
             args["options"][:weights] &&
             args["options"][:weights][index]
    when "aggregate"
      return args["options"] && args["options"][:aggregate]
    end
    false
  end

  def create_zinterstore(args)
    data = {} ## value => score
    args["args"].each_index do |index|
      redis_zget([args["args"][index]]).each do |hash|
        value = hash[0]
        score = hash[1]
        if !data[value] && index.zero?
          data[value] = []
        end
        if data[value]
          weight = 1
          if check_options(args, index, "weights")
            weight = args["options"][:weights][index].to_i
          end
          data[value].push(score.to_f * weight)
        end
      end
    end
    data
  end

  def redis_zget(args)
    ## tablename = "sarray"
    table = @options[:keyspace] + ".sarray"
    command = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = DIRECT_SELECT(command)
      if result
        return eval(result)
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    {}
  end

  def createDocWithAggregate(data, aggregate)
    doc = {}
    case aggregate
    when "SUM" then
      data.each_key do |key|
        doc[key] = data[key].inject(:+)
      end
    when "MAX" then
      data.each_key do |key|
        doc[key] = data[key].max
      end
    when "MIN" then
      data.each_key do |key|
        doc[key] = data[key].min
      end
    else
      @logger.error("Unsupported Aggregating Operation #{aggregate}")
    end
    doc
  end

  ############
  ## Hashes ##
  ############
  # @conv {"HSET" => ["UPDATE"]}
  ## args = [key, field, value]
  def REDIS_HSET(args, hash = nil)
    ## hash {field => value}
    ## tablename = "hash"
    table = @options[:keyspace] + ".hash"
    command = "UPDATE #{table} SET value = "
    command += if !hash.nil?
                 "value + " + hash.to_json.tr('"', "'")
               else
                 "value + {'#{args[1]}':'#{args[2]}'}"
               end
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"HGET" => ["SELECT"]}
  ## args = [key, field]
  def REDIS_HGET(args)
    ## tablename = "hash"
    table = @options[:keyspace] + ".hash"
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    begin
      value = DIRECT_EXECUTER(command)
      data = eval(value)
      if args[1]
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
    table = @options[:keyspace] + ".hash"
    command = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args["key"]}';"
    result = []
    begin
      value = DIRECT_EXECUTER(command)
      data = eval(value)
      if args["args"]
        args["args"].each do |field|
          result.push(data[field.to_sym])
        end
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
    REDIS_HSET([args["key"]], args["args"])
  end

  # @conv {"HINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, field, integer]
  def REDIS_HINCRBY(args)
    hash = REDIS_HGET(args)
    value = hash.to_i + args[2].to_i
    REDIS_HSET([args[0], args[1], value])
  end

  # @conv {"HEXISTS" => ["SELECT"]}
  ## args = [key, field]
  def REDIS_HEXISTS(args)
    if REDIS_HGET(args)
      return true
    end
    false
  end

  # @conv {"HDEL" => ["DELETE"]}
  ## args = [key, field]
  def REDIS_HDEL(args)
    ## tablename = "hash"
    table = @options[:keyspace] + ".hash"
    command = "DELETE value['#{args[1]}'] FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"HLEN" => ["SELECT"]}
  ## args = [key]
  def REDIS_HLEN(args)
    @schemas[args[0]].fields.size
  end

  # @conv {"HKEYS" => ["SELECT"]
  ## args = [key]
  def REDIS_HKEYS(args)
    @schemas[args[0]].fields
  end

  # @conv {"HVALS" => ["SELECT"]}
  ## args = [key]
  def REDIS_HVALS(args)
    hash = REDIS_HGETALL("key" => args[0])
    hash.values
  end

  # @conv {"HGETALL" => ["SELECT"]}
  ## args = [key]
  def REDIS_HGETALL(args)
    hash = {}
    keys = REDIS_HKEYS([args["key"]])
    args["args"] = keys
    values = REDIS_HMGET(args)
    keys.each_index do |index|
      hash[keys[index]] = values[index]
    end
    hash
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["reset@client"]}
  def REDIS_FLUSHALL
    queries = []
    queries.push("drop keyspace if exists #{@options[:keyspace]};")
    queries.push("create keyspace #{@options[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3};")
    @schemas.each do |_, s|
      queries.push(s.createQuery)
    end
    queries.each do |query|
      begin
        DIRECT_EXECUTER(query)
      rescue => e
        @logger.error(query)
        @logger.error(e.message)
        return false
      end
    end
    true
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
end
