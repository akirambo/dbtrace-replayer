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
  def redis_set(args, cond = {}, onTime = false)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}','#{args[1]}')"
    r = true
    if cond["ttl"]
      command += " USING TTL #{cond["ttl"]}"
    end
    begin
      direct_executer(command + ";", onTime)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      r = false
    end
    r
  end

  # @conv {"GET" => ["SELECT"]}
  def redis_get(args, onTime = false)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "SELECT value FROM #{table}"
    unless args.empty?
      command += " WHERE key = '#{args[0]}' ;"
    end
    begin
      value = direct_executer(command, onTime)
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
  def redis_setnx(args)
    if redis_get(args, false) == ""
      return redis_set(args)
    end
    false
  end

  # @conv {"SETEX" => ["INSERT"]}
  def redis_setex(args)
    redis_xsetex(args, "s")
  end

  # @conv {"PSETEX" => ["INSERT"]}
  def redis_psetex(args)
    redis_xsetex(args, "p")
  end

  def redis_xsetex(args, type)
    value = if type == "p"
              1_000_000
            else
              1_000
            end
    args[2] = (args[2].to_f / value.to_f).to_i + 1
    hash = { "ttl" => args[2] }
    redis_set(args, hash)
  end

  # @conv {"MSET" => ["INSERT"]}
  def redis_mset(args)
    args.each do |key, value|
      r = redis_set([key, value])
      unless r
        return false
      end
    end
    true
  end

  # @conv {"MGET" => ["SELECT"]}
  def redis_mget(args)
    result = []
    args.each do |key, _|
      result.push(redis_get([key], false))
    end
    result
  end

  # @conv {"MSETNX" => ["INSERT"]}
  def redis_msetnx(args)
    args.each do |key, value|
      r = redis_setnx([key, value])
      unless r
        return false
      end
    end
    true
  end

  # @conv {"INCR" => ["SELECT","INSERT"]}
  def redis_incr(args)
    redis_incr_decr(args, "incr")
  end

  # @conv {"INCRBY" => ["SELECT","INSERT"]}
  def redis_incrby(args)
    redis_incr_decr(args, "incrby")
  end

  # @conv {"DECR" => ["SELECT","INSERT"]}
  def redis_decr(args)
    redis_incr_decr(args, "decr")
  end

  # @conv {"DECRBY" => ["SELECT","INSERT"]}
  def redis_decrby(args)
    redis_incr_decr(args, "decrby")
  end

  def redis_incr_decr(args, type)
    number = case type
             when "incr"
               1
             when "incrby"
               args[1].to_i
             when "decr"
               -1
             when "decrby"
               args[1].to_i * -1
             end
    value = redis_get([args[0]], false).to_i + number.to_i
    redis_set([args[0], value])
  end

  # @conv {"APPEND" => ["SELECT","INSERT"]}
  def redis_append(args)
    value = redis_get([args[0]], false).to_s + args[1]
    redis_set([args[0], value])
  end

  # @conv {"GETSET" => ["SELECT","INSERT"]}
  def redis_getset(args)
    val = redis_get([args[0]])
    redis_set(args)
    val
  end

  # @conv {"STRLEN" => ["SELECT","LENGTH@client"]}
  def redis_strlen(args)
    redis_get([args[0]], false).size
  end

  # @conv {"DEL" => ["DELETE"]}
  def redis_del(args, onTime = false)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "DELETE FROM #{table} WHERE key = '#{args[0]}';"
    r = true
    begin
      direct_executer(command, onTime)
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
  def redis_lpush(args)
    redis_push(args, "lpush")
  end
  
  # @conv {"RPUSH" => ["UPDATE"]}
  def redis_rpush(args)
    redis_push(args, "rpush")
  end

  def redis_push(args, type)
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    command = "UPDATE #{table} SET value = "
    command += if type == "lpush"
                 "['#{args[1]}'] + value "
               elsif type == "rpush"
                 "value + ['#{args[1]}'] "
               end
    command += "WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"LPOP" => ["SELECT","DELETE"]}
  def redis_lpop(args)
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    ### GET
    values = redis_lget(args)
    ### DELETE
    command = "DELETE value[0] FROM #{table} WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    values.first
  end

  # @conv {"RPOP" => ["SELECT","DELETE"]}
  def redis_rpop(args, stdout = false)
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    ### GET
    values = redis_lget(args)
    ### DELETE
    command = "DELETE value[#{values.size - 1}] FROM #{table} WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
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
  def redis_lrange(args)
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
  def redis_lrem(args)
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
    new_values = lrem_counter(count, value, values)
    redis_lreset(args, new_values)
  end

  def lrem_count_less_than_zero(count, args, value, values)
    count *= -1
    values.reverse!
    new_values = lrem_counter(count, value, values)
    new_values.reverse!
    redis_lreset(args, new_values)
  end

  def lrem_counter(count, value, values)
    new_values = []
    values.each_index do |index|
      if values[index] == value && count > 0
        count -= 1
      else
        new_values.push(values[index])
      end
    end
    new_values
  end

  # @conv {"lindex" => ["select"]}
  def redis_lindex(args)
    value = redis_lget(args)
    value[args[1].to_i]
  end

  # @conv {"RPOPLPUSH" =>  ["SELECT","DELETE","UPDATE"]}
  def redis_rpoplpush(args)
    value = redis_rpop(args)
    redis_lpush(args)
    value
  end

  # @conv {"LSET" => ["UPDATE"]}
  def redis_lset(args)
    index = args[1]
    value = args[2]
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    command = "UPDATE #{table} SET value[#{index}] = '#{value}'"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      result = false
    end
    result
  end

  # @conv {"LTRIM" => ["SELECT","INSERT"]
  def redis_ltrim(args)
    new_data = redis_lget(args) - redis_lrange(args)
    redis_lreset(args, new_data)
  end

  # @conv {"LLEN" => ["SELECT","length@client"]}
  def redis_llen(args)
    list = redis_lget(args)
    if list.nil?
      return 0
    end
    list.size
  end

  def redis_lreset(args, value)
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    command = "UPDATE #{table} SET value = ['#{value.join("','")}']"
    command += " WHERE key = '#{args[0]}'"
    begin
      direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  def redis_lget(args)
    ## tablename = "list"
    table = @option[:keyspace] + ".list"
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    data = []
    begin
      result = direct_executer(command)
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
  def redis_sadd(args)
    table = @option[:keyspace] + ".array"
    array = redis_smembers(args)
    if args[1].class == Array
      args[1].each do |e|
        array.push(e)
      end
    else
      array.push(args[1])
    end
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
    begin
      direct_executer(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"SREM" => ["SELECT","INSERT"]}
  def redis_srem(args)
    table = @option[:keyspace] + ".array"
    array = redis_smembers(args)
    array.delete(args[1])
    begin
      command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
      direct_executer(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"SMEMBERS" => ["SELECT"]}
  def redis_smembers(args)
    ## tablename = "array"
    table = @option[:keyspace] + ".array"
    command = "SELECT value FROM #{table}"
    value = []
    begin
      values = direct_select(command)
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

  # @conv {"SISMEMber" => ["select"]}
  def redis_sismember(args)
    array = redis_smembers(args)
    array.include?(args[1])
  end

  # @conv {"SRANDMEMBER" => ["SELECT"]}
  def redis_srandmember(args)
    array = redis_smembers(args)
    array.sample
  end

  # @conv {"SPOP" => ["SELECT","INSERT"]}
  def redis_spop(args)
    table = @option[:keyspace] + ".array"
    array = redis_smembers(args)
    array.pop
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}',{'#{array.join("','")}'})"
    begin
      direct_executer(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"SMOVE" => ["SELECT"]}
  def redis_smove(args)
    srckey = args[0]
    dstkey = args[1]
    member = args[2]
    ## REMOVE member from srtKey
    redis_srem([srckey, member])
    ## ADD member to dstKey
    redis_sadd([dstkey, member])
  end

  # @conv {"SCARD" => ["SELECT"]}
  def redis_scard(args)
    redis_smembers(args).size
  end

  # @conv {"SDIFF" => ["redisDeserialize@client","redisSerialize@client","GET"]}
  def redis_sdiff(args)
    common = []
    members_array = []
    args.each do |key|
      members = redis_smembers([key])
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
  def redis_sdiffstore(args)
    dstkey = args.shift
    value = redis_sdiff(args)
    redis_sadd([dstkey, value])
  end

  # @conv {"SINTER" => ["SELECT"]}
  def redis_sinter(args)
    result = []
    args.each do |key|
      members = redis_smembers([key])
      result = if result.size.zero?
                 members
               else
                 result & members
               end
    end
    result
  end

  # @conv {"SINTERSTORE" => ["SELECT","INSERT"]}
  def redis_sinterstore(args)
    dstkey = args.shift
    result = redis_sinter(args)
    redis_sadd([dstkey, result])
  end

  # @conv {"SUNION" => ["SELECT"]}
  def redis_sunion(args)
    value = []
    args.each do |key|
      members = redis_smembers([key])
      value += members
    end
    value
  end

  # @conv {"SUNION" => ["SELECT","INSERT"]}
  def redis_sunionstore(args)
    dstkey = args.shift
    result = redis_sunion(args)
    redis_sadd([dstkey, result])
  end

  #################
  ## Sorted Sets ##
  #################
  # @conv {"ZADD" => ["UPDATE"]}
  ## args = [key, score, member]
  def redis_zadd(args, hash = nil)
    ## hash { member => score}
    ## tablename = "sarray"
    table = @option[:keyspace] + ".sarray"
    command = "UPDATE #{table} SET value = "
    command += if hash
                 hash.to_json.tr('"', "'")
               else
                 "value + {'#{args[2]}':#{args[1].to_f}}"
               end
    command += " WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"ZREM" => ["DELETE"]}
  ## args = [key, member]
  def redis_zrem(args)
    ## tablename = "sarray"
    redis_hdel_zrem(args, ".sarray")
  end

  # @conv {"ZINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, score, member]
  def redis_zincrby(args)
    value = redis_zget(args)
    value[args[2].to_sym] = args[1].to_f + value[args[2].to_sym].to_f
    redis_zadd([args[0], value[args[2].to_sym], args[2]])
  end

  # @conv {"ZRANK" => ["SELECT"]}
  ## args = [key, member]
  def redis_zrank(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| v }]
    data.keys.find_index(args[1].to_sym) + 1
  end

  # @conv {"ZREVRANK" => ["SELECT"]}
  ## args = [key, member]
  def redis_zrevrank(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| -v }]
    data.keys.find_index(args[1].to_sym) + 1
  end

  # @conv {"ZRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def redis_zrange(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| v }].keys
    zrange(data, args)
  end

  # @conv {"ZREVRANGE" => ["SELECT","sort@client"]}
  ## args = [key, first, last]
  def redis_zrevrange(args)
    value = redis_zget(args)
    data = Hash[value.sort_by { |_, v| -v }].keys
    zrange(data, args)
  end

  def zrange(data, args)
    first = args[1].to_i - 1
    last  = args[2].to_i - 1
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
  def redis_zrangebyscore(args)
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
  def redis_zcount(args)
    selected = redis_zrangebyscore(args)
    selected.size
  end

  # @conv {"ZCARD" => ["SELECT","count@client"]}
  ## args = [key]
  def redis_zcard(args)
    selected = redis_zget(args)
    selected.keys.size
  end

  # @conv {"ZSCORE" => ["SELECT"]}
  ## args = [key,member]
  def redis_zscore(args)
    value = redis_zget(args)
    value[args[1].to_sym]
  end

  # @conv {"ZREMRANGEBYSCORE" => ["SELECT","UPDATE"]}
  ## args = [key,min,max]
  def redis_zremrangebyscore(args)
    all = redis_zget(args)
    remkey = redis_zrangebyscore(args)
    remkey.each do |rkey|
      all.delete(rkey.to_sym)
    end
     redis_zadd(args, all)
  end

  # @conv {"ZREMRANGEBYRANK" => ["SELECT","UPDATE"]}
  ## args = [key,min,max]
  def redis_zremrangebyrank(args)
    all = redis_zget(args)
    min = args[1].to_i - 1
    max = args[2].to_i - 1
    if max < 0
      max = all.keys.size - 1
    end
    keys = all.keys
    keys.each_index do |index|
      if index >= min && index <= max
        all.delete(keys[index].to_sym)
      end
    end
    redis_zadd(args, all)
  end

  # @conv {"ZUNIONSTORE" => ["SELECT","UPDATE"]}
  ## args {"key"      => dstKey,
  ##       "args"     => [srcKey0,srcKey1,...],
  ##       "option"  => {:weights => [1,2,...],:aggregete => SUM/MAX/MIN}
  def redis_zunionstore(args)
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
        if check_option(args, index, "weights")
          weight = args["option"][:weights][index].to_i
        end
        data[value].push(score.to_f * weight)
      end
    end
    ## UNION
    store_for_zunion(args, data)
  end

  # @conv {"ZINTERSTORE" => ["SELECT","UPDATE"]}
  ## args {"key" => dstKey, "args" => [srcKey0,srcKey1,...], "option" => {:weights => [1,2,...], :aggregete => SUM/MAX/MIN}
  def redis_zinterstore(args)
    data = create_zinterstore(args)
    store_for_zunion(args, data)
  end

  def store_for_zunion(args, data)
    unless data.keys.empty?
      aggregate = "SUM"
      if check_option(args, nil, "aggregate")
        aggregate = args["option"][:aggregate].upcase
      end
      hash = create_doc_with_aggregate(data, aggregate)
      return redis_zadd([args["key"]], hash)
    end
    false
  end

  def check_option(args, index, condition)
    case condition
    when "weights"
      return args["option"] &&
             args["option"][:weights] &&
             args["option"][:weights][index]
    when "aggregate"
      return args["option"] && args["option"][:aggregate]
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
          if check_option(args, index, "weights")
            weight = args["option"][:weights][index].to_i
          end
          data[value].push(score.to_f * weight)
        end
      end
    end
    data
  end

  def redis_zget(args)
    ## tablename = "sarray"
    table = @option[:keyspace] + ".sarray"
    command = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      result = direct_select(command)
      if result
        tmp = eval(result)
        ret = {}
        tmp.each do |k, v|
          ret[k] = v.to_f
        end
        return ret
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    {}
  end

  def create_doc_with_aggregate(data, aggregate)
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
  ## hashes ##
  ############
  # @conv {"HSET" => ["UPDATE"]}
  ## args = [key, field, value]
  def redis_hset(args, hash = nil)
    ## hash {field => value}
    ## tablename = "hash"
    table = @option[:keyspace] + ".hash"
    command = "UPDATE #{table} SET value = "
    command += if !hash.nil?
                 "value + " + hash.to_json.tr('"', "'")
               else
                 "value + {'#{args[1]}':'#{args[2]}'}"
               end
    command += " WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"HGET" => ["SELECT"]}
  ## args = [key, field]
  def redis_hget(args)
    ## tablename = "hash"
    table = @option[:keyspace] + ".hash"
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    begin
      value = direct_executer(command)
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
  def redis_hmget(args)
    ## tablename = "hash"
    table = @option[:keyspace] + ".hash"
    command = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args["key"]}';"
    result = []
    begin
      value = direct_executer(command)
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
  def redis_hmset(args)
    redis_hset([args["key"]], args["args"])
  end

  # @conv {"HINCRBY" => ["SELECT","UPDATE"]}
  ## args = [key, field, integer]
  def redis_hincrby(args)
    hash = redis_hget(args)
    value = hash.to_i + args[2].to_i
    redis_hset([args[0], args[1], value])
  end

  # @conv {"HEXISTS" => ["SELECT"]}
  ## args = [key, field]
  def redis_hexists(args)
    if redis_hget(args)
      return true
    end
    false
  end

  # @conv {"HDEL" => ["DELETE"]}
  ## args = [key, field]
  def redis_hdel(args)
    ## tablename = "hash"
    redis_hdel_zrem(args, ".hash")
  end

  def redis_hdel_zrem(args, type)
    table = @option[:keyspace] + type
    command = "DELETE value['#{args[1]}'] FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      direct_executer(command)
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      return false
    end
    true
  end

  # @conv {"HLEN" => ["SELECT"]}
  ## args = [key]
  def redis_hlen(args)
    redis_hkeys(args).size
  end

  # @conv {"HKEYS" => ["SELECT"]
  ## args = [key]
  def redis_hkeys(args)
    redis_hgetall(args).keys
  end

  # @conv {"HVALS" => ["SELECT"]}
  ## args = [key]
  def redis_hvals(args)
    redis_hgetall(args).values
  end

  # @conv {"HGETALL" => ["SELECT"]}
  ## args = [key]
  def redis_hgetall(args)
    hash = {}
    table = @option[:keyspace] + ".hash"
    command = "SELECT value FROM #{table}"
    command += " WHERE key = '#{args[0]}';"
    begin
      value = direct_executer(command)
      data = eval(value)
      data.each do |f, v|
        hash[f.to_s] = v
      end
    rescue => e
      @logger.debug(command)
      @logger.error(e.message)
      abort
    end
    hash
  end

  ############
  ## OTHRES ##
  ############
  # @conv {"FLUSHALL" => ["reset@client"]}
  def redis_flushall(_)
    queries = []
    queries.push("drop keyspace if exists #{@option[:keyspace]};")
    queries.push("create keyspace #{@option[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3};")
    @schemas.each do |_, s|
      queries.push(s.create_query)
    end
    queries.each do |query|
      begin
        direct_executer(query)
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
end
