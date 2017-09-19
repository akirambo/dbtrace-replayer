
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

module RedisOperation
  private

  ############
  ## String ##
  ############

  def set(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def get(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def setnx(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def setex(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def psetex(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def mset(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def mget(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end

  def msetnx(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def incr(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def incrby(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def decr(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def decrby(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def append(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def getset(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end

  def strlen(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end
  ###########
  ## Lists ##
  ###########

  def lpush(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def rpush(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def lpop(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def rpop(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def lrange(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end

  def lrem(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def lindex(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end

  def rpoplpush(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end

  def lset(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def ltrim(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def llen(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end
  #########
  ## Set ##
  #########

  def srandmember(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def smembers(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def sdiff(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def sdiffstore(args)
    s_store(__method__, args)
  end

  def sinter(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def sinterstore(args)
    s_store(__method__, args)
  end

  def sunion(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def sunionstore(args)
    s_store(__method__, args)
  end

  def sismember(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def srem(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def smove(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def scard(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def sadd(args)
    command = "#{__method__} #{args["key"]} #{args["args"]}"
    redis_cxx_executer(__method__, command)
  end

  def spop(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command, true)
  end
  #################
  ## Sorted Sets ##
  #################

  def zadd(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def zrem(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def zincrby(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def zrank(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zrevrank(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zrange(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zrevrange(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zrangebyscore(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zcount(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zcard(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zscore(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zremrangebyscore(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zremrangebyrank(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def zunionstore(args)
    z_xstore(__method__, args)
  end

  def zinterstore(args)
    z_xstore(__method__, args)
  end
  ############
  ## Hashes ##
  ############

  def hset(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def hget(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def hmget(args, attime = false)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redis_cxx_executer(__method__, command, true, attime)
  end

  def hmset(args)
    s_store(__method__, args)
  end

  def hincrby(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def hexists(args, attime = false)
    get_type_operation(__method__, args, attime)
  end

  def hdel(args)
    command = "#{__method__} #{args.join(" ")}"
    redis_cxx_executer(__method__, command)
  end

  def hlen(args, attime = false)
    hlen_type_operation(__method__, args, attime)
  end

  def hkeys(args, attime = false)
    hlen_type_operation(__method__, args, attime)
  end

  def hvals(args, attime = false)
    hlen_type_operation(__method__, args, attime)
  end

  def hgetall(args, attime = false)
    hlen_type_operation(__method__, args, attime)
  end

  ############
  ## OTHRES ##
  ############

  def flushall(args, initFlag = false)
    @logger.debug("GENERATED QUERY: #{__method__}")
    connect
    if @option[:async]
      if args && args.size == 2
        unless initFlag
          add_count(__method__)
        end
        query = "#{__method__} #{args[0]} #{args[1]}"
      else
        unless initFlag
          add_count(__method__)
        end
        query = __method__.to_s
      end
      v = redis_async_executer(query)
    else
      query = __method__.to_s
      if args && args.size == 2
        query += " #{args[0]} #{args[1]}"
      end
      v = @client.syncExecuter(query)
      if @metrics && !initFlag
        add_duration(@client.getDuration, "database", __method__)
      end
    end
    close
    v
  end

  def del(args)
    command = "#{__method__} #{args[0]}"
    redis_cxx_executer(__method__, command)
  end

  def keys(pattern, type)
    keys = @client.keys
    targets = []
    if type == "keyspace"
      pattern = "#{pattern}."
    elsif type == "table"
      pattern = ".#{pattern}"
    end
    keys.each do |key|
      if key.include?(pattern)
        targets.push(key)
      end
    end
    targets
  end
  ###################
  ## Common method ##
  ###################

  def get_type_operation(name, args, attime)
    command = "#{name} #{args.join(" ")}"
    redis_cxx_executer(name, command, true, attime)
  end

  def hlen_type_operation(name, args, attime)
    command = "#{name} #{args[0]}"
    redis_cxx_executer(name, command, true, attime)
  end

  def z_xstore(name, args)
    command = "#{name} #{args["key"]} #{args["args"].size} #{args["args"].join(" ")}"
    if args["option"] != {}
      command += redis_optionhash2command(args["option"])
    end
    redis_cxx_executer(name, command)
  end

  def s_store(name, args)
    command = "#{name} #{args["key"]} #{args["args"].join(" ")}"
    redis_cxx_executer(name, command)
  end
  #############
  ## PREPARE ##
  #############

  def prepare_redis(operand, args)
    result = {}
    result["operand"] = operand
    result["args"] = args
    case1 = %w[zunionstore zinterstore].freeze
    case2 = %w[mset mget msetnx].freeze
    case3 = %w[hmset hmget sdiffstore sinterstore sunionstore].freeze
    if case1.include?(operand)
      result["args"] = @parser.extract_z_x_store_args(args)
    elsif case2.include?(operand)
      result["args"] = @parser.args2hash(args)
    elsif case3.include?(operand)
      result["args"] = @parser.args2key_args(args)
    end
    result
  end

  ##################
  ## CXX Executer ##
  ##################
  def redis_cxx_reply
    if @option[:async]
      @client.getAsyncReply
    else
      @client.getReply
    end
  end

  def redis_cxx_executer(method, query, getValue = false, attime = false)
    @logger.debug("GENERATED QUERY: #{query}")
    connect
    if @option[:async]
      v = redis_async_executer(query, attime)
    else
      if @client.syncExecuter(query)
        v = "OK"
      end
      add_duration(@client.getDuration, "database", method)
    end
    if getValue && (attime || !@option[:async])
      v = redis_cxx_reply
    end
    close
    v
  end
  ###########
  ## Async ##
  ###########

  def redis_async_executer(query, attime = false)
    @pool_request_size += 1
    if query.nil?
      redis_async_executer_non_query
    elsif attime
      redis_async_executer_attime(query)
    elsif @option[:poolRequestMaxSize] == -1 || @pool_request_size <= @option[:poolRequestMaxSize]
      @client.commitQuery(query)
      method = query.split(" ")[0]
      add_count(method.to_sym)
    elsif @pool_request_size > @option[:poolRequestMaxSize]
      @client.commitQuery(query)
      method = query.split(" ")[0]
      add_count(method.to_sym)
      @metrics.start_monitor("database", "AsyncExec")
      @client.asyncExecuter
      add_total_duration(@client.getDuration, "database")
      @metrics.end_monitor("database", "AsyncExec")
      @pool_request_size = 0
    end
    "OK"
  end

  def redis_async_executer_non_query
    if @client.pooledQuerySize > 0
      @pool_request_size = 0
      add_count("AsyncExec")
      @metrics.start_monitor("database", "AsyncExec")
      @client.asyncExecuter
      add_total_duration(@client.getDuration, "database")
      @metrics.end_monitor("database", "AsyncExec")
    end
  end

  def redis_async_executer_attime(query)
    if @client.pooledQuerySize > 0
      @client.commitQuery(query)
      method = query.split(" ")[0]
      add_count(method.to_sym)
      @metrics.start_monitor("database", "AsyncExec")
      @client.asyncExecuter
      add_total_duration(@client.getDuration, "database")
      @metrics.end_monitor("database", "AsyncExec")
    else
      method = query.split(" ")[0]
      monitor("database", method.to_sym)
      @client.syncConnect(@host, @port.to_i)
      @client.syncExecuter(query)
      monitor("database", method.to_sym)
      @client.syncClose
    end
  end
  ################
  ## Sub Method ##
  ################

  def redis_optionhash2command(hash)
    command = ""
    hash.each do |k, v|
      if v.class == Array
        command += " #{k} #{v.join(" ")}"
      elsif v.class == String
        command += " #{k} #{v}"
      end
    end
    command
  end
end
