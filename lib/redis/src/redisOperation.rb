
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

  def SET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def GET(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SETNX(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SETEX(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def PSETEX(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def MSET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def MGET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end

  def MSETNX(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def INCR(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def INCRBY(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def DECR(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def DECRBY(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def APPEND(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def GETSET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end

  def STRLEN(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end
  ###########
  ## Lists ##
  ###########

  def LPUSH(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def RPUSH(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def LPOP(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def RPOP(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def LRANGE(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end

  def LREM(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def LINDEX(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end

  def RPOPLPUSH(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end

  def LSET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def LTRIM(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def LLEN(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end
  #########
  ## Set ##
  #########

  def SRANDMEMBER(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SMEMBERS(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SDIFF(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SDIFFSTORE(args)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SINTER(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SINTERSTORE(args)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SUNION(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SUNIONSTORE(args)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SISMEMBER(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SREM(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SMOVE(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def SCARD(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def SADD(args)
    command = "#{__method__} #{args["key"]} #{args["args"]}"
    redisCxxExecuter(__method__, command)
  end

  def SPOP(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true)
  end
  #################
  ## Sorted Sets ##
  #################

  def ZADD(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def ZREM(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def ZINCRBY(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def ZRANK(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZREVRANK(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZRANGE(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZREVRANGE(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZRANGEBYSCORE(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZCOUNT(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZCARD(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZSCORE(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZREMRANGEBYSCORE(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZREMRANGEBYRANK(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def ZUNIONSTORE(args)
    z_xstore(args)
  end

  def ZINTERSTORE(args)
    z_xstore(args)
  end

  ## Common method
  def z_xstore(args)
    command = "#{__method__} #{args["key"]} #{args["args"].size} #{args["args"].join(" ")}"
    if args["options"] != {}
      command += redisOptionHash2Command(args["options"])
    end
    redisCxxExecuter(__method__, command)
  end
  ############
  ## Hashes ##
  ############

  def HSET(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def HGET(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HMGET(args, atTime = false)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HMSET(args)
    command = "#{__method__} #{args["key"]} #{args["args"].join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def HINCRBY(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def HEXISTS(args, atTime = false)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HDEL(args)
    command = "#{__method__} #{args.join(" ")}"
    redisCxxExecuter(__method__, command)
  end

  def HLEN(args, atTime = false)
    command = "#{__method__} #{args[0]}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HKEYS(args, atTime = false)
    command = "#{__method__} #{args[0]}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HVALS(args, atTime = false)
    command = "#{__method__} #{args[0]}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  def HGETALL(args, atTime = false)
    command = "#{__method__} #{args[0]}"
    redisCxxExecuter(__method__, command, true, atTime)
  end

  ############
  ## OTHRES ##
  ############

  def FLUSHALL(args, initFlag = false)
    @logger.debug("GENERATED QUERY: #{__method__}")
    connect
    if @option[:async]
      if args && args.size == 2
        unless initFlag
          addCount(__method__)
        end
        query = "#{__method__} #{args[0]} #{args[1]}"
        v = redisAsyncExecuter(query)
      else
        unless initFlag
          addCount(__method__)
        end
        query = __method__.to_s
        v = redisAsyncExecuter(query)
      end
    else
      query = __method__.to_s
      if args && args.size == 2
        query += " #{args[0]} #{args[1]}"
      end
      v = @client.syncExecuter(query)
      if @metrics && !initFlag
        addDuration(@client.getDuration, "database", __method__)
      end
    end
    close
    v
  end

  def DEL(args)
    command = "#{__method__} #{args[0]}"
    redisCxxExecuter(__method__, command)
  end

  def KEYS(pattern, type)
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

  #############
  ## PREPARE ##
  #############

  def prepare_REDIS(operand, args)
    result = {}
    result["operand"] = operand
    result["args"] = args
    case1 = %w[ZUNIONSTORE ZINTERSTORE].freeze
    case2 = %w[MSET MGET MSETNX].freeze
    case3 = %w[HMSET HMGET SDIFFSTORE SINTERSTORE SUNIONSTORE].freeze
    if case1.include?(operand)
      result["args"] = @parser.extractZ_X_STORE_ARGS(args)
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
  
  def redisCxxReply
    if @option[:async]
      @client.getAsyncReply
    else
      @client.getReply
    end
  end

  def redisCxxExecuter(method, query, getValue = false, atTime = false)
    @logger.debug("GENERATED QUERY: #{query}")
    connect
    if @option[:async]
      v = redisAsyncExecuter(query, atTime)
    else
      if @client.syncExecuter(query)
        v = "OK"
      end
      addDuration(@client.getDuration, "database", method)
    end
    if getValue && (atTime || !@option[:async])
      v = redisCxxReply
    end
    close
    v
  end
  ###########
  ## Async ##
  ###########

  def redisAsyncExecuter(query, atTime = false)
    @pool_request_size += 1
    if query.nil?
      if @client.pooledQuerySize > 0
        @pool_request_size = 0
        addCount("AsyncExec")
        @metrics.start_monitor("database", "AsyncExec")
        @client.asyncExecuter
        addTotalDuration(@client.getDuration, "database")
        @metrics.end_monitor("database", "AsyncExec")
      end
    elsif atTime
      if @client.pooledQuerySize > 0
        @client.commitQuery(query)
        method = query.split(" ")[0]
        addCount(method.to_sym)
        @metrics.start_monitor("database", "AsyncExec")
        @client.asyncExecuter
        addTotalDuration(@client.getDuration, "database")
        @metrics.end_monitor("database", "AsyncExec")
      else
        method = query.split(" ")[0]
        monitor("database", method.to_sym)
        @client.syncConnect(@host, @port.to_i)
        @client.syncExecuter(query)
        monitor("database", method.to_sym)
        @client.syncClose
      end
    elsif @option[:poolRequestMaxSize] == -1 || @pool_request_size <= @option[:poolRequestMaxSize]
      @client.commitQuery(query)
      method = query.split(" ")[0]
      addCount(method.to_sym)
    elsif @pool_request_size > @option[:poolRequestMaxSize]
      @client.commitQuery(query)
      method = query.split(" ")[0]
      addCount(method.to_sym)
      @metrics.start_monitor("database", "AsyncExec")
      @client.asyncExecuter
      addTotalDuration(@client.getDuration, "database")
      @metrics.end_monitor("database", "AsyncExec")
      @pool_request_size = 0
    end
    "OK"
  end

  ################
  ## Sub Method ##
  ################

  def redisOptionHash2Command(hash)
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
