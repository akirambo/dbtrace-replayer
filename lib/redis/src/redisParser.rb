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

require_relative "redisLog"
require_relative "../../common/abstractDBParser"

class RedisParser < AbstractDBParser
  def initialize(filename, option, logger)
    @typePosition = [1]
    @skip_types = %w[PING INFO PUBLISH FLUSHALL COMMAND].freeze
    command2basics
    logs = RedisLogsSimple.new(@command2basic, option, logger)
    super(filename, logs, supported_commands, option, logger)
  end

  def command2basics
    @command2basic = {}
    ## Append Pattern #1 %w[READ UPDATE]
    %w[GETSET APPEND].each do |name|
      @command2basic[name] = %w[READ UPDATE].freeze
    end
    ## Append Pattern #2 INSERT
    %w[SET SETEX SETNX PSETEX LPUSH RPUSH MSET MSETNX SADD ZADD HMSET HSET LSET].each do |name|
      @command2basic[name] = "INSERT"
    end
    ## Append Pattern #3 READ
    %w[GET LPOP RPOP MGET SPOP STRLEN HGET HMGET HEXISTS SRANDMEMBER SINTER SDIFF SUNION LINDEX].each do |name|
      @command2basic[name] = "READ"
    end
    ## Append Pattern #4 UPDATE
    %w[INCR INCRBY DECR DECRBY SREM HINCRBY HDEL ZREM ZINCRBY LTRIM LREM ZREMRANGEBYSCORE ZREMRANGEBYRANK DEL].each do |name|
      @command2basic[name] = "UPDATE"
    end

    ## Append Pattern #5 SCAN
    %w[LRANGE HGETALL HKEYS HVALS HLEN SMEMBERS SCARD ZCOUNT ZCARD ZRANK ZREVRANK ZRANGE ZRANGEBYSCORE ZREVRANGE ZSCORE LLEN].each do |name|
      @command2basic[name] = "SCAN"
    end
    ## Append Pattern #6 SCAN INSERT
    %w[SMOVE].each do |name|
      @command2basic[name] = %w[SCAN INSERT].freeze
    end
    ## Append Pattern #7 READ INSERT
    %w[SUNIONSTORE SINTERSTORE SDIFFSTORE ZUNIONSTORE ZINTERSTORE].each do |name|
      @command2basic[name] = %w[READ INSERT].freeze
    end
    ## Append Pattern #8 UPDATE INSERT
    %w[RPOPLPUSH].each do |name|
      @command2basic[name] = %w[UPDATE INSERT].freeze
    end
  end

  def supported_commands
    supported_command = @command2basic.keys
    if option[:mode] == "run"
      supported_command = [
        ## STRINGS
        "SET", "GET", "SETNX", "SETEX", "PSETEX",
        "MSET", "MGET", "MSETNX",
        "INCR", "INCRBY", "DECR", "DECRBY",
        "APPEND", "GETSET", "STRLEN",
        ## SET
        "SADD", "SPOP", "SREM", "SMOVE", "SCARD", "SISMEMBER",
        "SUNION", "SUNIONSTORE",
        "SINTER", "SINTERSTORE", "SDIFF", "SDIFFSTORE",
        "SMEMBERS", "SRANDMEMBER",
        ## LIST
        "LPUSH", "RPUSH", "LPOP", "RPOP",
        "LLEN", "LRANGE", "LTRIM", "LINDEX", "LSET",
        "LREM", "RPOPLPUSH",
        ## SORTED SET
        "ZADD", "ZREM", "ZINCRBY", "ZSCORE", "ZCARD",
        "ZRANK", "ZREVRANK", "ZRANGE", "ZREVRANGE",
        "ZRANGEBYSCORE", "ZCOUNT",
        "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
        "ZUNIONSTORE", "ZINTERSTORE",
        ## HASH
        "HSET", "HGET", "HMSET", "HMGET",
        "HINCRBY", "HEXISTS", "HDEL", "HLEN",
        "HKEYS", "HVALS", "HGETALL",
        ## OTHERES
        "FLUSHALL", "DEL"
      ]
    end
    supported_command
  end

  def parse(line)
    data = line.chop.split("\s\"")
    @typePosition.each do |index|
      if data.size > index
        command = data[index].sub(/\"/, "").upcase
        if @supportedCommand.include?(command)
          result = {}
          args = data.delete_if(&:empty?)
          ## Skip [time]
          args.shift
          ## Skip [command]
          args.shift
          result[command] = []
          args.each do |arg|
            result[command].push(arg.sub("\"", ""))
          end
          return result
        else
          unless @skip_types.include?(command)
            @logger.warn("Unsupported Command #{command}")
          end
        end
      end
    end
    nil
  end
end
