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
    @type_position = [1]
    @skip_types = %w[PING INFO PUBLISH FLUSHALL COMMAND].freeze
    command2basics(option)
    logs = RedisLogsSimple.new(@command2basic, option, logger)
    super(filename, logs, supported_commands(option), option, logger)
  end

  def command2basics(_)
    @command2basic = {}
    ## Append Pattern #1 %w[READ UPDATE]
    %w[getset append].each do |name|
      @command2basic[name] = %w[read update].freeze
    end
    ## Append Pattern #2 INSERT
    %w[set setex setnx psetex lpush rpush mset msetnx sadd zadd hmset hset lset].each do |name|
      @command2basic[name] = "insert"
    end
    ## Append Pattern #3 READ
    %w[get lpop rpop mget spop strlen hget hmget hexists srandmember sinter sdiff sunion lindex].each do |name|
      @command2basic[name] = "read"
    end
    ## Append Pattern #4 UPDATE
    %w[incr incrby decr decrby srem hincrby hdel rem zincrby ltrim lrem zremrangebyscore zremrangebyrank del].each do |name|
      @command2basic[name] = "update"
    end

    ## Append Pattern #5 SCAN
    %w[lrange hgetall hkeys hvals hlen smembers scard zcount zcard zrank zrevrank zrange zrangebyscore zrevrange zscore llen].each do |name|
      @command2basic[name] = "scan"
    end
    ## Append Pattern #6 SCAN INSERT
    %w[smove].each do |name|
      @command2basic[name] = %w[scan insert].freeze
    end
    ## Append Pattern #7 READ INSERT
    %w[sunionstore sinterstore sdiffstore zunionstore zinterstore].each do |name|
      @command2basic[name] = %w[read insert].freeze
    end
    ## Append Pattern #8 UPDATE INSERT
    %w[rpoplpush].each do |name|
      @command2basic[name] = %w[update insert].freeze
    end
  end

  def supported_commands(option)
    supported_command = @command2basic.keys
    if option[:mode] == "run"
      supported_command = [
        ## STRINGS
        "set", "get", "setnx", "setex", "psetex",
        "mset", "mget", "msetnx",
        "incr", "incrby", "decr", "decrby",
        "append", "getset", "strlen",
        ## set
        "sadd", "spop", "srem", "smove", "scard", "sismember",
        "sunion", "sunionstore",
        "sinter", "sinterstore", "sdiff", "sdiffstore",
        "smembers", "srandmember",
        ## list
        "lpush", "rpush", "lpop", "rpop",
        "llen", "lrange", "ltrim", "lindex", "lset",
        "lrem", "rpoplpush",
        ## sorted set
        "zadd", "zrem", "zincrby", "zscore", "zcard",
        "zrank", "zrevrank", "zrange", "zrevrange",
        "zrangebyscore", "zcount",
        "zremrangebyrank", "zremrangebyscore",
        "zunionstore", "zinterstore",
        ## hash
        "hset", "hget", "hmset", "hmget",
        "hincrby", "hexists", "hdel", "hlen",
        "hkeys", "hvals", "hgetall",
        ## otheres
        "flushall", "del"
      ]
    end
    supported_command
  end

  def parse(line)
    data = line.chop.split("\s\"")
    @type_position.each do |index|
      if data.size > index
        command = data[index].sub(/\"/, "").downcase
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
