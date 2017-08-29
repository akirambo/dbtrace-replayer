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
    @skipTypes    = ["PING","INFO","PUBLISH", "FLUSHALL","COMMAND"]
    @command2basic = {
      "GETSET"  => ["READ","UPDATE"],
      "APPEND"  => ["READ","UPDATE"],
      "SET"     => "INSERT",
      "SETEX"   => "INSERT",
      "SETNX"   => "INSERT",
      "PSETEX"  => "INSERT",
      "GET"     => "READ",
      "INCR"    => "UPDATE",
      "INCRBY"  => "UPDATE",
      "DECR"    => "UPDATE",
      "DECRBY"  => "UPDATE",
      "LPUSH"   => "INSERT",
      "RPUSH"   => "INSERT",
      "LPOP"    => "READ",
      "RPOP"    => "READ",
      "MSET"    => "INSERT",
      "MSETNX"  => "INSERT",
      "MGET"    => "READ",
      "SADD"    => "INSERT",
      "SPOP"    => "READ", 
      "STRLEN"  => "READ",
      "SREM"    => "UPDATE",
      "LRANGE"  => "SCAN",
      "HGETALL" => "SCAN",
      "ZADD"    => "INSERT",
      "HMSET"   => "INSERT",
      "HSET"    => "INSERT",
      "HGET"    => "READ",
      "HKEYS"   => "SCAN",
      "HVALS"   => "SCAN",
      "HMGET"   => "READ",
      "HINCRBY" => "UPDATE",
      "HEXISTS" => "READ",
      "HDEL"    => "UPDATE",
      "HLEN"    => "SCAN",
      "SMEMBERS" => "SCAN",
      "SRANDMEMBER" => "READ",
      "SCARD" => "SCAN",
      "ZCOUNT" => "SCAN",
      "ZCARD" => "SCAN",
      "ZRANK" => "SCAN",
      "ZREVRANK" => "SCAN",
      "ZRANGE" => "SCAN",
      "ZRANGEBYSCORE" => "SCAN",
      "ZREVRANGE" => "SCAN",
      "ZSCORE" => "SCAN",
      "ZREM" => "UPDATE",
      "ZINCRBY" => "UPDATE",
      "SMOVE" => ["SCAN","INSERT"],
      "SINTER" => "READ",
      "SDIFF"  => "READ",
      "SUNION" => "READ",
      "SUNIONSTORE" => ["READ","INSERT"],
      "SINTERSTORE" => ["READ","INSERT"],
      "SDIFFSTORE"  => ["READ","INSERT"],
      "ZUNIONSTORE" => ["READ","INSERT"],
      "ZINTERSTORE" => ["READ","INSERT"],
      "LLEN" => "SCAN",
      "LTRIM" => "UPDATE",
      "LINDEX" => "READ",
      "LSET" => "INSERT",
      "LREM" => "UPDATE",
      "RPOPLPUSH" => ["UPDATE","INSERT"],
      "ZREMRANGEBYSCORE" => "UPDATE" ,
      "ZREMRANGEBYRANK" => "UPDATE",
      "DEL" => "UPDATE"
    }
    supportedCommand = @command2basic.keys()
    if(option[:mode] == "run")then
      supportedCommand = [
        ## STRINGS
        "SET","GET","SETNX","SETEX","PSETEX",
        "MSET","MGET","MSETNX",
        "INCR","INCRBY","DECR","DECRBY",
        "APPEND","GETSET","STRLEN",
        ## SET
        "SADD","SPOP","SREM","SMOVE","SCARD","SISMEMBER",
        "SUNION","SUNIONSTORE",
        "SINTER","SINTERSTORE","SDIFF","SDIFFSTORE",
        "SMEMBERS","SRANDMEMBER",
        ## LIST 
        "LPUSH","RPUSH","LPOP","RPOP",
        "LLEN","LRANGE","LTRIM","LINDEX","LSET",
        "LREM","RPOPLPUSH",
        ## SORTED SET
        "ZADD","ZREM","ZINCRBY","ZSCORE","ZCARD",
        "ZRANK","ZREVRANK","ZRANGE","ZREVRANGE",
        "ZRANGEBYSCORE","ZCOUNT",
        "ZREMRANGEBYRANK","ZREMRANGEBYSCORE",
        "ZUNIONSTORE","ZINTERSTORE",
        ## HASH
        "HSET","HGET","HMSET","HMGET",
        "HINCRBY","HEXISTS","HDEL","HLEN",
        "HKEYS","HVALS","HGETALL",
        ## OTHERES
        "FLUSHALL","DEL"
      ]
    end
    logs = RedisLogsSimple.new(@command2basic, option, logger)
    super(filename, logs, supportedCommand, option, logger)
  end
  def parse(line)
    data = line.chop.split("\s\"")
    @typePosition.each{|index|
      if(data.size > index)then
        command = data[index].sub(/\"/,"").upcase()
        if(@supportedCommand.include?(command))then
          result = Hash.new
          args = data.delete_if(&:empty?)
          ## Skip [time]
          args.shift
          ## Skip [command]
          args.shift
          result[command] = Array.new
          args.each{|arg|
            result[command].push(arg.sub("\"",""))
          }
          return result
        else
          if(!@skipTypes.include?(command))then
            @logger.warn("Unsupported Command #{command}")
          end
        end
      end
    }
    return nil
  end
end
