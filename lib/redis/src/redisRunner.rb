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

require "json"

require_relative "../../common/abstractRunner"
require_relative "./cxx/redisCxxRunner"

## Parser
require_relative "../../mongodb/src/mongodbArgumentParser"
require_relative "../../mongodb/src/mongodbQueryParser"
require_relative "../../mongodb/src/mongodbQueryProcessor"

require_relative "../../redis/src/redisArgumentParser"
require_relative "../../memcached/src/memcachedArgumentParser"
require_relative "../../cassandra/src/cassandraArgumentParser"

require_relative "./redisOperation"
require_relative "./memcached2RedisOperation"
require_relative "./mongodb2RedisOperation"
require_relative "./cassandra2RedisOperation"


class RedisRunner < AbstractRunner
  ## REDIS OPERATION 
  include RedisOperation
  ## MEMCACHED TO REDIS OPERATION
  include Memcached2RedisOperation
  ## MONGODB TO REDIS OPERATION
  include MongoDB2RedisOperation
  ## CASSANDRA TO REDIS OPERATION
  include Cassandra2RedisOperation
  
  def initialize(logDBName,
                 logger,
                 option)
    @host = "127.0.0.1"
    @port = 6379
    if(ENV["REDIS_IPADDRESS"])then
      @host = ENV["REDIS_IPADDRESS"]
    end
    ## SETUP
    @parser = nil
    @logger = logger
    @option = option
    @pool_request_size = 0
    if(@option[:poolRequestMaxSize] == nil)then
      @option[:poolRequestMaxSize] = -1  ## Redis Default 2500
    end
    
    case @option[:sourceDB].upcase
    when "MONGODB" then
      @parser = MongodbArgumentParser.new(logger)
      @queryParser = MongodbQueryParser.new(logger)
      @queryProcessor = MongodbQueryProcessor.new(logger)
    when "REDIS" then
      @parser = RedisArgumentParser.new(logger)
    when "MEMCACHED" then
      @parser = MemcachedArgumentParser.new(logger,option)
    when "CASSANDRA" then
      @parser = CassandraArgumentParser.new(logger,option)
    else
      if(option[:mode] != "clear")then
        @logger.error("Unsupported DB Log #{option[:sourceDB].upcase}" )
      end
    end
    
    ## Setup Client
    @client = nil
    if(@option[:keepalive])then
      case @option[:api]
      when "cxx" then
        @client = RedisCxxRunner.new()
        if(@option[:async])then
          @client.asyncConnect(@host, @port.to_i)
        else
          @client.syncConnect(@host, @port.to_i)
        end
      end
      super(logDBName,logger,option)
    end
    def connect
      if(!@option[:keepalive])then
        case @option[:api] 
        when "cxx" then
        @client = RedisCxxRunner.new()
        if(@option[:async])then
          @client.asyncConnect(@host, @port.to_i)
        else
          @client.syncConnect(@host, @port.to_i)
        end
        end
      end
    end
    def close
      if(!@option[:keepalive])then
        case @option[:api] 
        when "cxx" then
          if(@option[:async])then
            @client.asyncClose()
          else
            @client.syncClose()
          end
        end
      end
    end
    def init
      if(@option[:clearDB])then
        refresh
      end
    end
    def refresh
      FLUSHALL(nil,true)
    end
    def finish
      if(@option[:clearDB])then
        refresh
      end
    end
    def asyncExec()
      if(@pool_request_size > 0)then
        redisAsyncExecuter(nil,true)
      end
   end
  end
end
