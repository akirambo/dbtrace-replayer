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

require_relative "../../common/abstract_runner"
require_relative "./cxx/redisCxxRunner"

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

  def initialize(log_db_name,
                 logger,
                 option)
    @host = "127.0.0.1"
    @port = 6379
    if ENV["REDIS_IPADDRESS"]
      @host = ENV["REDIS_IPADDRESS"]
    end
    ## SETUP
    @parser = nil
    @logger = logger
    @option = option
    @pool_request_size = 0
    if @option[:poolRequestMaxSize].nil?
      ## Redis Default 2500
      @option[:poolRequestMaxSize] = -1
    end
    ## Setup Client
    @client = nil
    if @option[:keepalive]
      setup_client
    end
    super(log_db_name, logger, option)
  end

  def connect
    unless @option[:keepalive]
      setup_client
    end
  end

  def close
    unless @option[:keepalive]
      case @option[:api]
      when "cxx" then
        if @option[:async]
          @client.asyncClose
        else
          @client.syncClose
        end
      end
    end
  end

  def init
    if @option[:clearDB]
      refresh
    end
  end

  def refresh
    FLUSHALL(nil, true)
  end

  def finish
    if @option[:clearDB]
      refresh
    end
  end

  def async_exec
    if @pool_request_size > 0
      redis_async_executer(nil, true)
    end
  end

  def setup_client
    case @option[:api]
    when "cxx" then
      @client = RedisCxxRunner.new
      if @option[:async]
        @client.asyncConnect(@host, @port.to_i)
      else
        @client.syncConnect(@host, @port.to_i)
      end
    end
  end
end
