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

require "mongo"

require_relative "../../common/abstract_runner"
require_relative "./cxx/mongodbCxxRunner"

require_relative "../../mongodb/src/mongodbArgumentParser"
require_relative "../../redis/src/redisArgumentParser"
require_relative "../../memcached/src/memcachedArgumentParser"
require_relative "../../cassandra/src/cassandraArgumentParser"

require_relative "./mongodbOperation"
require_relative "./redis2MongodbOperation"
require_relative "./memcached2MongodbOperation"
require_relative "./cassandra2MongodbOperation"


class MongodbRunner < AbstractRunner
  ## MONGODB OPERATION 
  include MongodbOperation
  ## MEMCACHED TO MONGODB OPERATION
  include Memcached2MongodbOperation
  ## REDIS TO MONGODB OPERATION
  include Redis2MongodbOperation
  ## CASSANDRA TO MONGODB OPERATION
  include Cassandra2MongodbOperation
  
  def initialize(logDBName, logger, option)
    @host = "127.0.0.1"
    @port = 27017
    if(ENV["MONGODB_IPADDRESS"])then
      @host = ENV["MONGODB_IPADDRESS"]
    end
    ## SETUP
    @client = nil
    @parser = nil
    @logger = logger
    @option = option
    case option[:sourceDB].upcase
    when "MONGODB" then
      @parser = MongodbArgumentParser.new(logger)
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
    super(logDBName,logger,option)
  end
  def connect
    if(!@option[:keepalive])then
      @client.connect("mongodb://#{@host}:#{@port}")
    end
  end
  def close
    if(!@option[:keepalive])then
      @client.close()
    end
  end
  def async_exec
    @metrics.start_monitor("database","INSERT")
    @client.insertMany()
    #add_count("INSERT_MANY")
    add_total_duration(@client.getDuration(),"database")
    #@metrics.end_monitor("database","INSERT_MANY")
    @metrics.end_monitor("database","INSERT")
  end 

  def init
    setupClient
    if(@option[:clearDB])then
      refresh
    end
  end
  def finish
    if(@option[:clearDB])then
      refresh
    end
  end
  def refresh
    setupClient
    connect
    @client.drop()
    @client.close()
  end
  def setupClient
    @client = MongodbCxxRunner.new()
  end
end
