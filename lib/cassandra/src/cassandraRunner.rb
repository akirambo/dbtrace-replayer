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

require 'cassandra'

require_relative "../../common/abstractRunner"
require_relative "./cxx/cassandraCxxRunner"

require_relative "../../mongodb/src/mongodbArgumentParser"
require_relative "../../mongodb/src/mongodbQueryParser"
require_relative "../../mongodb/src/mongodbQueryProcessor"

require_relative "../../redis/src/redisArgumentParser"
require_relative "../../memcached/src/memcachedArgumentParser"
require_relative "../../cassandra/src/cassandraArgumentParser"

require_relative "./redis2CassandraOperation"
require_relative "./memcached2CassandraOperation"
require_relative "./cassandraOperation"
require_relative "./mongodb2CassandraOperation"

class CassandraRunner < AbstractRunner
  ## CASSANDRA OPERATION
  include CassandraOperation
  ## MONGODB TO CASSANDRA OPERATION 
  include Mongodb2CassandraOperation
  ## MEMCACHED TO MONGODB OPERATION
  include Memcached2CassandraOperation
  ## REDIS TO MONGODB OPERATION
  include Redis2CassandraOperation
  
  def initialize(logDBName, logger, option)
    ## SETUP
    @host = "127.0.0.1"
    if(ENV["CASSANDRA_IPADDRESS"])then
      @host = ENV["CASSANDRA_IPADDRESS"]
    end
    @client = nil
    @parser = nil
    @logger = logger
    @option = option

    ## Cassandra Specific Variable
    @columnFamily2keySpace = {}
    _schema_ = CassandraArgumentParser.new(logger,option)
    @schemas = _schema_.schemas
    @createQueries = _schema_.schemas
    @poolRequestSize = 0
    @poolByteSize = 0
    if(@option[:poolRequestMaxSize] == nil)then
      @option[:poolRequestMaxSize] = 250 ## Cassandra Default 256
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
    end
    ## EXTRACT SCHEMA
    init 
    super(logDBName,logger,option)
  end
  def refresh
    @client = CassandraCxxRunner.new()
    @client.connect(@host)
    @client.resetDatabase()
    @client.close()
  end  
  private

  def setupCxx(keyspace)
    @client = CassandraCxxRunner.new()
    @client.connect(@host)
    @client.syncExecuter("create keyspace if not exists  #{@option[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3}")
    ## Refresh By Schema File 
    @schemas.each{|cf,schema|
      ## Create Keyspace
      begin
        @client.syncExecuter(schema.createKeyspaceQuery)
      rescue => e
        @logger.error("Cannot Create Keyspace. #{e.message}")
        @logger.error("QUERY :: #{schema.dropQuery}")
      end
      begin 
        @client.syncExecuter(schema.createQuery)
      rescue => e
        @logger.error("Cannot Create Table. #{e.message}")
        @logger.error("QUERY :: #{schema.createQuery}")
      end
      schema.createIndexes.each{|query|
        begin
          @client.syncExecuter(query)
        rescue => e
          @logger.error("Cannot Create Index. #{e.message}")
          @logger.error("QUERY :: #{query}")
        end
      }
    }
    @client.resetQuery()
    @poolRequestSize = 0    
    if(!@option[:keepalive])then
      @client.close()
    end
  end

  def connect
    if(!@option[:keepalive])then
      @client.connect(@host)
    end
  end
  def close
    if(!@option[:keepalive])then
      @client.close()
    end
  end
  def asyncExec()
    # @metrics.start_monitor("database","AsyncExec")
    @client.asyncExecuter()
    addTotalDuration(@client.getDuration(),"database")
    # @metrics.end_monitor("database","AsyncExec")
    @client.resetQuery()
    @poolRequestSize = 0
    @poolByteSize = 0
  end 
  def init
    setupCxx(@option[:keyspace])
    if(@option[:clearDB])then
      refresh
    end
  end
  def finish
    if(@option[:clearDB])then
      refresh
    end
  end
end
