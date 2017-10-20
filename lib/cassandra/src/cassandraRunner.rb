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

require_relative "../../common/abstract_runner"
require_relative "./cxx/cassandraCxxRunner"

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

  def initialize(log_dbname, logger, option)
    ## SETUP
    @host = "127.0.0.1"
    if ENV["CASSANDRA_IPADDRESS"]
      @host = ENV["CASSANDRA_IPADDRESS"]
    else
      host = get_ip_from_file("CASSANDRA_IPADDRESS")
      if host
        @host = host
      end
    end
    @client = nil
    @parser = nil
    @logger = logger
    @option = option

    ## Cassandra Specific Variable
    schema_ = CassandraArgumentParser.new(logger, option)
    @schemas = schema_.schemas
    @pool_request_size = 0
    @pool_byte_size = 0
    ## EXTRACT SCHEMA
    init
    super(log_dbname, logger, option)
  end

  def refresh
    @client = CassandraCxxRunner.new
    @client.connect(@host)
    @client.resetDatabase
    @client.close
  end

  private

  def setup_cxx
    @client = CassandraCxxRunner.new
    @client.connect(@host)
    # @client.syncExecuter("create keyspace if not exists  #{@option[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3}")
    ## Refresh By Schema File
    @schemas.each do |_, schema|
      ### Create Keyspace
      cassandra_setup_query(schema.create_keyspace_query,
                            "Cannot Create Keyspace")
      cassandra_setup_query(schema.create_query,
                            "Cannot Create Table")
      schema.create_indexes.each do |query|
        cassandra_setup_query(query,
                              "Cannot Create Index")
      end
    end
    @client.resetQuery
    @pool_request_size = 0
    unless @option[:keepalive]
      @client.close
    end
  end

  def cassandra_setup_query(query, code)
    @logger.debug(query)
    begin
      @client.syncExecuter(query)
    rescue => e
      @logger.error("#{code} #{e.message}")
      @logger.error("QUERY :: #{query}")
    end
  end

  def connect
    unless @option[:keepalive]
      @client.connect(@host)
    end
  end

  def close
    unless @option[:keepalive]
      @client.close
    end
  end

  def async_exec
    # @metrics.start_monitor("database","AsyncExec")
    @client.asyncExecuter
    add_total_duration(@client.getDuration, "database")
    # @metrics.end_monitor("database","AsyncExec")
    @client.resetQuery
    @pool_request_size = 0
    @pool_byte_size = 0
  end

  def init
    if @option[:clearDB]
      refresh
    end
    setup_cxx
  end

  def finish
    if @option[:clearDB]
      refresh
    end
  end
end
