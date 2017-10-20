
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
require_relative "./cxx/memcachedCxxRunner"

require_relative "./memcachedOperation"
require_relative "./redis2MemcachedOperation"
require_relative "./mongodb2MemcachedOperation"
require_relative "./cassandra2MemcachedOperation"

class MemcachedRunner < AbstractRunner
  ## MEMCACHED OPERATION
  include MemcachedOperation
  ## REDIS TO MEMCACHED OPERATION
  include Redis2MemcachedOperation
  ## MONGODB TO MEMCACHED OPERATION
  include MongoDB2MemcachedOperation
  ## CASSANDRA TO MEMCACHED OPERATION
  include Cassandra2MemcachedOperation
  def initialize(log_dbname, logger, option)
    @host = "127.0.0.1"
    @port = "11211"
    if ENV["MEMCACHED_IPADDRESS"]
      @host = "#{ENV["MEMCACHED_IPADDRESS"]}:#{@port}"
    else
      host = get_ip_from_file("MEMCACHED_IPADDRESS")
      if host
        @host = host
      end
    end
    ## SETUP
    @client = nil
    @parser = nil
    @logger = logger
    @option = option
    @mget   = false
    init
    super(log_dbname, logger, option)
  end

  def connect
    unless @option[:keepalive]
      setup_client
    end
  end

  def close
    unless @option[:keepalive]
      @client.close
    end
  end

  def init
    if @option[:keepalive]
      setup_client
    end
    if @option[:clearDB]
      refresh
    end
  end

  def refresh
    FLUSH(nil, true)
  end

  def finish
    if @option[:clearDB]
      refresh
    end
  end

  def async_exec
    if @mget
      @metrics.start_monitor("database", "mget")
      @client.mget
      add_total_duration(@client.getDuration, "database")
      @metrics.end_monitor("database", "mget")
      @mget = false
    end
  end

  private

  def setup_client
    @client = MemcachedCxxRunner.new
    ## true means BinaryProtocol
    @client.connect("#{@host}:#{@port}", true)
  end
end
