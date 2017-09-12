
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

require_relative "./utils"
require_relative "./metrics"

require_relative "../../mongodb/src/mongodbArgumentParser"
require_relative "../../redis/src/redisArgumentParser"
require_relative "../../memcached/src/memcachedArgumentParser"
require_relative "../../cassandra/src/cassandraArgumentParser"

require_relative "../../mongodb/src/mongodbQueryParser"
require_relative "../../mongodb/src/mongodbQueryProcessor"

class AbstractRunner
  def initialize(dbname, logger, option)
    @log_dbname = dbname.downcase
    @parser = nil
    ## REMOVE START
    @logger = logger
    @option = option
    ## REMOVE END
    @utils = Utils.new
    @monitor_name = nil
    @metrics = Metrics.new(@logger, @option)
    setup_argument_parser
  end

  def setup_argument_parser
    case @option[:sourceDB].upcase
    when "MONGODB" then
      @parser = MongodbArgumentParser.new(@logger)
      @query_parser = MongodbQueryParser.new(@logger)
      @query_processor = MongodbQueryProcessor.new(@logger)
    when "REDIS" then
      @parser = RedisArgumentParser.new(@logger)
    when "MEMCACHED" then
      @parser = MemcachedArgumentParser.new(@logger, @option)
    when "CASSANDRA" then
      @parser = CassandraArgumentParser.new(@logger, @option)
    else
      if option[:mode] != "clear"
        @logger.error("Unsupported DB Log #{@option[:sourceDB].upcase}")
      end
    end
  end
  
  def exec(workload)
    init
    workload.each do |ope|
      ope.keys.each do |command|
        cmd = command.upcase.sub("_*", "")
        @logger.debug(" -- #{cmd} --")
        operation(cmd, ope[command])
      end
    end
    if @option[:async]
      async_exec
    end
    @metrics.output
    finish
  end

  def init
    @logger.warn("Database Init Function is NOT implemented.")
  end

  def refresh
    @logger.warn("Database Reset Function is NOT implemented.")
  end

  def finish
    @logger.warn("Database Finish Function is NOT implemented.")
  end

  def operation(operand, args)
    ## prepare
    begin
      conv = send("prepare_#{@log_dbname}", operand, args)
    rescue => e
      @logger.fatal("Crash @ prepare_#{@log_dbname}")
      @logger.error(e.message)
      @logger.error(args)
    end
    ## run
    begin
      @monitor_name = operand.downcase
      @metrics.start_monitor("database", @monitor_name)
      @metrics.start_monitor("client", @monitor_name)
      send(conv["operand"], conv["args"])
      @metrics.end_monitor("database", @monitor_name)
      @metrics.end_monitor("client", @monitor_name)
    rescue => e
      @logger.error("[#{operand}] Operation([#{@option[:sourceDB]}] TO [#{@option[:targetDB]}] is not supported @ #{__FILE__}")
      if !conv.nil? && !conv["operand"].nil?
        @logger.error("Operator :: #{conv["operand"]}")
      end
      @logger.error(e.message)
      @logger.error(args)
    end
  end

  def fatal(operand, args)
    @logger.fatal("Illegal Arguments @ #{operand} --> #{args}")
  end

  def parse_json(data)
    @utils.parse_json(data)
  end

  def convert_json(hash)
    @utils.convert_json(hash)
  end

  def create_numbervalue(bytesize)
    @utils.create_numbervalue(bytesize)
  end

  def create_string(bytesize)
    @utils.create_string(bytesize)
  end

  def change_numeric_when_numeric(input)
    @utils.change_numeric_when_numeric(input)
  end

  #########################
  ## Performance Monitor ##
  #########################
  def monitor(type, targetQuery = "all")
    @metrics.monitor(type, targetQuery)
  end

  def add_duration(duration, type, targetQuery = "all")
    @metrics.add_duration(duration, type, targetQuery)
  end

  def add_total_duration(duration, type)
    @metrics.add_total_duration(duration, type)
  end

  def add_count(targetQuery = "all")
    @metrics.add_count(targetQuery)
  end

  ###################
  # Async Execution #
  ###################
  def async_exec
    puts "Please Implement #{__method__}"
  end
end
