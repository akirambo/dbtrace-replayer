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

module Memcached2CassandraOperation
  private

  # @conv {"SET" => ["INSERT"]}
  def memcached_set(args)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}','#{args[1]}');"
    begin
      direct_executer(command)
    rescue => e
      @logger.error(e.message)
      @logger.error(command)
      return false
    end
    true
  end

  # @conv {"GET" => ["SELECT"]}
  def memcached_get(args)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    begin
      value = direct_executer(command)
      unless value.empty?
        return value
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    ""
  end

  # @conv {"ADD" => ["INSERT","SELECT"]}
  def memcached_add(args)
    if memcached_get([args[0]]) == ""
      return memcached_set(args)
    end
    true
  end

  # @conv {"REPLACE" => ["INSERT"]}
  def memcached_replace(args)
    memcached_set(args)
  end

  # @conv {"GETS" => ["SELECT"]}
  def memcached_gets(args)
    memcached_get(args)
  end

  # @conv {"APPEND" => ["SELECT","INSERT"]}
  def memcached_append(args)
    str = memcached_get(args)
    monitor("client", "Processing")
    str += args[args.length - 1].to_s
    args[args.length - 1] = change_numeric_when_numeric(str)
    monitor("client", "Processing")
    memcached_set(args)
  end

  # @conv {"PREPEND(args x3)" => ["SELECT","INSERT"]}
  def memcached_prepend(args)
    str = memcached_get(args).to_s
    monitor("client", "Processing")
    str = args[args.length - 1].to_s + str
    args[args.length - 1] = change_numeric_when_numeric(str)
    monitor("client", "Processing")
    memcached_set(args)
  end

  # @conv {"CAS)" => ["SELECT"]}
  def memcached_cas(args)
    args.pop
    memcached_set(args)
  end

  # @conv {"incr" => ["select","insert"]}
  def memcached_incr(args)
    memcached_incr_decr(args, "incr")
  end

  # @conv {"decr" => ["select","insert"]}
  def memcached_decr(args)
    memcached_incr_decr(args, "decr")
  end

  def memcached_incr_decr(args, type)
    val = memcached_get(args).to_i
    monitor("client", "Processing")
    args[args.length - 1] = if type == "incr"
                              val + args[args.length - 1].to_i
                            elsif type == "decr"
                              val - args[args.length - 1].to_i
                            end
    monitor("client", "Processing")
    memcached_set(args)
  end

  # @conv {"DELETE" => ["DELETE"]}
  def memcached_delete(args)
    table = @option[:keyspace] + "." + @option[:columnfamily]
    command = "DELETE FROM #{table} WHERE key = '#{args[0]}'"
    direct_executer(command)
  end

  # @conv {"FLUSH" => ["TRUNCATE"]}
  def memcached_flush
    queries = []
    queries.push("drop keyspace if exists #{@option[:keyspace]}")
    queries.push("create keyspace #{@option[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3}")
    @schemas.each do |_, v|
      queries.push(v.create_query)
    end
    queries.each do |query|
      begin
        direct_executer(query)
      rescue => e
        @logger.error(e.message)
        @logger.error(command)
        return false
      end
    end
    true
  end

  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand, args)
    result = {}
    ## PREPARE SPECIAL OPERATION
    if %w[flushall].include?(operand)
      result["operand"] = operand
      return result
    end
    ## PREPARE OPERATION & ARGS
    result["operand"] = "memcached_#{operand.downcase}"
    result["args"] = @parser.exec(operand.downcase, args)
    result
  end
end
