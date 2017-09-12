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
  def MEMCACHED_SET(args)
    table = @option[:keyspace]+"."+@option[:columnfamily]
    command = "INSERT INTO #{table} (key,value) VALUES ('#{args[0]}','#{args[1]}');"
    begin
      DIRECT_EXECUTER(command)
    rescue => e
      @logger.error(e.message)
      @logger.error(command)
      return false
    end
    return true
  end
  # @conv {"GET" => ["SELECT"]}
  def MEMCACHED_GET(args)
    table = @option[:keyspace]+"."+@option[:columnfamily]
    command = "SELECT value FROM #{table} WHERE key = '#{args[0]}';"
    begin
      value = DIRECT_EXECUTER(command)
      if(value.size > 0)then
        return value
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    return ""
  end
  # @conv {"ADD" => ["INSERT","SELECT"]}
  def MEMCACHED_ADD(args)
    if(MEMCACHED_GET([args[0]]) == "")then
      return MEMCACHED_SET(args)
    end
    return true
  end
  # @conv {"REPLACE" => ["INSERT"]}
  def MEMCACHED_REPLACE(args)
    return MEMCACHED_SET(args)
  end
  # @conv {"GETS" => ["SELECT"]}
  def MEMCACHED_GETS(args)
    return MEMCACHED_GET(args)
  end
  # @conv {"APPEND" => ["SELECT","INSERT"]}
  def MEMCACHED_APPEND(args)
    str = MEMCACHED_GET(args)
    monitor("client","Processing")
    str += args[args.length-1].to_s
    args[args.length-1] = change_numeric_when_numeric(str)
    monitor("client","Processing")
    return  MEMCACHED_SET(args)
  end
  # @conv {"PREPEND(args x3)" => ["SELECT","INSERT"]}
  def MEMCACHED_PREPEND(args)
    str = MEMCACHED_GET(args).to_s 
    monitor("client","Processing")
    str = args[args.length-1].to_s + str
    args[args.length-1] = change_numeric_when_numeric(str)
    monitor("client","Processing")
    MEMCACHED_SET(args)
  end
  # @conv {"CAS)" => ["SELECT"]}
  def MEMCACHED_CAS(args)
    args.pop
    MEMCACHED_SET(args)
  end
  # @conv {"INCR" => ["SELECT","INSERT"]}
  def MEMCACHED_INCR(args)
    val = MEMCACHED_GET(args).to_i
    monitor("client","Processing")
    args[args.length-1] = val + args[args.length-1].to_i
    monitor("client","Processing")
    return MEMCACHED_SET(args)
  end
  # @conv {"DECR" => ["SELECT","INSERT"]}
  def MEMCACHED_DECR(args)
    val = MEMCACHED_GET(args).to_i
    monitor("client","Processing")
    args[args.length-1] = val - args[args.length-1].to_i
    monitor("client","Processing")
    return MEMCACHED_SET(args)
  end
  # @conv {"DELETE" => ["DELETE"]}
  def MEMCACHED_DELETE(args)
    table = @option[:keyspace]+"."+@option[:columnfamily]
    command = "DELETE FROM #{table} WHERE key = '#{args[0]}'"
    return DIRECT_EXECUTER(command)
  end
  # @conv {"FLUSH" => ["TRUNCATE"]}
  def MEMCACHED_FLUSH()
    queries = []
    queries.push("drop keyspace if exists #{@option[:keyspace]}")
    queries.push("create keyspace #{@option[:keyspace]} with replication = {'class':'SimpleStrategy','replication_factor':3}")
    @schemas.each{|k,v|
      queries.push(v.createQuery)
    }
    queries.each{|q|
      begin 
        DIRECT_EXECUTER(command)
      rescue => e
        @logger.error(e.message)
        @logger.error(command)
        return false
      end
    }
    return true
  end
  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand,args)
    result = {}
    ## PREPARE SPECIAL OPERATION
    if(["FLUSHALL"].include?(operand))then
      result["operand"] = operand
      return result
    end
    
    ## PREPARE OPERATION & ARGS
    result["operand"] = "MEMCACHED_#{operand.upcase}"
    result["args"] = @parser.exec(operand.upcase, args)
    return result
  end
end
