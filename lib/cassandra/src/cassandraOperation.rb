
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

module CassandraOperation
  private
  
  #####################
  ## DIRECT EXECUTER ##
  #####################
  def direct_select(query)
    value = {}
    query = normalizeCassandraQuery(query)
    connect
    if @client.syncExecuter(query)
      value = @client.getReply(0)
    end
    add_duration(@client.getDuration, "database", "SELECT")
    close
    value
  end

  def direct_executer(query, onTime = true)
    if query.class == Array
      query.each do |q|
        r = direct_executer(q)
        unless r
          return false
        end
      end
      return true
    else
      value = {}
      query = normalizeCassandraQuery(query)
      connect
      command = query.split(" ")[0].upcase
      if @option[:async] && command != "DROP" && command != "CREATE"
        ## Async Execution
        add_count(query.split(" ")[0])
        @pool_request_size += 1
        @pool_byte_size += query.bytesize
        commit_condition =
          @option[:poolRequestMaxSize] == -1 || (@pool_request_size < @option[:poolRequestMaxSize] && @pool_byte_size < 64_000)
        if !onTime && commit_condition
          ####################
          ## COMMIT QUERIES ##
          ####################
          value = @client.commitQuery(query)
        elsif !onTime && !commit_condition
          value = exec_buffered_queries
          @client.commitQuery(query)
          @pool_request_size = query.bytesize
          @pool_byte_size = 1
        elsif onTime
          exec_buffered_queries
          @client.syncExecuter(query)
          value = @client.getReply(0)
        end
      else
        ## Sync Execution
        @client.syncExecuter(query)
        add_duration(@client.getDuration, "database", query.split(" ")[0].upcase)
        if query.split(" ")[0].casecmp("SELECT")
          value = @client.getReply(0)
        end
      end
      close
    end
    value
  end
  
  #############
  ## PREPARE ##
  #############
  def prepare_cassandra(operand, args)
    ## PREPARE OPERATION & ARGS
    result = { "operand" => "direct_executer" }
    result["args"] = if operand.casecmp("batch_mutate").zero? ||
                        operand.casecmp("get_slice").zero? ||
                        operand.casecmp("get_range_slice").zero? ||
                        operand.casecmp("get_indexed_slice").zero? ||
                        operand.casecmp("multi_get_slices").zero?
                       send("prepare_#{operand.downcase}", args)
                     else
                       args.join(" ")
                     end
    result
  end

  def normalizeCassandraQuery(query)
    query.tr!("\"", "'")
    query.tr!('"', "'")
    query.delete!("-")
    query.gsub!("__DOUBLEQ__", '"')
    unless query.include?(";")
      query += ";"
    end
    query
  end

  def exec_buffered_queries
    @metrics.start_monitor("database", "AsyncExec")
    @client.asyncExecuter
    add_total_duration(@client.getDuration, "database")
    @metrics.end_monitor("database", "AsyncExec")
    @client.resetQuery
    @pool_request_size = 0
    @pool_byte_size = 0
    value = @client.getReply(0)
    value
  end

  ##-------------------##
  ##--- BatchMutate ---##
  ##-------------------##
  def prepare_batch_mutate(args)
    result = @parser.parse_batch_mutate_parameter(args)
    if !result["counterColumn"]
      if !result["keyValue"].keys.empty?
        keys = result["rowKey"] + "," + result["keyValue"].keys.join(",")
        values = '"' + result["rowValue"] + '","' + result["keyValue"].values.join('","') + '"'
      else
        @logger.fatal("Fatal Error@BATCH_MUTATE")
        return ""
      end
      query = "INSERT INTO #{result["table"]} (#{keys}) VALUES(#{values})".delete("\"")
      return query
    else
      ## Counter Column
      queries = []
      result["keyValue"].each_index do |index|
        where = []
        where.push("#{result["rowKey"]} = #{result["rowValue"]}")
        result["keyValue"][index].each do |ck, cv|
          where.push("#{ck} = '#{cv}'")
        end
        result["counterKeyValue"][index].each do |k, v|
          q = "UPDATE #{result["table"]} SET #{k} = #{k} + #{v} "
          q += " WHERE #{where.join(" AND ")}"
          queries.push(q)
        end
      end
      queries
    end
  end
  
  ##------------------------##
  ##--- GET_RANGE_SLICES ---##
  ##------------------------##
  def prepare_get_range_slices(args)
    result = @parser.parse_get_range_slices_parameter(args)
    query = "SELECT * FROM #{result["table"]}"
    if result["start_key"] && result["end_key"]
      query += " WHERE #{result["primaryKey"]} >= #{result["start_key"]}"
      query += " AND #{result["primaryKey"]} <= #{result["end_key"]}"
    elsif result["start_key"]
      query += " WHERE #{result["primaryKey"]} >= #{result["start_key"]}"
    elsif result["end_key"]
      query += " WHERE #{result["primaryKey"]} <= #{result["end_key"]}"
    end
    if result["count"]
      query += " limit #{result["count"]}"
    end
    query += ";"
    query
  end

  ##-----------------##
  ##--- GET_SLICE ---##
  ##-----------------##
  def prepare_get_slice(args)
    result = @parser.parse_get_slice_parameter(args)
    query = "SELECT * FROM #{result["table"]} WHERE #{result["primaryKey"]} = #{result["targetKey"]}"
    if result["count"]
      query += " limit #{result["count"]}"
    end
    query += ";"
    query
  end

  ##--------------------------##
  ##--- GET_INDEXED_SLICES ---##
  ##--------------------------##
  def prepare_get_indexed_slices(args)
    @parser.parse_get_indexed_slices_parameter(args)
  end

  ##----------------------##
  ##--- MULTIGET_SLICE ---##
  ##----------------------##
  def prepare_multiget_slice(args)
    result = @parser.parse_multi_get_slice_parameter(args)
    query = "SELECT * FROM #{result["table"]}"
    query += " WHERE #{result["primaryKey"]} IN (#{result["keys"].join(",")});"
    query
  end
end
