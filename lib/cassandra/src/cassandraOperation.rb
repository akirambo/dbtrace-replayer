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
  def DIRECT_SELECT(query)
    value = {}
    query = normalizeCassandraQuery(query)
    connect 
    if(@client.syncExecuter(query))then
      value = @client.getReply(0)
    end
    add_duration(@client.getDuration(),"database", "SELECT")
    close
    return value
  end
  def DIRECT_EXECUTER(query,onTime=true)
    if(query.class == Array)then
      query.each{|q|
        r = DIRECT_EXECUTER(q)
        if(!r)then
          return false
        end
      }
      return true
    else
      value = {}
      query = normalizeCassandraQuery(query)
      connect
      command = query.split(" ")[0].upcase()
      if(@option[:async] and
          command != "DROP" and command != "CREATE")then
        ## Async Execution
        add_count(query.split(" ")[0])
        @pool_request_size += 1
        @poolByteSize += query.bytesize
        commitCondition = 
          @option[:poolRequestMaxSize] == -1 or
          (@pool_request_size < @option[:poolRequestMaxSize] and
          @poolByteSize < 64000)
        if(!onTime and commitCondition)then
          ####################
          ## COMMIT QUERIES ##
          ####################
          value = @client.commitQuery(query)
        elsif(!onTime and !commitCondition)then
          value = execBufferedQueries()
          @client.commitQuery(query)
          @pool_request_size = query.bytesize
          @poolByteSize = 1
        elsif(onTime)then
          execBufferedQueries()
          @client.syncExecuter(query)
          value = @client.getReply(0)
        end
      else
        ## Sync Execution
        @client.syncExecuter(query)
        add_duration(@client.getDuration(),"database", query.split(" ")[0].upcase)
        if(query.split(" ")[0].upcase == "SELECT")then
          value = @client.getReply(0)
        end
      end
      close
    end
    return value
  end
  
  #############
  ## PREPARE ##
  #############
  def prepare_cassandra(operand, args)
    ## PREPARE OPERATION & ARGS
    result = {"operand" => "DIRECT_EXECUTER"}
    if(operand.upcase == "BATCH_MUTATE" or 
        operand.upcase == "GET_SLICE" or 
        operand.upcase == "GET_RANGE_SLICES" or 
        operand.upcase == "GET_INDEXED_SLICES" or 
        operand.upcase == "MULTIGET_SLICE")then
      result["args"] = send("prepare_#{operand.upcase}",args)
    else
      result["args"] = args.join(" ")
    end
    return result
  end

  def normalizeCassandraQuery(query)
    query.gsub!("\"","'")
    query.gsub!('"',"'")
    query.gsub!('-',"")
    query.gsub!('__DOUBLEQ__','"')
    if(!query.include?(";"))then
      query += ";"
    end
    return query
  end

  def execBufferedQueries()
    @metrics.start_monitor("database","AsyncExec")
    @client.asyncExecuter()
    add_total_duration(@client.getDuration(),"database")
    @metrics.end_monitor("database","AsyncExec")
    @client.resetQuery()
    @pool_request_size = 0
    @poolByteSize = 0    
    value = @client.getReply(0)
    return value
  end

  ######################
  ##---- JAVA API ----##
  ######################

  
  ##-------------------##
  ##--- BatchMutate ---##
  ##-------------------##
  def prepare_BATCH_MUTATE(args) 
    result = @parser.parseBatchMutateParameter(args)
    if(!result["counterColumn"])then
      if(result["keyValue"].keys.size > 0)then
        keys   = result["rowKey"]+','+result["keyValue"].keys().join(',')
        values = '"'+result["rowValue"]+'","'+ result["keyValue"].values().join('","') +'"'
      else
        @logger.fatal("Fatal Error@BATCH_MUTATE")
        return ""
      end
      query = "INSERT INTO #{result["table"]} (#{keys}) VALUES(#{values})".gsub(/\"/,"")
      return query
    else
      ## Counter Column
      queries = []
      result["keyValue"].each_index{|index|
        where = []
        where.push("#{result["rowKey"]} = #{result["rowValue"]}")
        result["keyValue"][index].each{|ck,cv|
          where.push("#{ck} = '#{cv}'")
        }
        result["counterKeyValue"][index].each{|k,v|
          q = "UPDATE #{result["table"]} SET #{k} = #{k} + #{v} "
          q += " WHERE #{where.join(" AND ")}"
          queries.push(q)
        }
      }
      return queries
    end
  end
  
  ##------------------------##
  ##--- GET_RANGE_SLICES ---##
  ##------------------------##
  def prepare_GET_RANGE_SLICES(args)
    result = @parser.parseGetRangeSlicesParameter(args)
    query = "SELECT * FROM #{result["table"]}"
    if(result["start_key"] and result["end_key"])then
      query += " WHERE #{result["primaryKey"]} >= #{result["start_key"]}"
      query += " AND #{result["primaryKey"]} <= #{result["end_key"]}"
    elsif(result["start_key"])then
      query += " WHERE #{result["primaryKey"]} >= #{result["start_key"]}"
    elsif(result["end_key"])then
      query += " WHERE #{result["primaryKey"]} <= #{result["end_key"]}"
    end
    if(result["count"])then
      query += " limit #{result["count"]}"
    end
    return query += ";"
  end


  ##-----------------##
  ##--- GET_SLICE ---##
  ##-----------------##
  def prepare_GET_SLICE(args)
    result = @parser.parseGetSliceParameter(args)
    query = "SELECT * FROM #{result["table"]} WHERE #{result["primaryKey"]} = #{result["targetKey"]}"
    if(result["count"])then
      query += " limit #{result["count"]}"
    end
    return query +";"
  end

  ##--------------------------##
  ##--- GET_INDEXED_SLICES ---##
  ##--------------------------##
  def prepare_GET_INDEXED_SLICES(args)
    result = @parser.parseGETINDEXEDSLICESParameter(args)
    return result
  end

  ##----------------------##
  ##--- MULTIGET_SLICE ---##
  ##----------------------##
  def prepare_MULTIGET_SLICE(args)
    result = @parser.parseMULTIGETSLICEParameter(args)
    query  = "SELECT * FROM #{result["table"]}"
    query += " WHERE #{result["primaryKey"]} IN (#{result["keys"].join(",")});" 
    return query
  end  
end
