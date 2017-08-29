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

require_relative "cassandraLog"
require_relative "../../common/abstractDBParser"

class CassandraParser < AbstractDBParser
  def initialize(filename, option, logger)
    @typePosition = [0]
    @skipTypes     = ["CREATE","EXECUTE_CQL3_QUERY","USE"]
    @command2primitive = {
      "INSERT"   => "INSERT",
      "SELECT"   => "READ",
      "SELECT_*" => "SCAN",
      "DELETE"   => "UPDATE",
      "UPDATE"   => "UPDATE",
      "DROP"     => "UPDATE",
      ## JAVA APIs
      "BATCH_MUTATE" => "INSERT",
      "GET"          => "SCAN",
      "GET_SLICE"    => "SCAN",
      "GET_COUNT"    => "SCAN",
      "GET_RANGE_SLICES"  => "SCAN",
      "MULTIGET_SLICE"     => "SCAN",
      "GET_INDEXED_SLICES" => "SCAN"
    }
    logs = CassandraLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys(), option, logger)
  end
  def parse(line)
    case @option[:inputFormat] 
    when "cql3" then
      return parseCQL3(line)
    else
      @logger.error("Unsupported input Format #{@option[:inputFormat]}")
      return nil
    end
  end
  
  #########################################################################
  ## This trace type is able to get the following command @cql
  ## ----------------------------------------------------------------------
  ## % tracing on
  ## % query
  ## % select parameters,request,started_at from system_traces.sessions;
  #########################################################################
  def parseCQL3(line)
    if(line.include?("'query':") and 
       line.include?("Execute CQL3 query") 
       !line.include?("system") and 
         line != "\n")then
      ## Extract Query
      data = ""
      elements = line.chop.split("|")
      elements.each_index{|idx|
        if(elements[idx].include?("'query': "))then
          data = elements[idx].sub(";","").gsub("''",'"').gsub(/\\n/,"")
        end
      }
      query = nil
      query = data.split("'query': ")[1]
      if(data and query = data.split("'query': ")[1])then
        if(query.include?("', 'serial_consistency_level':"))then
          query = query.split("', 'serial_consistency_level':")[0]
        end
        if(query)then
          query.sub!("\'}","")
          query.sub!(/\A\'/,"")
          command = query.split("\s")[0].upcase
          if(@supportedCommand.include?(command))then
            result = Hash.new
            result[command] = query.split("\s")
            begin 
              result = send("parse#{command}_CQL3",result)
            rescue => e
              @logger.error e.message
              @logger.error "Unimplemented #{command}"
              return nil
            end
            return result
          else
            if(!@skipTypes.include?(command))then
              @logger.warn("Unsupported Command #{command}")
            end
          end
        end
      end
    end
    return nil
  end
=begin
  def parseJavaAPI(line)
    data = line.split("|")
    ## parameters|request|started_at 
    if(data.size == 3)then
      parameters = data[0].sub(/\A\s+/,"")
      command = data[1].gsub(/\s/,"").upcase
      if(@supportedCommand.include?(command))then
        result = Hash.new
        result[command] = parameters
        return result
      else
        if(!@skipTypes.include?(command))then
          @logger.warn("Unsupported Command #{command}")
        end
      end
    end
    return nil
  end
=end
  
  def parseINSERT_CQL3(result)
    valuesFlag = false
    command = "INSERT"
    result[command].each_index{|idx|
      if(valuesFlag and result[command][idx].include?("("))then
        result[command][idx].sub!("(","('")
        result[command][idx].sub!(")","')")
        result[command][idx].gsub!(",","','")
        valuesFlag = false
      end
      if(!valuesFlag and result[command][idx] == "values")then
        valuesFlag = true
      end
    }
    return result
  end
  def parseSELECT_CQL3(result)
    targetFlag = false
    command = "SELECT"
    result[command].each_index{|idx|
      if(targetFlag and !result[command][idx].include?("("))then
        result[command][idx] = "'#{result[command][idx]}'"
        targetFlag = false
      end
      if(!targetFlag and result[command][idx] == "=")then
        targetFlag = true
      end
    }
    return result
  end
  def parseUPDATE_CQL3(result)
    return result
  end
  def parseDELETE_CQL3(result)
    return result
  end
  def parseDROP_CQL3(result)
    return result
  end
end
