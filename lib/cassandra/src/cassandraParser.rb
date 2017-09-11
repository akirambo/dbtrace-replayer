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
    @type_position = [0]
    @skipTypes = %w[CREATE EXECUTE_CQL3_QUERY USE].freeze
    @command2primitive = {
      "INSERT" => "INSERT",
      "SELECT" => "READ",
      "SELECT_*" => "SCAN",
      "DELETE" => "UPDATE",
      "UPDATE" => "UPDATE",
      "DROP" => "UPDATE",
      ## JAVA APIs
      "BATCH_MUTATE" => "INSERT",
      "GET" => "SCAN",
      "GET_SLICE" => "SCAN",
      "GET_COUNT" => "SCAN",
      "GET_RANGE_SLICES" => "SCAN",
      "MULTIGET_SLICE" => "SCAN",
      "GET_INDEXED_SLICES" => "SCAN",
    }
    logs = CassandraLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys, option, logger)
  end
  def parse(line)
    case @option[:inputFormat]
    when "cql3"
      parseCQL3(line)
    else
      @logger.error("Unsupported input Format #{@option[:inputFormat]}")
      nil
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
    if line.include?("'query':") &&
       line.include?("Execute CQL3 query") &&
       !line.include?("system") &&
       line != "\n"
      ## Extract Query
      data = ""
      elements = line.chop.split("|")
      elements.each_index do |idx|
        if elements[idx].include?("'query': ")
          data = elements[idx].sub(";", "").gsub("''", '"').gsub(/\\n/, "")
        end
      end
      query = data.split("'query': ")[1]
      if data && query
        if query.include?("', 'serial_consistency_level':")
          query = query.split("', 'serial_consistency_level':")[0]
        end
        if query
          query.sub!("\'}", "")
          query.sub!(/\A\'/, "")
          command = query.split("\s")[0].upcase
          if @supportedCommand.include?(command)
            result = {}
            result[command] = query.split("\s")
            begin
              result = send("parse#{command}_CQL3", result)
            rescue => e
              @logger.error e.message
              @logger.error "Unimplemented #{command}"
              return nil
            end
            return result
          else
            unless @skipTypes.include?(command)
              @logger.warn("Unsupported Command #{command}")
            end
          end
        end
      end
    end
    nil
  end

  def parseINSERT_CQL3(result)
    values_flag = false
    command = "INSERT"
    result[command].each_index do |idx|
      if values_flag && result[command][idx].include?("(")
        result[command][idx].sub!("(", "('")
        result[command][idx].sub!(")", "')")
        result[command][idx].gsub!(",", "','")
        values_flag = false
      end
      if !values_flag && result[command][idx] == "values"
        values_flag = true
      end
    end
    result
  end

  def parseSELECT_CQL3(result)
    target_flag = false
    command = "SELECT"
    result[command].each_index do |idx|
      if target_flag && !result[command][idx].include?("(")
        result[command][idx] = "'#{result[command][idx]}'"
        target_flag = false
      end
      if !target_flag && result[command][idx] == "="
        target_flag = true
      end
    end
    result
  end

  def parseUPDATE_CQL3(result)
    result
  end

  def parseDELETE_CQL3(result)
    result
  end

  def parseDROP_CQL3(result)
    result
  end
end
