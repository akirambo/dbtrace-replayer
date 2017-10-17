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
    @skip_types = %w[CREATE EXECUTE_CQL3_QUERY USE].freeze
    @command2primitive = {
      "insert" => "INSERT",
      "select" => "READ",
      "select_*" => "SCAN",
      "delete" => "UPDATE",
      "update" => "UPDATE",
      "drop" => "UPDATE",
      ## JAVA APIs
      "batch_mutate" => "INSERT",
      "get" => "SCAN",
      "get_slice" => "SCAN",
      "get_count" => "SCAN",
      "get_range_slices" => "SCAN",
      "multiget_slice" => "SCAN",
      "get_indexed_slices" => "SCAN",
    }
    logs = CassandraLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys, option, logger)
  end

  def parse(line)
    case @option[:inputFormat]
    when "cql3"
      parse_cql3(line)
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
  def parse_cql3(line)
    if has_query?(line)
      ## Extract Query
      query = extract_query(line)
      if query
        query.sub!("\'}", "")
        query.sub!(/\A\'/, "")
        command = query.split("\s")[0].downcase
        if @supported_command.include?(command)
          result = {}
          result[command] = query.split("\s")
          begin
            result = send("parse_#{command}_cql3", result)
          rescue => e
            @logger.error e.message
            @logger.error "Unimplemented #{command}"
            return nil
          end
          return result
        else
          unless @skip_types.include?(command)
            @logger.warn("Unsupported Command #{command}")
          end
        end
      end
    end
    nil
  end

  def has_query?(line)
    line.include?("'query':") &&
      line.include?("Execute CQL3 query") &&
      !line.include?("system") &&
      line != "\n"
  end

  def extract_query(line)
    data = ""
    elements = line.chop.split("|")
    elements.each_index do |idx|
      if elements[idx].include?("'query': ")
        data = elements[idx].sub(";", "").gsub("''", '"').gsub(/\\n/, "")
      end
    end
    query = data.split("'query': ")[1]
    if query && query.include?("', 'serial_consistency_level':")
      query = query.split("', 'serial_consistency_level':")[0]
    end
    query
  end

  def parse_insert_cql3(result)
    values_flag = false
    command = "insert"
    result[command].each_index do |idx|
      if values_flag && result[command][idx].include?("(")
        result[command][idx].gsub!('"', "'")
        #result[command][idx].sub!(")", "')")
        #result[command][idx].gsub!(",", "','")
        values_flag = false
      end
      if !values_flag && result[command][idx] == "values"
        values_flag = true
      end
    end
    result
  end

  def parse_select_cql3(result)
    target_flag = false
    command = "select"
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

  def parse_update_cql3(result)
    result
  end

  def parse_delete_cql3(result)
    result
  end

  def parse_drop_cql3(result)
    result
  end
end
