
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

module Mongodb2CassandraOperation
  MONGODB_NUMERIC_QUERY = %w[$gt $gte $lt $lte].freeze
  MONGODB_STRING_QUERY = %w[$eq $ne $in $nin].freeze

  private

  # @conv {"INSERT" => ["INSERT"]}
  def MONGODB_INSERT(args)
    args.each do |arg|
      name = arg[0]
      kv = mongo_insert_check(arg)
      if kv == false
        return false
      end
      ## convert _id --> monogoid ( only @schema Hash _id)
      if kv.keys.include?("_id") &&
         @schemas[name] && @schemas[name].fields
        kv["mongoid"] = kv["_id"].delete("-")
        kv.delete("_id")
      end
      if !@schemas[name].nil?
        ## args[2] == true means bulk
        return mongo_bulk_schemas(kv, name, arg[2])
      else
        return false
      end
    end
    true
  end

  # @conv {"UPDATE" => ["UPDATE"]}
  def MONGODB_UPDATE(arg)
    name = arg["key"]
    command = "UPDATE #{name} SET"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    updates = []
    if arg["update"] && arg["update"]["$set"]
      arg["update"]["$set"].each do |key, value|
        if @schemas[name].stringType(key)
          updates.push(" #{key}='#{value}'")
        end
      end
      command += updates.join(",")
    end
    ## EXTRACT QUERY
    query = mongodbParseQuery(arg["query"])
    if arg["query"] || query
      if query != ""
        command += " WHERE " + query
      end
    end
    begin
      DIRECT_EXECUTER(command + ";")
    rescue => e
      @logger.error(e.message)
      @logger.error("Execute Command -- #{command}")
      return false
    end
    true
  end

  # @conv {"FIND" => ["SELECT"]}
  def MONGODB_FIND(arg)
    name = arg["key"]
    command = "SELECT * FROM #{name}"
    result = true
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    query = mongodbParseQuery(arg["filter"])
    if arg["filter"] || query
      unless query.empty?
        command += " WHERE " + query + " ALLOW FILTERING"
      end
    end
    begin
      result = DIRECT_EXECUTER(command + ";")
    rescue => e
      @logger.error("QUERY :: #{command}")
      @logger.error(e.message)
      result = ""
    end
    result
  end

  # @conv {"COUNT" => ["SELECT"]}
  def MONGODB_COUNT(arg)
    command = "SELECT count(*) FROM #{arg["key"]}"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    query = mongodbParseQuery(arg["filter"])
    if arg["filter"] && query
      command += " WHERE " + query
    end
    @logger.debug("Execute Command -- #{command}")
    begin
      result = DIRECT_EXECUTER(command + ";")
      if result.class == Hash
        return 0
      end
      return result.to_i
    rescue => e
      @logger.error("QUERY :: #{command}")
      @logger.error(e.message)
    end
    0
  end

  # @conv {"DELETE" => ["DELETE"]}
  def MONGODB_DELETE(arg)
    name = arg["key"]
    if arg["filter"].nil? || arg["filter"].size.zero?
      command = "TRUNCATE #{name};"
      begin
        DIRECT_EXECUTER(command)
      rescue
        return false
      end
    else
      command = "DELETE FROM #{name}"
      ## EXTRACT QUERY
      query = mongodbParseQuery(arg["filter"])
      if arg["filter"] && query
        where = " WHERE " + query
      end
      @logger.debug("Execute Command -- #{command} #{where}")
      exec = "#{command}#{where};"
      begin
        DIRECT_EXECUTER(exec)
      rescue => e
        @logger.error(e.message)
        @logger.error(command)
        return false
      end
    end
    true
  end

  # @conv {"AGGREGATE" => ["SELECT"]}
  def MONGODB_AGGREGATE(arg)
    name = @options[:keyspace] + "." + arg["key"]
    if arg["key"].include?(".")
      name = arg["key"]
    end
    arg.delete("key")
    target_keys = @queryParser.targetKeys(arg)
    command = if target_keys.empty?
                "SELECT * FROM #{name}"
              else
                "SELECT " + target_keys.join(",") + " FROM #{name}"
              end
    if arg["match"]
      where = []
      arg["match"].each do |k, v|
        ## primary key ?
        if @schemas[name].primaryKeys.include?(k)
          where.push("#{k} = '#{v}'")
        end
      end
      unless where.empty?
        command += " WHERE #{where.join(" AND ")}"
      end
    end
    begin
      ans = DIRECT_EXECUTER(command + ";")
      docs = @queryParser.csv2docs(target_keys, ans)
      params = @queryParser.getParameter(arg)
      result = {}
      docs.each do |doc|
        ## create group key
        key = @queryParser.createGroupKey(doc, params["cond"])
        if result[key].nil?
          result[key] = {}
        end
        params["cond"].each do |k, v|
          monitor("client", "aggregate")
          result[key][k] = @queryProcessor.aggregation(result[key][k], doc, v)
          monitor("client", "aggregate")
        end
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    result
  end

  #############
  ## PREPARE ##
  #############
  def prepare_mongodb(operand, args)
    result = { "operand" => "MONGODB_#{operand}", "args" => nil }
    result["args"] = @parser.exec(operand, args)
    result
  end

  def mongodbParseQuery(hash)
    where = []
    id_where = []
    hash.each do |col, queries|
      if queries.class == Hash
        queries.each do |operand, value|
          case operand
          when "$gt" then
            where.push("#{col} > #{value}")
          when "$gte" then
            where.push("#{col} >= #{value}")
          when "$lt" then
            where.push("#{col} < #{value}")
          when "$lte" then
            where.push("#{col} <= #{value}")
          else
            @logger.warn("Unsupported #{operand}")
          end
        end
      elsif col == "_id"
        id_where.push("mongoid = '#{queries.delete("-")}'")
      else
        where.push("#{col} = '#{queries}'")
      end
    end
    query = where.join(" AND ")
    if !query.size.zero? && !id_where.size.zero?
      query += " AND (" + id_where.join(" OR ") + ")"
    elsif query.size.zero? && !id_where.size.zero?
      return id_where.join(" OR ")
    end
    where.join(" AND ")
  end

  def mongo_insert_check(arg)
    kv = nil
    if arg[1].nil?
      return false
    end

    if arg[1].class == String
      begin
        kv = parse_json(arg[1])
      rescue => e
        @logger.error(e.message)
        return false
      end
    elsif arg[1].class == Hash
      kv = arg[1]
    elsif arg[1].class == Array
      kv = parse_json(arg[1][0])
    end
    if kv.nil? || kv.keys.empty?
      return false
    end
    kv
  end

  def mongo_bulk_schemas(kv, name, arg)
    if @schemas[name].check(kv, arg)
      filtered_kv = @schemas[name].extractKeyValue(kv)
      command = "INSERT INTO #{name} "
      command += "(" + filtered_kv["key"] + ") VALUES "
      command += "(" + filtered_kv["value"] + ");"
      begin
        DIRECT_EXECUTER(command)
      rescue => e
        @logger.error("Cannot Execute Command #{command}")
        @logger.error(" - Error Message #{e.message}")
        return false
      end
    else
      @logger.warn("Not Found Table [#{name}]. Please Create Table @ INSERT .")
      return false
    end
    true
  end
end
