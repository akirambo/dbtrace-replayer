
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
  require_relative "../../mongodb/src/mongodb_utils"
  include MongodbUtils
  MONGODB_NUMERIC_QUERY = %w[$gt $gte $lt $lte].freeze
  MONGODB_STRING_QUERY = %w[$eq $ne $in $nin].freeze

  private

  # @conv {"INSERT" => ["INSERT"]}
  def mongodb_insert(args)
    args.each do |arg|
      name = mongodb_get_table(arg[0])
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
      if @schemas[name]
        ## args[2] == true means bulk
        return mongodb_bulk_schemas(kv, name, arg[2])
      else
        return false
      end
    end
    true
  end

  def mongodb_upsert(args)
    mongodb_insert(args)
  end

  # @conv {"UPDATE" => ["UPDATE"]}
  def mongodb_update(arg)
    name = mongodb_get_table(arg["key"])
    command = "UPDATE #{name} SET"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    updates = []
    if arg["update"] && arg["update"]["$set"]
      arg["update"]["$set"].each do |key, value|
        if @schemas[name].string_type(key)
          updates.push(" #{key}='#{value}'")
        end
      end
      command += updates.join(",")
    end
    mongodb_update_exec(command, arg)
  end

  def mongodb_update_exec(command, arg)
    ## EXTRACT QUERY
    query = mongodb_parse_query(arg["query"])
    if arg["query"] || query
      if query != ""
        command += " WHERE " + query
      end
    end
    begin
      direct_executer(command + ";")
    rescue => e
      @logger.error(e.message)
      @logger.error("Execute Command -- #{command}")
      return false
    end
    true
  end

  # @conv {"FIND" => ["SELECT"]}
  def mongodb_find(arg)
    name = mongodb_get_table(arg["key"])
    command = "SELECT * FROM #{name}"
    result = true
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    query = mongodb_parse_query(arg["filter"])
    if arg["filter"] || query
      unless query.empty?
        command += " WHERE " + query + " ALLOW FILTERING"
      end
    end
    begin
      result = direct_executer(command + ";")
    rescue => e
      @logger.error("QUERY :: #{command}")
      @logger.error(e.message)
      result = ""
    end
    result
  end

  # @conv {"COUNT" => ["SELECT"]}
  def mongodb_count(arg)
    name = mongodb_get_table(arg["key"])
    command = "SELECT count(*) FROM #{name}"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    query = if arg["filter"]
              mongodb_parse_query(arg["filter"])
            end
    if arg["filter"] && query
      command += " WHERE " + query
    end
    @logger.debug("Execute Command -- #{command}")
    begin
      result = direct_executer(command + ";")
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
  def mongodb_delete(arg)
    name = mongodb_get_table(arg["key"])
    if arg["filter"].nil? || arg["filter"].size.zero?
      command = "TRUNCATE #{name};"
      begin
        direct_executer(command)
      rescue
        return false
      end
      return true
    else
      return mongodb_delete_exec(arg)
    end
  end

  def mongodb_delete_exec(arg)
    name = mongodb_get_table(arg["key"])
    command = "DELETE FROM #{name}"
    ## EXTRACT QUERY
    query = mongodb_parse_query(arg["filter"])
    if arg["filter"] && query
      where = " WHERE " + query
    end
    @logger.debug("Execute Command -- #{command} #{where}")
    exec = "#{command}#{where};"
    begin
      direct_executer(exec)
    rescue => e
      @logger.error(e.message)
      @logger.error(command)
      return false
    end
    true
  end

  # @conv {"AGGREGATE" => ["SELECT"]}
  def mongodb_aggregate(arg)
    name = mongodb_get_table(arg["key"])
    arg.delete("key")
    target_keys = @query_parser.targetkeys(arg)
    command = if target_keys.empty?
                "SELECT * FROM #{name}"
              else
                "SELECT " + target_keys.join(",") + " FROM #{name}"
              end
    if arg["match"]
      where = []
      if arg["match"].class == String
        arg["match"] = eval(arg["match"])
      end
      arg["match"].each do |k, v|
        ## primary key ?
        key = k.to_s
        if @schemas[name].primarykeys.include?(key)
          where.push("#{key} = '#{v}'")
        end
      end
      unless where.empty?
        command += " WHERE #{where.join(" AND ")}"
      end
    end
    mongodb_aggregate_exec(command, target_keys, arg)
  end

  def mongodb_aggregate_exec(command, target_keys, arg)
    begin
      ans = direct_executer(command + ";")
      docs = @query_parser.csv2docs(target_keys, ans)
      params = @query_parser.get_parameter(arg)
      result = {}
      docs.each do |doc|
        ## create group key
        key = @query_parser.create_groupkey(doc, params["cond"])
        if result[key].nil?
          result[key] = {}
        end
        params["cond"].each do |k, v|
          monitor("client", "aggregate")
          result[key][k] = @query_processor.aggregation(result[key][k], doc, v)
          monitor("client", "aggregate")
        end
      end
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    result
  end

  def mongodb_mapreduce(_)
    @logger.warn("Unsupported Mapreduce Command")
  end

  def mongodb_group(_)
    @logger.warn("Unsupported Group Command")
  end

  def mongodb_parse_query(hash)
    where = []
    id_where = []
    hash.each do |col, queries|
      if queries.class == Hash
        result = mongodb_parse_queries(queries, col)
        unless result.empty?
          where.concat(result)
        end
      elsif col == "_id"
        id_where.push("mongoid = '#{queries.delete("-")}'")
      else
        where.push("#{col} = '#{queries}'")
      end
    end
    mongodb_concat_condition(where, id_where)
  end

  def mongodb_concat_condition(where, id_where)
    if where.empty? && !id_where.empty?
      return id_where.join(" OR ")
    elsif !where.empty? && !id_where.size.zero?
      return where.join(" AND ") + " AND (" + id_where.join(" OR ") + ")"
    end
    where.join(" AND ")
  end

  def mongodb_parse_queries(queries, col)
    where = []
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
    where
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
    check_kv(kv)
  end

  def check_kv(kv)
    if kv.nil? || kv.keys.empty?
      return false
    end
    kv
  end

  def mongodb_bulk_schemas(kv, name, arg)
    if @schemas[name].check(kv, arg)
      filtered_kv = @schemas[name].extract_keyvalue(kv)
      command = "INSERT INTO #{name} "
      command += "(" + filtered_kv["key"] + ") VALUES "
      command += "(" + filtered_kv["value"] + ");"
      cassandra_exec(command)
    else
      @logger.warn("Not Found Table [#{name}]. Please Create Table @ INSERT .")
      false
    end
  end

  def mongodb_get_table(default)
    name = @option[:keyspace] + "."
    name += if default.include?(".")
              default.split(".")[1]
            else
              default
            end
    name
  end

  def cassandra_exec(command)
    begin
      direct_executer(command)
    rescue => e
      @logger.error("Cannot Execute Command #{command}")
      @logger.error(" - Error Message #{e.message}")
      return false
    end
    true
  end
end
