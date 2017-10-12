
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

class CassandraSchema
  attr_reader :keyspace, :table, :primarykeys,
              :create_keyspace_query, :create_query,
              :drop_query, :create_indexes

  def initialize(keyspace, create_query, logger)
    @keyspace = keyspace
    @table = nil
    @primarykeys = nil
    @fields = {} ## key => type
    @create_query = nil
    @create_keyspace_query = nil
    @create_indexes = []
    @logger = logger
    @values = []
    @drop_query = nil
    parse(create_query)
  end

  def name
    @keyspace + "." + @table
  end

  def check(kv, bulk = false)
    kv.each do |k, v|
      if bulk
        if @fields.keys && !kv.keys.zero? &&
           @fields.keys.size >= kv.keys.size
          k = @fields.keys[kv.keys.index(k)]
        else
          @logger.warn("Please Set Fields bulk0, ... bulk#{@fields.keys.size}")
          return false
        end
      end
      unless check_after(k, v)
        return false
      end
    end
    true
  end

  def check_after(k, v)
    if @fields[k]
      unless check_field_type(v, @fields[k])
        @logger.error("Unmatch DataType #{k} is not #{@fields[k]} AT #{name}")
        @logger.error("                 #{k} is #{v.class}")
        @logger.error("Under Construction Auto Table Creation")
        return false
      end
    else
      ## Skip Field
      @logger.debug("Skip Field:: #{k}")
    end
    true
  end

  def extract_keyvalue(kv)
    keys = []
    values = []
    kvs = { "key" => [], "value" => [] }
    kv.each do |key_, value|
      key = key_.delete("_")
      @logger.debug("KEY    :: #{key} IN Fields :: #{@fields.keys}")
      if @fields.keys.include?(key)
        keys.push(key)
        values.push(extract_keyvalue_normalize(value))
      else
        @logger.error("ERROR KEY : #{key} => #{value} (#{value.class})")
      end
    end
    kvs["key"] = keys.join(",")
    kvs["value"] = values.join(",")
    kvs
  end

  def extract_keyvalue_normalize(value)
    v = case value.class.to_s
        when "Array" then
          extract_keyvalue_from_array(value)
        when "Hash" then
          extract_keyvalue_from_hash(value)
        when "Fixnum" then
          extract_keyvalue_from_fixnum(key, value)
        when "String" then
          extract_keyvalue_from_string(value)
        when "Float" then
          value.to_f
        else
          "'" + value.to_s + "'"
        end
    v
  end

  def extract_keyvalue_from_array(value)
    ret = "{}"
    if value.size > 1
      if value[0].class == Hash
        hash = []
        value.each do |hash__|
          string = hash__.to_json
          string.delete!("\"")
          hash.push(string.to_s)
        end
        ret = "{\"#{hash.join(",")}\"}"
      else
        ret = "{#{value.join(",")}}"
      end
    end
    ret
  end

  def extract_keyvalue_from_hash(value)
    val_ = []
    value.each do |k_, v_|
      if v_.class == Hash || v_.class == Array
        newval = "'#{k_}':'#{v_.to_json}'"
        val_.push(newval.gsub('"', "__DOUBLEQ__"))
      else
        val_.push("'#{k_}':'#{v_}'")
      end
    end
    "{" + val_.join(",") + "}"
  end

  def extract_keyvalue_from_fixnum(key, value)
    if @fields[key] == "INT"
      value.to_i
    elsif @fields[key] == "TEXT"
      "'" + value.to_s + "'"
    end
  end

  def extract_keyvalue_from_string(value)
    if value.include?("(")
      "'" + value.sub("('", "").sub("')", "") + "'"
    else
      "'" + value + "'"
    end
  end

  def fields
    @fields.keys
  end

  def keys(size)
    size -= 1
    @fields.keys[0..size]
  end

  def get_key(index)
    @fields.keys[index]
  end

  def field_type(field)
    @fields[field]
  end

  def counter_key
    @fields.each do |key, value|
      if value == "counter"
        return key
      end
    end
    nil
  end

  def primarykey_type
    @fields[@primarykeys[0]]
  end

  def string_type(field)
    (@fields[field].casecmp("varchar").zero? || @fields[field].casecmp("text").zero?)
  end

  def push_create_index(query)
    @create_indexes.push(query)
  end

  private

  def parse(create_query)
    parse_prepare(create_query)
    field = extract_field
    keep = nil
    field.each do |kv|
      if !kv.include?("primary key") && !kv.include?("PRIMARY KEY")
        extract_keyvalue_from_field(kv, keep)
      end
    end
  end

  def extract_keyvalue_from_field(kv, keep)
    if kv.include?("<") && !kv.include?(">")
      keep = kv
    elsif keep
      keep += "," + kv
      if kv.include?(">")
        ## Finish
        extract_keyvalue_for_priv(keep)
        keep = nil
      end
    else
      extract_keyvalue_for_priv(kv)
    end
    keep
  end

  def parse_prepare(create_query)
    table = ""
    if create_query.include?("CREATE TABLE")
      table = create_query.split("CREATE TABLE ")[1].split(" ")[0]
    elsif create_query.include?("create table")
      table = create_query.split("create table ")[1].split(" ")[0]
    end
    @keyspace = table.split(".")[0]
    @create_keyspace_query =
      "create keyspace if not exists #{@keyspace} with replication = {'class':'SimpleStrategy','replication_factor':3}"
    @table = table.split(".")[1]
    @primarykeys = create_query.downcase.split("primary key")[1].delete("(").delete(")").delete(" ").delete(";").split(",")
    @create_query = create_query.delete("_")
    @create_query.sub!(@keyspace.delete("_"), @keyspace)
    @create_query.sub!(@table.delete("_"), @table)
    if !@create_query.include?("if not exists") &&
       !@create_query.include?("IF NOT EXISTS")
      @create_query.gsub!("create table", "create table if not exists")
      @create_query.gsub!("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    end
    @drop_query = "drop table if exists #{@keyspace}.#{@table}"
  end

  def extract_field
    field_ = ""
    if @keyspace
      field_ = create_query.split(" #{@keyspace}.#{@table} ")[1]
      if field_.include?("primary key")
        field_ = field_.split("primary key")[0]
      elsif field_.include?("PRIMARY KEY")
        field_ = field_.split("PRIMARY KEY")[0]
      end
    end
    field_ = field_.delete("(").delete(")").delete("\t").delete("\n", "")
    field_.split(",")
  end

  def check_field_type(value, schema_type_)
    schema_type = schema_type_.downcase
    case value.class.to_s
    when "String" then
      return (schema_type == "varchar" || schema_type == "text")
    when "Integer" then
      return schema_type == "int"
    when "Float" then
      return schema_type == "float"
    when "Array" then
      return (schema_type.include?("set") || schema_type.include?("map"))
    when "TrueClass" then
      return schema_type == "text"
    when "FalseClass" then
      return schema_type == "text"
    when "Hash" then
      return schema_type.include?("map")
    else
      @logger.error("Unsupport field type #{value.class}")
    end
    false
  end

  def extract_keyvalue_for_priv(string)
    kv = string.sub(/^\s*/, "").split(" ")
    if kv.size == 2
      field_name = kv[0].delete(" ", "")
      field_type = kv[1].delete(" ", "")
      return @fields[field_name] = field_type
    end
  end
end
