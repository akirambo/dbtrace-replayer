
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

require_relative "./cassandraSchema"

class CassandraArgumentParser
  attr_reader :schemas
  def initialize(logger, option)
    @logger = logger
    @option = option
    @schemas = {}
    extract_schema_from_file
  end

  def exec(operand, args)
    send("prepare_args_#{operand.downcase}_#{@option[:inputFormat].downcase}", args)
  end

  def structure_type(_operand, _args)
    "others"
  end

  #--------------------#
  # PREPARE for INSERT #
  #--------------------#
  def prepare_args_insert_cql(args)
    result = { "table" => nil, "primaryKey" => nil,
               "schema_fields" => 0, "args" => {} }
    ## table name
    result["table"] = args[2]
    ## primary key
    result["primaryKey"] = get_primarykey_from_schemas(result["table"])
    ## field name
    field_names = args[3].sub(/\A\(/, "").sub(/\)\Z/, "").split(",")
    ## schema fields
    result["schema_fields"] = field_names.size
    ## values
    values = ""
    args.each_index do |index|
      if index > 4 ## after values
        values += args[index]
      end
    end
    values = values.sub(/\A\(/, "").sub(/\)\Z/, "").split(",")
    values.each_index do |index|
      result["args"][field_names[index].to_s] = if index != 0
                                                  "'" + values[index].to_s + "'"
                                                else
                                                  values[index].to_s
                                                end
    end
    result
  end

  def prepare_args_insert_basic(args)
    result = { "key" => nil, "args" => {} }
    ## table name
    result["key"] = args[2]
    ## field name
    field_names = args[3].sub(/\A\(/, "").sub(/\)\Z/, "").split(",")
    ## values
    values = ""
    args.each_index do |index|
      if index > 4
        values += args[index]
      end
    end
    values = values.sub(/\A\(/, "").sub(/\)\Z/, "").split(",")
    values.each_index do |index|
      result["args"][field_names[index].to_s] = if !index.zero?
                                                  "'" + values[index].to_s + "'"
                                                else
                                                  values[index].to_s + "'"
                                                end
    end
    result
  end

  #--------------------#
  # PREPARE for SELECT #
  #--------------------#
  def prepare_args_select_cql(args)
    ## key
    result = cassandra_prepare_select_parse(args)
    ## primary key
    result["primaryKey"] = get_primarykey_from_schemas(result["table"])
    ## schema fields
    result["schema_fields"] = @schemas[result["table"]].fields.size
    result
  end

  def get_primarykey_from_schemas(name)
    if !@schemas || !@schemas[name]
      return nil
    end
    @schemas[name].primarykeys[0]
  end

  def cassandra_prepare_select_parse(args)
    result = {
      "table" => nil, "primaryKey" => nil,
      "schema_fields" => 0, "fields" => [],
      "cond_keys" => [], "cond_values" => [],
      "limit" => nil
    }
    select_target_flag = false
    args.each_index do |index|
      case args[index].downcase
      when "select" then
        select_target_flag = true
      when "from" then
        select_target_flag = false
        result["table"] = args[index + 1]
      when "where" then
        ret = cassandra_prepare_select_where_parse(args, index)
        %w[cond_keys cond_values].each do |k_|
          result[k_].concat(ret[k_])
        end
      when "limit" then
        result["limit"] = args[index + 1]
      else
        if select_target_flag
          result["fields"].push(args[index])
        end
      end
    end
    result
  end

  def cassandra_prepare_select_where_parse(args, index)
    ret = { "cond_keys" => [], "cond_values" => [] }
    @logger.debug("Unsupported Multi Condition with AND/OR ")
    ret["cond_keys"].push(args[index + 1])
    if args[index + 2] == "=" && args.size >= index + 3
      ret["cond_values"].push(args[index + 3])
    end
    ret
  end

  def prepare_args_select_basic(args)
    result = {
      "key" => nil,
      "fields" => [],
      "where" => [],
      "limit" => nil,
    }
    ## key
    select_target_flag = false
    args.each_index do |index|
      case args[index]
      when "SELECT" then
        select_target_flag = true
      when "FROM" then
        select_target_flag = false
        result["key"] = args[index + 1]
      when "WHERE" then
        @logger.warn("Unsupported Multi Condition with AND/OR ")
        result["where"].push(args[index + 1])
      when "Limit" then
        result["where"] = args[index + 1]
      else
        if select_target_flag
          result["fields"].push(args[index])
        end
      end
    end
    result
  end

  #--------------------#
  # PREPARE for UPDATE #
  #--------------------#
  def prepare_args_update_cql(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "set" => {},
      "cond_keys" => [],
      "cond_values" => [],
    }
    result["table"] = args[1]
    result["primaryKey"] = get_primarykey(result["table"])
    ## schema fields
    # result["schema_fields"] = result["fields"].size
    result["schema_fields"] = @schemas[result["table"]].fields.size
    ## "field=value,..."
    args[3].split(",").each do |data__|
      data = data__.split("=")
      result["set"][data[0]] = data[1]
    end
    conds_ = get_condition_array(args)
    result["cond_keys"] = conds_[0] ## keys
    result["cond_values"] = conds_[1] ## values
    result
  end

  #--------------------#
  # PREPARE for DELETE #
  #--------------------#
  def prepare_args_delete_cql(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "fields" => [],
      "cond_keys" => [],
      "cond_values" => [],
    }
    if args.size != 5
      result["fields"] = args[1].split(",")
      result["table"] = args[3]
    else
      result["fields"] = ["*"]
      result["table"] = args[2]
    end
    args.each_index do |index|
      if args[index] == "WHERE"
        result["cond_keys"].push(index + 1)
        result["cond_values"].push(index + 3)
      end
    end
    result["primaryKey"] = get_primarykey(result["table"])
    ## schema fields
    result["schema_fields"] = result["fields"].size
    result
  end

  #------------------#
  # PREPARE for DROP #
  #------------------#
  def prepare_args_drop_cql(args)
    result = {}
    result["type"] = args[1]
    result["key"] = args[2]
    ## schema fields
    result["schema_fields"] = 0
    if result["fields"]
      result["schema_fields"] = result["fields"].size
    end
    result
  end

  #--------------------------#
  # PREPARE for BATCH_MUTATE #
  #--------------------------#
  def prepare_args_batch_mutate_java(args)
    data = parse_batch_mutate_parameter(args, false)
    hash = data["keyValue"]
    if !data["counterColumn"]
      hash[data["rowKey"]] = data["rowValue"]
    else
      hash.each do |h|
        h[data["rowKey"]] = data["rowValue"]
      end
    end
    result = {
      "key"  => data["table"],
      "args" => hash,
      "counterColumn" => data["counterColumn"],
    }
    result
  end

  # Parser #
  def parse_batch_mutate_parameter(param, cassandra = true)
    result = {
      "table" => nil, "rowKey" => "key", "rowValue" => nil,
      "cf" => nil, "keyValue" => {}, "counterColumn" => false,
      "counterKeyValue" => {}
    }
    # 1. key_name
    if param.match(/{(.+?):(.+?):(.*)/)
      rowkey = $1
      rowkey.delete!("'")
      columnfamily = $2
      others = $3
      result["cf"] = columnfamily.delete!(" ").sub!(/\'/, "")
      ## Find Keyspace
      result["table"] = extract_keyspace_for_batch_mutate_parameter(result)
      ## Primary Key's Field Type
      result["rowValue"] = if @schemas[result["table"]].primarykey_type != "blob"
                             rowkey
                           else
                             "0x" + rowkey
                           end
      kv = {}
      values = []
      counter_column_flag = false
      if others.include?("column:Column(")
        values = others.split("Mutation(column_or_supercolumn:ColumnOrSuperColumn(column:Column(")
      elsif others.include?("counter_column:CounterColumn(")
        counter_column_flag = true
        result["counterColumn"] = true
        values = others.split("Mutation(column_or_supercolumn:ColumnOrSuperColumn(counter_column:CounterColumn(")
      else
        @logger.fatal("Cannot Parse BatchMutateParameter #{__FILE__}")
      end
      ## Remove "["
      values.shift
      values.pop
      if !counter_column_flag
        values.each do |cols__|
          cols = cols__.split(",")
          key = nil
          cols.each_index do |index|
            key_value = cols[index].split(":")
            if key_value.size == 2 && !key_value[0].include?("timestamp")
              if index.zero?
                ## name
                key = [key_value[1].delete(" ")].pack("H*")
                key = @schemas[result["table"]].get_key(key.sub("C", "").to_i + 1)
              elsif index == 1
                ## value
                kv[key] = extract_values_for_batch_mutate_parameter(result, key, cassandra, key_value)
              end
            end
          end
        end
        result["keyValue"] = kv
      else
        #### CounterColumn
        kvs = []
        ckvs = []
        ckv = {}
        values.each do |cols__|
          cols = cols__.delete(")").delete(" ").split(",")
          key = nil
          cols.each_index do |index|
            key_value = cols[index].split(":")
            if index.zero?
              ## Name
              key_name = @schemas[result["table"]].get_key(1)
              kv[key_name] = extract_values_for_batch_mutate_parameter(result, key, cassandra. key_value)
            elsif index == 1
              ## Counter
              key = @schemas[result["table"]].counter_key
              if key
                ckv[key] = key_value[1].delete(" ").to_i
              end
            end
          end
          kvs.push(kv)
          ckvs.push(ckv)
          kv = {}
          ckv = {}
        end
        result["keyValue"] = kvs
        result["counterKeyValue"] = ckvs
      end
    end
    result
  end

  def extract_keyspace_for_batch_mutate_parameter(result)
    @schemas.keys.each do |ks_tb|
      if ks_tb.include?(".#{result["cf"]}")
        return ks_tb
      end
    end
  end

  def extract_values_for_batch_mutate_parameter(result, key, cassandra, key_value)
    if @schemas[result["table"]].field_type(key) == "blob" && cassandra
      "0x" + key_value[1].delete(" ")
    elsif @schemas[result["table"]].field_type(key) == "counter" && cassandra
      key_value[1].delete(" ").to_i
    else
      [key_Value[1].delete(" ")].pack("H*")
    end
  end

  #------------------------------#
  # PREPARE for GET_RANGE_SLICES #
  #------------------------------#
  def prepare_args_get_range_slices_java(args)
    data = parse_get_range_slices_parameter(args, false)
    result = { "key" => data["table"], "limit" => data["count"],
               "where" => [], "fields" => "*" }
    %w[start_key end_key].each do |name|
      key = prepare_condition_query(data, name)
      if key
        result["where"].push(key)
      end
    end
    result
  end

  def prepare_condition_query(data, name)
    if data[name]
      return "#{data["primaryKey"]}=" + data[name].to_s
    end
    nil
  end

  def parse_get_range_slices_parameter(args, _)
    result = { "table" => nil, "cf" => nil,
               "primaryKey" => nil, "start_key" => nil,
               "end_key" => nil, "count" => nil }
    ## Get Column Family
    result["cf"] = get_column_family(args)
    ## Find Keyspace & Primary Key
    res = get_tablename_and_primarykey(args, result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    ## Range
    if args.match(/KeyRange\((.+?)\)/)
      $1.split(",").each do |kv__|
        kv = kv__.gsub(/\s/, "").split(":")
        result[kv[0]] = if kv[0] == "start_key" && kv[0] == "end_key" && kv[1]
                          [kv[1].delete(" ")].pack("H*")
                        else
                          kv[1]
                        end
      end
    end
    result
  end

  #-----------#
  # GET_SLICE #
  #-----------#
  def prepare_args_get_slice_jave(args)
    data = parse_get_slice_parameter(args, false)
    { "key" => data["table"], "limit" => data["count"],
      "where" => [], "fields" => "*" }
  end

  def parse_get_slice_parameter(args, cassandra = true)
    result = { "table" => nil, "cf" => nil,
               "primaryKey" => nil, "targetKey" => nil,
               "count" => nil }
    ## Get Column Family
    if args.match(/.+column_family:(\w+?)\).*/)
      result["cf"] = $1
    end
    ## Find Keyspace
    ret = find_keyspace(result["cf"])
    if ret["table"]
      result["table"] = ret["table"]
      result["primaryKey"] = ret["primaryKey"]
    end
    ## Get Target Key
    if args.match(/.+key': '(\w+?)'.+/)
      result["targetKey"] = if @schemas[result["table"]].primarykey_type == "blob" && cassandra
                              "0x" + $1
                            else
                              [$1.delete(" ", "")].pack("H*")
                            end
    end
    ## Get Count
    if args.match(/.+count': '(\w+?)'.+/)
      result["count"] = $1
    end
    result
  end

  ##------------------------##
  ##-- GET_INDEXED_SLICES --##
  ##------------------------##
  def prepare_get_indexed_slice_parameter(args)
    result = { "table" => nil, "cf" => nil,
               "primaryKey" => nil, "targetKey" => nil,
               "count" => nil }
    ## Get Column Family
    if args.match(/.+column_family:(\w+?)\).*/)
      result["cf"] = $1
    end
    ## Find Keyspace
    ret = find_keyspace(result["cf"])
    if ret["table"]
      result["table"] = ret["table"]
      result["primaryKey"] = ret["primaryKey"]
    end
    result
  end

  def find_keyspace(cf)
    ret = {}
    @schemas.keys.each do |ks_tb|
      if ks_tb.include?(cf)
        ret["table"] = ks_tb
        ret["primaryKey"] = @schemas[ks_tb].primarykeys[0]
      end
    end
    ret
  end

  ## --------------------##
  ## -- MULTIGET_SLICE --##
  ## --------------------##
  def prepare_args_multiget_slice_java(args)
    data = prepare_multiget_slice_parameter(args, false)
    result = { "key" => data["table"], "limit" => data["count"],
               "where" => [], "fields" => "*" }
    data["keys"].each do |key|
      result["where"].push("#{data["primaryKey"]}=#{key}")
    end
    result
  end

  def prepare_multiget_slice_parameter(args, __cassandra)
    result = { "table" => nil, "primaryKey" => "key",
               "cf" => nil, "keys" => [] }
    ## Get ColumnFamily
    result["cf"] = get_column_family(args)
    ## Find Keyspace & Primary Key
    res = get_tablename_and_primarykey(args, result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    if args.match(/'keys':\s'\[(.+)\]/)
      array = $1.delete(" ").split(",")
      array.uniq.each do |v|
        result["keys"].push("0x#{v}")
      end
    end
    result
  end

  ##############
  ##-- CQL3 --##
  ##############
  def prepare_args_insert_cql3(args)
    prepare_args_insert_cql(args)
  end

  def prepare_args_drop_cql3(args)
    prepare_args_drop_cql(args)
  end

  def prepare_args_select_cql3(args)
    prepare_args_select_cql(args)
  end

  def prepare_args_update_cql3(args)
    prepare_args_update_cql(args)
  end

  private

  ###############
  ##-- UTILS --##
  ###############
  def get_column_family(str)
    if str.match(/.+column_family:(\w+?)\).*/)
      $1
    end
  end

  def get_tablename_and_primarykey(_, cf)
    result = { "table" => nil, "primaryKey" => nil }
    ret = find_keyspace(cf)
    if ret["table"]
      result["table"] = ret["table"]
      result["primaryKey"] = ret["primaryKey"]
    end
    result
  end

  #####################
  # Parse Schema File #
  #####################
  def extract_schema_from_file
    filename = @option[:schemaFile]
    keyspace = @option[:keyspace]
    unless filename
      filename = "#{File.dirname(__FILE__)}/cassandraDefaultSchema.schema"
    end
    name = ""
    File.read(filename).split("\n").each do |query|
      unless query.empty?
        if query.downcase.include?("create table")
          newschema = CassandraSchema.new(keyspace, query.delete("\t"), @logger)
          name = newschema.name
          @schemas[name] = newschema
        elsif query.downcase.include?("create index")
          @schemas[name].push_create_index(query)
        end
      end
    end
  end

  def get_primarykey(result)
    ## primary key
    if @schemas && @schemas[result]
      return @schemas[result].primarykeys[0]
    end
    nil
  end

  def get_condition_array(args)
    array = []
    keys  = []
    values = []
    args.each_index do |index|
      if args[index] == "WHERE"
        keys.push(index + 1)
        values.push(index + 3)
      end
    end
    array.push(keys)
    array.push(values)
    array
  end
end
