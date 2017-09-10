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
  def initialize(logger, options)
    @logger = logger
    @options = options
    @schemas = {}
    extractSchemaFromFile
  end

  def exec(operand, args)
    send("prepareArgs_#{operand}_#{@options[:inputFormat].upcase}", args)
  end

  def structureType(_operand, _args)
    "others"
  end

  #--------------------#
  # PREPARE for INSERT #
  #--------------------#
  def prepareArgs_INSERT_CQL(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "schema_fields" => 0,
      "args" => {},
    }
    ## table name
    result["table"] = args[2]
    ## primary key
    if @schemas && @schemas[result["table"]]
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
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

  def prepareArgs_INSERT_BASIC(args)
    result = {
      "key" => nil,
      "args" => {},
    }
    ## table name
    result["key"] = args[2]
    ## field name
    field_names = args[3].sub(/\A\(/, "").sub(/\)\Z/, "").split(",")
    ## values
    values = ""
    args.each_index do |index|
      if index > 4
        ## after values
        values += args[index]
      end
    end
    # values = values.sub(/\A\(/,"").sub(/\)\Z/,"").split("','")
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
  def prepareArgs_SELECT_CQL(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "schema_fields" => 0,
      "fields" => [],
      "cond_keys" => [],
      "cond_values" => [],
      "limit" => nil,
    }
    ## key
    select_target_flag = false
    args.each_index do |index|
      case args[index].upcase
      when "SELECT" then
        select_target_flag = true
      when "FROM" then
        select_target_flag = false
        result["table"] = args[index + 1]
      when "WHERE" then
        @logger.debug("Unsupported Multi Condition with AND/OR ")
        result["cond_keys"].push(args[index + 1])
        if args[index + 2] == "=" && args.size >= index + 3
          result["cond_values"].push(args[index + 3])
        end
      when "LIMIT" then
        result["limit"] = args[index + 1]
      else
        if select_target_flag
          result["fields"].push(args[index])
        end
      end
    end
    ## primary key
    if @schemas && @schemas[result["table"]]
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    result["schema_fields"] = @schemas[result["table"]].fields.size
    result
  end

  def prepareArgs_SELECT_BASIC(args)
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
  def prepareArgs_UPDATE_CQL(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "set" => {},
      "cond_keys" => [],
      "cond_values" => [],
    }
    result["table"] = args[1]
    ## primary key
    if @schemas && @schemas[result["table"]]
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    # result["schema_fields"] = result["fields"].size
    result["schema_fields"] = @schemas[result["table"]].fields.size
    ## "field=value,..."
    args[3].split(",").each do |data__|
      data = data__.split("=")
      result["set"][data[0]] = data[1]
    end
    args.each_index do |index|
      if args[index] == "WHERE"
        result["cond_keys"].push(index + 1)
        result["cond_values"].push(index + 3)
      end
    end
    result
  end

  #--------------------#
  # PREPARE for DELETE #
  #--------------------#
  def prepareArgs_DELETE_CQL(args)
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
    ## primary key
    if @schemas && @schemas[result["table"]]
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    result["schema_fields"] = result["fields"].size
    result
  end

  #------------------#
  # PREPARE for DROP #
  #------------------#
  def prepareArgs_DROP_CQL(args)
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
  def prepareArgs_BATCH_MUTATE_JAVA(args)
    data = parseBatchMutateParameter(args, false)
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
  def parseBatchMutateParameter(param, cassandra = true)
    result = {
      "table" => nil,
      "rowKey" => "key",
      "rowValue" => nil,
      "cf" => nil,
      "keyValue" => {},
      "counterColumn" => false,
      "counterKeyValue" => {},
    }
    # 1. keyName
    if param.match(/{(.+?):(.+?):(.*)/)
      rowkey = $1
      columnfamily = $2
      others = $3
      result["cf"] = columnfamily.delete!(" ").sub!(/\'/, "")
      ## Find Keyspace
      @schemas.keys.each do |ks_tb|
        if ks_tb.include?(".#{result["cf"]}")
          result["table"] = ks_tb
        end
      end
      ## Primary Key's Field Type
      result["rowValue"] = if @schemas[result["table"]].primaryKeyType != "blob"
                             rowkey.delete!("'")
                           else
                             "0x" + rowkey.delete!("'")
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
            if key_value.size == 2 && !keyValue[0].include?("timestamp")
              if index.zero?
                ## name
                key = [key_value[1].delete(" ")].pack('H*')
                key = @schemas[result["table"]].getKey(key.sub("C", "").to_i + 1)
              elsif index == 1
                ## value
                if @schemas[result["table"]].fieldType(key) == "blob" && cassandra
                  kv[key] = "0x"+key_value[1].delete(" ")
                elsif@schemas[result["table"]].fieldType(key) == "counter" && cassandra
                  kv[key] = key_value[1].delete(" ").to_i
                else
                  kv[key] = [key_Value[1].delete(" ")].pack('H*')
                end
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
          cols = cols__.delete(")")
                 .delete(" ")
                 .split(",")
          key = nil
          cols.each_index do |index|
            keyValue = cols[index].split(":")
            if index.zero?
              ## Name 
              keyName = @schemas[result["table"]].getKey(1)
              if(@schemas[result["table"]].fieldType(key) == "blob" and cassandra)then
                kv[keyName] = "0x" + keyValue[1].delete(" ")
              else
                kv[keyName] = [keyValue[1].delete(" ")].pack('H*')
              end
            elsif index == 1
              ## Counter
              key = @schemas[result["table"]].counterKey
              if key
                ckv[key] = keyValue[1].delete(" ").to_i
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

  def counterColumn()
    # do nothing
  end

  #------------------------------#
  # PREPARE for GET_RANGE_SLICES #
  #------------------------------#
  def prepareArgs_GET_RANGE_SLICES_JAVA(args)
    data = parseGetRangeSlicesParameter(args, false)
    result = {
      "key"   => data["table"],
      "limit" => data["count"],
      "where" => [],
      "fields" => "*"
    }
    if(data["start_key"])then
      result["where"].push("#{data["primaryKey"]}=#{data["start_key"]}")
    end
    if(data["end_key"])then
      result["where"].push("#{data["primaryKey"]}=#{data["end_key"]}")
    end
    return result
  end
  def parseGetRangeSlicesParameter(args,cassandra=true)
    result = {
      "table" => nil,
      "cf"    => nil,
      "primaryKey" => nil,
      "start_key" => nil,
      "end_key" => nil,
      "count" => nil,
    }
    ## Get Column Family
    result["cf"] = getColumnFamily(args)
    ## Find Keyspace & Primary Key
    res = getTableNameANDPrimaryKey(args, result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    
    ## Range
    if args.match(/KeyRange\((.+?)\)/)
      $1.split(",").each do |kv__|
        kv = kv__.gsub(/\s/,"").split(":")
        if kv[0] == "start_key" && kv[0] == "end_key" && kv[1]
          if @schemas[result["table"]].primaryKeyType == "blob" && cassandra
            result[kv[0]] = [kv[1].delete(" ")].pack('H*')
          else
            result[kv[0]] = [kv[1].delete(" ")].pack('H*')
          end
        else
          result[kv[0]] = kv[1]
        end
      end
    end
    result
  end

  #-----------#
  # GET_SLICE #
  #-----------#
  def prepareArgs_GET_SLICE_JAVA(args)
    data = parseGetSliceParameter(args, false)
    result = {
      "key"   => data["table"],
      "limit" => data["count"],
      "where" => [],
      "fields" => "*"
    }
    result
  end

  def parseGetSliceParameter(args, cassandra = true)
    result = {
      "table" => nil,
      "cf"    => nil,
      "primaryKey" => nil,
      "targetKey" => nil,
      "count"     => nil,
    }
    ## Get Column Family
    if args.match(/.+column_family:(\w+?)\).*/)
      result["cf"] = $1
    end
    ## Find Keyspace
    @schemas.keys.each do |ks_tb|
      if ks_tb.include?(result["cf"])
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    end
    ## Get Target Key
    if args.match(/.+key': '(\w+?)'.+/)
      if @schemas[result["table"]].primaryKeyType == "blob" && cassandra
        result["targetKey"] = "0x" + $1
      else
        result["targetKey"] = [$1.delete(" ","")].pack('H*')
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
  def prepare_GET_INDEXED_SLICESParameter(args)
    result = {
      "table" => nil,
      "cf" => nil,
      "primaryKey" => nil,
      "targetKey" => nil,
      "count" => nil,
    } 
    ## Get Column Family
    if args.match(/.+column_family:(\w+?)\).*/)
      result["cf"] = $1
    end
    ## Find Keyspace
    @schemas.keys.each do |ks_tb|
      if ks_tb.include?(result["cf"])
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    end
    result
  end
  
  ##--------------------##
  ##-- MULTIGET_SLICE --## 
  ##--------------------##
  def prepareArgs_MULTIGET_SLICE_JAVA(args)
    data = prepare_MULTIGET_SLICEParameter(args, false)
    result = {
      "key"   => data["table"],
      "limit" => data["count"],
      "where" => [],
      "fields" => "*",
    }
    data["keys"].each do |key|
      result["where"].push("#{data["primaryKey"]}=#{key}")
    end
    result
  end

  def prepare_MULTIGET_SLICEParameter(args, cassandra = true)
    result = {
      "table" => nil,
      "primaryKey" => "key",
      "cf" => nil,
      "keys" => [],
    }
    ## Get ColumnFamily
    result["cf"] = getColumnFamily(args)
    ## Find Keyspace & Primary Key
    res = getTableNameANDPrimaryKey(args, result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    if args.match(/'keys':\s'\[(.+)\]/)
      array = $1.delete(" ").split(",")
      if @schemas[result["table"]].primaryKeyType == "blob" && cassandra
        array.uniq.each do |v|
          result["keys"].push("0x#{v}")
        end
      else
        array.uniq.each do |v|
          result["keys"].push("0x#{v}")
        end
      end
    end
    result
  end

  ##############
  ##-- CQL3 --##
  ##############
  def prepareArgs_INSERT_CQL3(args)
    prepareArgs_INSERT_CQL(args)
  end

  def prepareArgs_DROP_CQL3(args)
    prepareArgs_DROP_CQL(args)
  end

  def prepareArgs_SELECT_CQL3(args)
    prepareArgs_SELECT_CQL(args)
  end

  def prepareArgs_UPDATE_CQL3(args)
    prepareArgs_UPDATE_CQL(args)
  end

  private

  ###############
  ##-- UTILS --##
  ###############
  def getColumnFamily(str)
    if str.match(/.+column_family:(\w+?)\).*/)
      return $1
    end
  end

  def getTableNameANDPrimaryKey(_, cf)
    result = { "table" => nil, "primaryKey" => nil }
    @schemas.keys.each do |ks_tb|
      if ks_tb.include?(cf)
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    end
    result
  end

  #####################
  # Parse Schema File #
  #####################
  def extractSchemaFromFile
    filename = @options[:schemaFile]
    keyspace = @options[:keyspace]
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
          @schemas[name].pushCreateIndex(query)
        end
      end
    end
  end
end
