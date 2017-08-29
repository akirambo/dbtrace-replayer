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
    extractSchemaFromFile()
  end
  def exec(operand,args)
    return send("prepareArgs_#{operand}_#{@options[:inputFormat].upcase}", args)
  end
  def structureType(operand,args)
    structureType = "others"
    return structureType
  end
  #--------------------#
  # PREPARE for INSERT #
  #--------------------#
  def prepareArgs_INSERT_CQL(args)
    result = {
      "table"        => nil,
      "primaryKey"   => nil,
      "schema_fields" => 0,
      "args"          => {}
    }
    ## table name
    result["table"] = args[2]
    ## primary key
    if(@schemas and @schemas[result["table"]])then
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## field name
    fieldNames = args[3].sub(/\A\(/,"").sub(/\)\Z/,"").split(",")
    ## schema fields
    result["schema_fields"] = fieldNames.size
    ## values
    values = ""
    args.each_index{|index|
      if(index > 4)then ## after values
        values += args[index]
      end
    }
    values = values.sub(/\A\(/,"").sub(/\)\Z/,"").split(",")
    values.each_index{|index|
      if(index != 0)then
        result["args"][fieldNames[index].to_s] = "'"+ values[index].to_s + "'"
      else
        result["args"][fieldNames[index].to_s] = values[index].to_s 
      end
    }
    return result
  end
  def prepareArgs_INSERT_BASIC(args)
    result = {
      "key"   => nil,
      "args"  => {}
    }
    ## table name
    result["key"] = args[2]
    ## field name
    fieldNames = args[3].sub(/\A\(/,"").sub(/\)\Z/,"").split(",")
    ## values
    values = ""
    args.each_index{|index|
      if(index > 4)then ## after values
        values += args[index]
      end
    }
    # values = values.sub(/\A\(/,"").sub(/\)\Z/,"").split("','")
     values = values.sub(/\A\(/,"").sub(/\)\Z/,"").split(",")
    values.each_index{|index|
      if(index != 0)then
        result["args"][fieldNames[index].to_s] = "'"+ values[index].to_s + "'"
      else
        result["args"][fieldNames[index].to_s] = values[index].to_s + "'"
      end
    }
    return result
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
      "cond_keys"  => [],
      "cond_values"  => [], 
      "limit"  => nil
    }
    ## key
    select_target_flag = false
    args.each_index{|index|
      case args[index].upcase
      when "SELECT" then
        select_target_flag = true
      when "FROM" then
        select_target_flag = false
        result["table"] = args[index+1]
      when "WHERE" then
        @logger.debug("Unsupported Multi Condition with AND/OR ")
        result["cond_keys"].push(args[index+1])
        if(args[index+2] == "=" and args.size >= index+3)then
          result["cond_values"].push(args[index+3])
        end
      when "LIMIT" then
        result["limit"] = args[index+1]
      else
        if(select_target_flag)then
          result["fields"].push(args[index])
        end
      end
    }
    ## primary key
    if(@schemas and @schemas[result["table"]])then
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    result["schema_fields"] =  @schemas[result["table"]].fields.size
    return result
  end
  def prepareArgs_SELECT_BASIC(args)
    result = {
      "key" => nil,
      "fields" => [],
      "where"  => [],
      "limit"  => nil
    }
    ## key
    select_target_flag = false
    args.each_index{|index|
      case args[index]
      when "SELECT" then
        select_target_flag = true
      when "FROM" then
        select_target_flag = false
        result["key"] = args[index+1]
      when "WHERE" then
        @logger.warn("Unsupported Multi Condition with AND/OR ")
        result["where"].push(args[index+1])
      when "Limit" then
        result["where"] = args[index+1]
      else
        if(select_target_flag)then
          result["fields"].push(args[index])
        end
      end
    }
    return result
  end
  #--------------------#
  # PREPARE for UPDATE #
  #--------------------#
  def prepareArgs_UPDATE_CQL(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "set" => {},
      "cond_keys"  => [],
      "cond_values"  => [],
    }
    result["table"] = args[1]
    ## primary key
    if(@schemas and @schemas[result["table"]])then
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    #result["schema_fields"] = result["fields"].size
    result["schema_fields"] = @schemas[result["table"]].fields.size

    ## "field=value,..."
                                       
    args[3].split(",").each{|__data__|
      data = __data__.split("=")
      result["set"][data[0]] = data[1]
    }
    args.each_index{|index|
      if(args[index] == "WHERE")then
        result["cond_keys"].push(index+1)
        result["cond_values"].push(index+3)
      end
    }
    return result
  end
  #--------------------#
  # PREPARE for DELETE #
  #--------------------#
  def prepareArgs_DELETE_CQL(args)
    result = {
      "table" => nil,
      "primaryKey" => nil,
      "fields" => [],
      "cond_keys"  => [],
      "cond_values"  => []
    }
    if(args.size != 5)then
      result["fields"] = args[1].split(",")
      result["table"] = args[3]
    else
      result["fields"] = ["*"]
      result["table"] = args[2]
    end
    args.each_index{|index|
      if(args[index] == "WHERE")then
        result["cond_keys"].push(index+1)
        result["cond_values"].push(index+3)
      end
    }
    ## primary key
    if(@schemas and @schemas[result["table"]])then
      result["primaryKey"] = @schemas[result["table"]].primaryKeys[0]
    end
    ## schema fields
    result["schema_fields"] = result["fields"].size
    return result
  end
  #------------------#
  # PREPARE for DROP #
  #------------------#
  def prepareArgs_DROP_CQL(args)
    result = {}
    result["type"] = args[1]
    result["key"] = args[2]
    ## schema fields
    if(result["fields"])then
      result["schema_fields"] = result["fields"].size
    else
      result["schema_fields"] = 0
    end
    return result
  end
  #--------------------------#
  # PREPARE for BATCH_MUTATE #
  #--------------------------#
  def prepareArgs_BATCH_MUTATE_JAVA(args)

    data = parseBatchMutateParameter(args,false)
    hash = data["keyValue"]
    if(!data["counterColumn"])then
      hash[data["rowKey"]] = data["rowValue"]
    else
      hash.each{|h|
        h[data["rowKey"]] = data["rowValue"]
      }
    end
    result = {
      "key"  => data["table"],
      "args" => hash,
      "counterColumn" => data["counterColumn"]
    }
    return result
  end

  # Parser #
  def parseBatchMutateParameter(param,cassandra=true)
    result = {
      "table" => nil,
      "rowKey"   => "key",
      "rowValue" => nil,
      "cf"       => nil,
      "keyValue" => {},
      "counterColumn" => false,
      "counterKeyValue" => {}
    }
    # 1. keyName
    if(param.match(/{(.+?):(.+?):(.*)/))then
      rowKey = $1
      columnFamily = $2
      others = $3
      result["cf"] = columnFamily.gsub!(/\s/,"").sub!(/\'/,"")
      ## Find Keyspace
      @schemas.keys().each{|ks_tb|
        if(ks_tb.include?(".#{result["cf"]}"))then
          result["table"] = ks_tb
        end
      }
      ## Primary Key's Field Type      
      if( @schemas[result["table"]].primaryKeyType != "blob")then
        result["rowValue"] = rowKey.gsub!("'","")
      else
        result["rowValue"] = "0x"+rowKey.gsub!("'","")
      end
      kv = {}
      values = []
      counterColumnFlag = false
      if(others.include?("column:Column("))then
        values = others.split("Mutation(column_or_supercolumn:ColumnOrSuperColumn(column:Column(")
      elsif(others.include?("counter_column:CounterColumn("))then
        counterColumnFlag = true
        result["counterColumn"]= true

        values = others.split("Mutation(column_or_supercolumn:ColumnOrSuperColumn(counter_column:CounterColumn(")
      else
        @logger.fatal("Cannot Parse BatchMutateParameter #{__FILE__}")
      end
      ## Remove "["
      values.shift
      values.pop
      if(!counterColumnFlag)then
        values.each{|__cols__|
          cols = __cols__.split(",")
          key = nil
          cols.each_index{|index|
            keyValue = cols[index].split(":")
            if(keyValue.size == 2 and !keyValue[0].include?("timestamp"))then
              if(index == 0)then
                ## name
                key = [keyValue[1].gsub(" ","")].pack('H*')
                key =  @schemas[result["table"]].getKey(key.sub("C","").to_i + 1)
              elsif(index == 1)then
                ## value
                if(@schemas[result["table"]].fieldType(key) == "blob" and cassandra)then
                  kv[key] = "0x"+keyValue[1].gsub(" ","")
                elsif(@schemas[result["table"]].fieldType(key) == "counter" and cassandra)then
                  kv[key] = keyValue[1].gsub(" ","").to_i
                else
                  kv[key] = [keyValue[1].gsub(" ","")].pack('H*')
                end
              end
            end
          }
        }
        result["keyValue"] = kv
      else
        #### CounterColumn
        kvs = [] 
        ckvs = []
        ckv = {}
        values.each{|__cols__|
          cols = __cols__.gsub(")","")
            .gsub(/\s/,"")
            .split(",")
          key = nil
          cols.each_index{|index|
            keyValue = cols[index].split(":")
            if(index == 0)then
              ## Name 
              keyName = @schemas[result["table"]].getKey(1)
              if(@schemas[result["table"]].fieldType(key) == "blob" and cassandra)then
                kv[keyName] = "0x" + keyValue[1].gsub(" ","")
              else
                kv[keyName] = [keyValue[1].gsub(" ","")].pack('H*')
              end
            elsif(index == 1)then
              ## Counter
              key = @schemas[result["table"]].counterKey()
              if(key)then
                ckv[key] = keyValue[1].gsub(" ","").to_i
              end
            end
          }
          kvs.push(kv)
          ckvs.push(ckv)
          kv = {}
          ckv = {}
        }
        result["keyValue"] = kvs
        result["counterKeyValue"] = ckvs
      end
    end
    return result
  end

  def counterColumn()
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
      "count" => nil
    }
    ## Get Column Family
    result["cf"] = getColumnFamily(args)
    ## Find Keyspace & Primary Key
    res = getTableNameANDPrimaryKey(args,result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    
    ## Range
    if(args.match(/KeyRange\((.+?)\)/))then
      $1.split(",").each{|__kv__|
        kv = __kv__.gsub(/\s/,"").split(":")
        if((kv[0] == "start_key" or kv[0] == "end_key") and
            kv[1] != nil)then
          if(@schemas[result["table"]].primaryKeyType == "blob" and 
              cassandra)then
            result[kv[0]] = [kv[1].gsub(" ","")].pack('H*')
          else
            result[kv[0]] = [kv[1].gsub(" ","")].pack('H*')
          end
        else
          result[kv[0]] = kv[1]
        end
      }
    end
    return result
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
    return result
  end
  def parseGetSliceParameter(args, cassandra=true)
    result = {
      "table" => nil,
      "cf"    => nil,
      "primaryKey" => nil,
      "targetKey" => nil,
      "count"     => nil
    }
    ## Get Column Family
    if(args.match(/.+column_family:(\w+?)\).*/))then
      result["cf"] = $1
    end
    ## Find Keyspace
    @schemas.keys().each{|ks_tb|
      if(ks_tb.include?(result["cf"]))then
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    }
    ## Get Target Key
    if(args.match(/.+key': '(\w+?)'.+/))then
      if(@schemas[result["table"]].primaryKeyType == "blob" and cassandra )then
        result["targetKey"] = "0x" + $1
      else
        result["targetKey"] = [$1.gsub(" ","")].pack('H*')
      end
    end
    ## Get Count
    if(args.match(/.+count': '(\w+?)'.+/))then
      result["count"] = $1
    end
    return result
  end
  ##------------------------##
  ##-- GET_INDEXED_SLICES --##
  ##------------------------##
  def prepare_GET_INDEXED_SLICESParameter(args)
    result = {
      "table" => nil,
      "cf"    => nil,
      "primaryKey" => nil,
      "targetKey" => nil,
      "count"     => nil
    } 
    ## Get Column Family
    if(args.match(/.+column_family:(\w+?)\).*/))then
      result["cf"] = $1
    end
    ## Find Keyspace
    @schemas.keys().each{|ks_tb|
      if(ks_tb.include?(result["cf"]))then
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    }
    return result
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
      "fields" => "*"
    }
    data["keys"].each{|key|
      result["where"].push("#{data["primaryKey"]}=#{key}")
    }
    return result
  end
  def prepare_MULTIGET_SLICEParameter(args, cassandra=true)
    result = {
      "table"  => nil,
      "primaryKey" => "key",
      "cf"     => nil,
      "keys" => []
    }
    ## Get ColumnFamily
    result["cf"] = getColumnFamily(args)
    ## Find Keyspace & Primary Key
    res = getTableNameANDPrimaryKey(args,result["cf"])
    result["table"] = res["table"]
    result["primaryKey"] = res["primaryKey"]
    if(args.match(/'keys':\s'\[(.+)\]/))then
      array = $1.gsub(" ","").split(",")
      if(@schemas[result["table"]].primaryKeyType == "blob" and cassandra)then
        array.uniq.each{|v|
          result["keys"].push("0x#{v}")
        }
      else
        array.uniq.each{|v|
          result["keys"].push("0x#{v}")
        }
      end
    end
      return result
  end
  ##############
  ##-- CQL3 --##
  ##############
  def prepareArgs_INSERT_CQL3(args)
    return prepareArgs_INSERT_CQL(args)
  end
  def prepareArgs_DROP_CQL3(args)
    return prepareArgs_DROP_CQL(args)
  end
  def prepareArgs_SELECT_CQL3(args)
    return prepareArgs_SELECT_CQL(args)
  end
  def prepareArgs_UPDATE_CQL3(args)
    return prepareArgs_UPDATE_CQL(args)
  end
private
  ###############
  ##-- UTILS --##
  ###############
  def getColumnFamily(str)
    if(str.match(/.+column_family:(\w+?)\).*/))then
      return  $1
    end
  end
  def getTableNameANDPrimaryKey(str, cf)
    result = {"table" => nil, "primaryKey" => nil}
    @schemas.keys().each{|ks_tb|
      if(ks_tb.include?(cf))then
        result["table"] = ks_tb
        result["primaryKey"] = @schemas[ks_tb].primaryKeys[0]
      end
    }
    return result
  end

  #####################
  # Parse Schema File #
  #####################
  def extractSchemaFromFile
    filename = @options[:schemaFile]
    keyspace = @options[:keyspace]
    if(!filename)then
      filename = "#{File.dirname(__FILE__)}/cassandraDefaultSchema.schema"
    end
    name = ""
    File.read(filename).split("\n").each{|query|
      if(query.size > 0)then
        if(query.downcase.include?("create table"))then
          newSchema = CassandraSchema.new(keyspace,query.gsub("\t",""),@logger)
          @schemas[newSchema.name] = newSchema
          name = newSchema.name
        elsif(query.downcase.include?("create index"))then
          @schemas[name].pushCreateIndex(query)
        end
      end
    }
  end
end
