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
  attr_reader :keyspace, :table, :primaryKeys,
  :createKeyspaceQuery,:createQuery, :dropQuery, :createIndexes
  
  def initialize(keyspace,createQuery, logger)
    @keyspace = keyspace
    @table    = nil
    @primaryKeys = nil
    @fields   = {} ## key => type
    @createQuery = nil
    @createKeyspaceQuery = nil
    @createIndexes = []
    @logger = logger
    @values = []
    @dropQuery = nil
    parse(createQuery)

  end
  def name
    return @keyspace+"."+@table
  end
  def check(keyValue, bulk=false)
    keyValue.each{|k,v|
      if(bulk)then
        if(@fields.keys and keyValue.keys  and
            @fields.keys.size >= keyValue.keys.size)then
          k  = @fields.keys[keyValue.keys.index(k)]
        else
          @logger.warn("Please Set Fields bulk0, ... bulk#{@fields.keys.size}")
          return false
        end
      end
      if(@fields[k])then
        if(checkFieldType(v,@fields[k]) == false)then
          if(@fields[k])
            @logger.error("Unmatch DataType #{k} is not #{@fields[k]} AT #{name}")
            @logger.error("                 #{k} is #{v.class.to_s}")
            @logger.error("Under Construction Auto Table Creation")
            return false
          end
        end
      else
        ## Skip Field
        @logger.debug("Skip Field:: #{k}")
      end
    }
    return true
  end
  def extractKeyValue(keyValue)
    keys = []
    values = []
    kvs = {
      "key"   => [],
      "value" => []
    }
    keyValue.each{|_key,value|
      key = _key.gsub("_","")
      @logger.debug("KEY    :: #{key} IN Fields :: #{@fields.keys}")
      if(@fields.keys.include?(key))then
        keys.push(key)
        case value.class.to_s
        when "Array" then
          if(value.size > 1)then
            if(value[0].class == Hash)then
              hash = []
              value.each{|__hash__|
                string = __hash__.to_json
                string.gsub!("\"","")
                hash.push("#{string}")
              }
              values.push("{\"#{hash.join(",")}\"}")
            else
              values.push("{#{value.join(",")}}")
            end
          else
            values.push("{}")
          end
        when "Hash" then
          val_ = []
          value.each{|k_,v_|
            if(v_.class == Hash or v_.class == Array )then
              newVal= "'#{k_}':'#{v_.to_json}'"
              val_.push(newVal.gsub('"','__DOUBLEQ__'))
            else
              val_.push("'#{k_}':'#{v_}'")
            end
          }
          values.push("{"+val_.join(",")+"}")
        when "Fixnum" then
          if(@fields[key] == "INT")then
            values.push(value.to_i)
          elsif(@fields[key] == "TEXT")then
            values.push("'"+value.to_s+"'")
          end
        when "String" then
          if(value.include?("("))then
            values.push("'"+value.sub("('","").sub("')","")+"'")
          else
            values.push("'"+value+"'")
          end
        when "Float" then
          values.push(value.to_f)
        else
          values.push("'"+value.to_s+"'")
        end
      else
        @logger.error("ERROR KEY : #{key} => #{value} (#{value.class.to_s})")
      end
    }
    kvs["key"] = keys.join(",")
    kvs["value"] = values.join(",")
    return kvs
  end
  def fields
    @fields.keys
  end
  def keys(size)
    size -= 1
    @fields.keys[0..size]
  end
  def getKey(index)
    @fields.keys[index]
  end
  def fieldType(field)
    @fields[field]
  end
  def counterKey()
    @fields.each{|key,value|
      if(value == "counter")then
        return key
      end
    }
    return nil
  end
  def primaryKeyType
    @fields[@primaryKeys[0]]
  end
  def stringType(field)
    return (@fields[field].downcase() == "varchar" or
            @fields[field].downcase() == "text")
  end
  def pushCreateIndex(query)
    @createIndexes.push(query)
  end
  private
  def parse(createQuery)
    table = ""
    if(createQuery.include?("CREATE TABLE"))then
      table = createQuery.split("CREATE TABLE ")[1].split(" ")[0]
    elsif(createQuery.include?("create table"))then
      table = createQuery.split("create table ")[1].split(" ")[0]
    end
    @keyspace = table.split(".")[0]
    @createKeyspaceQuery = 
      "create keyspace if not exists #{@keyspace} with replication = {'class':'SimpleStrategy','replication_factor':3}"
    @table = table.split(".")[1]
    @primaryKeys = createQuery.downcase.split("primary key")[1]
      .gsub("(","").gsub(")","").gsub(/\s/,"").gsub(";","").split(",")
    @createQuery = createQuery.gsub("_","")
    @createQuery.sub!(@keyspace.gsub("_",""), @keyspace)
    @createQuery.sub!(@table.gsub("_",""), @table)
    @dropQuery =  "drop table if exists #{@keyspace}.#{@table}"
    
    field = {}
    if(@keyspace)then
      _field_ = createQuery.split(" #{@keyspace}.#{@table} ")[1]
      if(_field_.include?("primary key"))then
        _field_ = _field_.split("primary key")[0]
      elsif(_field_.include?("PRIMARY KEY"))then
        _field_ = _field_.split("PRIMARY KEY")[0]
      end
    end
    _field_ = _field_.gsub("(","").gsub(")","").gsub("\t","").gsub("\n","")
    field = _field_.split(",")
    test = {}
    keep = nil
    field.each{|kv|
      if(!kv.include?("primary key") and 
          !kv.include?("PRIMARY KEY"))then
        if(kv.include?("<") and !kv.include?(">"))then
          keep = kv
        elsif(keep)then
          if(kv.include?(">"))then
            ## Finish
            keep += ","+ kv
            extractKeyValueForPriv(keep)
            keep = nil
          else
            keep += ","+ kv
          end
        else
          extractKeyValueForPriv(kv)
        end
      end
    }
  end
  def checkFieldType(value, _schemaType_)
    schemaType = _schemaType_.downcase()
    case value.class.to_s
    when "String" then
      return (schemaType == "varchar" or schemaType == "text")
    when "Integer" then
      return (schemaType == "int")
    when "Float" then
      return (schemaType == "float")
    when "Array" then
      return (schemaType.include?("set") or schemaType.include?("map"))
    when "TrueClass" then
      return (schemaType == "text")
    when "FalseClass" then
      return (schemaType == "text")
    when "Hash" then
      return (schemaType.include?("map"))
    else
      @logger.error("Unsupport field type #{value.class}")
    end
    return false
  end
  private
  def extractKeyValueForPriv(string)
    kv = string.sub(/^\s*/,"").split(" ")
    if(kv.size == 2)then
      fieldName = kv[0].gsub(" ","")
      fieldType = kv[1].gsub(" ","")
      return @fields[fieldName] = fieldType
    end
  end
end
