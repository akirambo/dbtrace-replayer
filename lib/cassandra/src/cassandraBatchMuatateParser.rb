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

class CassandraBatchMutateParser
  def initizlie(logger, option)
    @logger = logger
    @option = option
    @schemas = {}
  end

  #--------------------------#
  # PREPARE for BATCH_MUTATE #
  #--------------------------#
  def prepare_args_batch_mutate_java(args, schemas)
    @schemas = schemas
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

  private

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
      values, counter_column_flag, result = extract_value_for_batch_mutate_parameter(others, result)
      result = if !counter_column_flag
                 extract_keyvalue_for_batch_mutate_parameter(values, result, cassandra)
               else
                 extract_counter_column_for_batch_mutate_parameter(values, result, cassandra)
               end
    end
    result
  end

  def extract_value_for_batch_mutate_parameter(others, result)
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
    return values, counter_column_flag, result
  end

  def extract_keyvalue_for_batch_mutate_parameter(values, result, cassandra)
    kv = {}
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
    result
  end

  def extract_counter_column_for_batch_mutate_parameter(values, result, cassandra)
    #### CounterColumn
    kv = {}
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
          kv[key_name] = extract_values_for_batch_mutate_parameter(result, key, cassandra, key_value)
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
end
