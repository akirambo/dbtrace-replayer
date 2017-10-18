
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

class MongodbQueryProcessor
  MONGODB_NUMERIC_QUERY = %w[$gt $gte $lt $lte].freeze
  MONGODB_STRING_QUERY  = %w[$eq $ne $in $nin].freeze

  def initialize(logger)
    @logger = logger
  end

  ## symbol [Variable] means that document is symbol type or not
  def aggregation(result, doc, conds, key2realkey = nil)
    if key2realkey.nil?
      key2realkey = {}
      doc.each_key do |key|
        key2realkey["$" + key] = key
      end
    end
    if conds.class == Hash
      conds.each do |k, c|
        case k.to_s
        when "$sum" then
          result = sum(result, doc, c, key2realkey)
        when "$max" then
          result = comp(result, doc, c, key2realkey, "max")
        when "$min" then
          result = comp(result, doc, c, key2realkey, "min")
        else
          @logger.warn("Unsupported Aggregation !!")
        end
      end
      return result
    else
      return real_value(doc, key2realkey[conds])
    end
  end

  def query(conds, value)
    conds.each_key do |cond_key|
      if MONGODB_NUMERIC_QUERY.include?(cond_key)
        result = change_string_to_num(conds[cond_key], value)
        unless numeric_query(cond_key, result["num"], result["cond_num"])
          return false
        end
      elsif MONGODB_STRING_QUERY.include?(cond_key)
        unless string_query(cond_key, value, cond[cond_key])
          return false
        end
      else
        @logger.warn("Unsupported operation '#{cond_key}' !!")
      end
    end
    true
  end

  private

  def change_string_to_num(cond, value)
    result = { "num" => 0, "cond_num" => 0 }
    if value.to_s.include?(".")
      result["num"] = value.to_f
      result["cond_num"] = cond.to_f
    else
      result["num"] = value.to_i
      result["cond_num"] = cond.to_i
    end
    result
  end

  def real_value(doc, conds__)
    if conds__.nil?
      return nil
    end
    conds = conds__.split("..")
    case conds.size
    when 1 then
      return real_value_one_cond(doc, conds[0])
    when 2 then
      return real_value_two_conds(doc, conds)
    when 3 then
      return real_value_three_conds(doc, conds)
    else
      @logger.error("[ERROR] Unsupported Deep layer(it means bigger than 3) Document.")
    end
  end

  def real_value_one_cond(doc, cond)
    ret = if doc[cond]
            doc[cond]
          else
            doc[cond.to_sym]
          end
    ret
  end

  def real_value_two_conds(doc, conds)
    ret = if doc[conds[0]]
            doc[conds[0]][conds[1]]
          else
            doc[conds[0].to_sym][conds[1].to_sym]
          end
    ret
  end

  def real_value_three_conds(doc, conds)
    ret = if doc[conds[0]]
            doc[conds[0]][conds[1]][conds[2]]
          else
            doc[conds[0].to_sym][conds[1].to_sym][conds[2].to_sym]
          end
    ret
  end

  ### Operater
  def sum(result, doc, c, key2realkey)
    if result.nil?
      result = 0
    end
    if c.to_i == 1
      # count the number of document
      result += 1
    else
      value = real_value(doc, key2realkey[c])
      result += value.to_i
    end
    result
  end

  def comp(result, doc, c, key2realkey, type)
    if result.nil?
      result = real_value(doc, key2realkey[c]).to_i
    else
      result_ = real_value(doc, key2realkey[c]).to_i
      if (type == "max" && result_ > result) ||
         (type == "min" && result_ < result)
        result = result_
      end
    end
    result
  end

  def numeric_query(operation, value, cond_value)
    if operation == "$gt"
      # Return False
      return value <= cond_value
    elsif operation == "$gte"
      return value < cond_value
    elsif operation == "$lt"
      ## Return false
      return value >= cond_value
    elsif operation == "$lte"
      ## Return false
      return value < cond_value
    else
      @logger.warn("Unsupported NUMERIC operation '#{operation}' !!")
    end
    true
  end

  def string_query(operation, value, cond_value)
    if %w[eq nq].include?(operation)
      string_query_eq(opration, value, cond_value)
    else
      @logger.warn("Unsupported STRING operation '#{operation}' !!")
    end
    true
  end

  def string_query_eq(operation, value, cond_value)
    if operation == "$eq"
      ## Return false
      if value != cond_value
        return false
      end
    elsif operation == "$ne"
      return false
    end
    true
  end
end
