
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

require_relative "../../common/utils"

class MongodbArgumentParser
  MONGODB_ACCUMULATORS = %w[$sum $avg $first $last $max $min $push $addToSet].freeze
  MONGODB_AGGREGATE_OPERATORS = %w[$collStats $project $match $redact $limit $skip $unwind $group $sample $sort $geoNear $lookup $out $indexStats $facet $bucket $bucketAuto $sortByCount $addFields $replaceRoot $count $graphLookup $unwind].freeze

  def initialize(logger)
    @logger = logger
    @utils  = Utils.new
    @key_value_num = 5
    @byte_size = 5
  end

  def exec(type, args, no_string = false)
    send(type.to_s, args, no_string)
  end

  def structure_type(_, _)
    "others"
  end

  private

  def parse_log(log_)
    log = '{ "key" :' + log_
    log.tr!("\"", '"')
    log.gsub!(/(\w+):/, '"\1":')
    log.gsub!('""', '"')
    log.gsub!('"://', "://")
    log.gsub!(/:\s*"\s*,/, ':"",')
    log.gsub!(/ObjectId\((\'\w+\')\)/, '"ObjectId(\1)"')
    log.gsub!(/new Date\((\w+)\)/, '\1')
    log.gsub!(/\s+:\s+/, ":")
    begin
      hash = JSON.parse(log)
    rescue JSON::ParserError => e
      @logger.error("--" * 32)
      @logger.error("Log Parse Error : " + e.message)
      @logger.error("                : " + log)
    end
    hash
  end

  def insert(args, no_string)
    ## Parse Arguments
    result = []
    log = parse_log(args)
    key = log["key"]
    doc = log["documents"]
    if doc.instance_of?(Array)
      result.push([key, doc, false])
    else
      ## Bulk insert / insert array
      @logger.info("Using Pseudo-data #{doc.to_i} documents  (#{@key_value_num} key-value data) on Bulk Insert/ Array Insert.")
      doc.to_i.times do
        hash = {}
        @key_value_num.times do |index|
          if index == 0
            hash[:_id] = @utils.create_string(@byte_size)
          else
            hash[@utils.create_string(@byte_size)] = @utils.create_string(@byte_size)
          end
        end
        #if no_string
        result.push([key, [hash], true])
        #else
        #  result.push([key, [@utils.convert_json(hash)], true])
        #end
      end
    end
    result
  end

  def upsert(args, no_string)
    insert(args, no_string)
  end

  def update(args, _)
    result = {
      "key"    => nil,
      "query"  => nil,
      "update" => nil,
      "multi"  => true,
      "upsert" => false,
    }
    data__ = args.split("updates: [")
    result["key"] = data__[0].delete("\"").delete(" ").sub(/,\Z/, "")
    data = data__[1].split("]")[0]
    begin
      hash = @utils.parse_json(data)
    rescue => e
      @logger.error(e.message)
      @logger.error(__FILE__)
      return nil
    end
    result["query"] = hash["q"]
    result["update"] = hash["u"]
    result["multi"] = hash["multi"]
    result["upsert"] = hash["upsert"]
    result
  end

  def count(args, _)
    result = {
      "key"   => nil,
      "query" => nil,
      "fields" => nil,
    }
    ## key
    result["key"] = args.split(",")[0].delete("\"").delete(" ")
    str = get_non_key_string(args, result["key"])
    ## query
    hash = @utils.parse_json(str)
    result["query"] = hash["query"]
    ## field
    result["fields"] = hash["fields"]
    result
  end

  def group(_, _)
    nil
  end

  def find(args, _)
    result = {
      "key"    => nil,
      "filter" => nil,
    }
    ## key
    result["key"] = args.split(",")[0].delete("\"").delete(" ")
    str = get_non_key_string(args, result["key"])
    ## filter
    hash = @utils.parse_json(str)
    if hash["filter"]
      result["filter"] = hash["filter"]
    elsif hash["deletes"] && hash["deletes"][0]["q"]
      result["filter"] = hash["deletes"][0]["q"]
    end
    result
  end

  def get_non_key_string(args, key)
    "{" + args.sub(",", "").sub(key, "").sub(/\"/, "").sub(/\"/, "")
  end

  def delete(args, no_string)
    find(args, no_string)
  end

  def aggregate(args, _)
    result = {
      "key"    => nil,
      "match"  => nil,
      "group"  => nil,
      "unwind" => nil,
    }
    result["key"] = args.split(",")[0].delete("\"").delete(" ")
    str = get_non_key_string(args, result["key"])
    @utils.parse_json(str)["pipeline"].each do |elem|
      MONGODB_AGGREGATE_OPERATORS.each do |ope|
        if elem[ope]
          case ope
          when "$match" then
            result["match"] = elem["$match"].to_json
          when "$group" then
            result["group"] = elem["$group"].to_json
          when "$unwind" then
            result["unwind"] = "{\"path\": \"" + elem["$unwind"] + "\"}"
          end
        end
      end
    end
    result
  end

  def mapreduce(_, _)
    @logger.warn("Unsupported MapReduce")
    {}
  end
end
