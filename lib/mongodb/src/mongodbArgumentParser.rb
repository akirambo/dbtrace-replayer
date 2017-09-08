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
  KeyValueNum = 5
  ByteSize    = 5
  MONGODB_ACCUMULATORS = ["$sum","$avg","$first","$last",
                          "$max","$min","$push","$addToSet"]
  MONGODB_AGGREGATE_OPERATORS = [
    "$collStats","$project","$match","$redact",
    "$limit","$skip","$unwind","$group","$sample",
    "$sort","$geoNear","$lookup","$out","$indexStats",
    "$facet","$bucket","$bucketAuto","$sortByCount",
    "$addFields","$replaceRoot","$count","$graphLookup",
    "$unwind"
  ]
  def initialize(logger)
    @logger = logger
    @utils  = Utils.new
  end
  def exec(type,args,noString=false)
    return send("#{type}",args,noString)
  end
  def structureType(operand,args)
    structureType = "others"
    return structureType
  end
  private
  def parseLog(_log_)
    log =  '{ "key" :'+_log_
    log.gsub!("\"",'"')
    log.gsub!(/(\w+):/,'"\1":')
    log.gsub!('""','"')
    log.gsub!('"://',"://")
    log.gsub!(/:\s*"\s*,/,':"",')
    log.gsub!(/ObjectId\((\'\w+\')\)/,'"ObjectId(\1)"')   
    log.gsub!(/new Date\((\w+)\)/,'\1')
    log.gsub!(/\s+:\s+/,":")
    begin
      hash = JSON.parse(log)
    rescue JSON::ParserError => e
      @logger.error("--"*32)
      @logger.error("Log Parse Error : " + e.message)
      @logger.error("                : " + log)
    end
    return hash
  end
  def INSERT(args,noString)
    ## Parse Arguments
    result = []
    log = parseLog(args)
    key = log["key"]
    doc = log["documents"]
    if(doc.instance_of?(Array))then
      result.push([key,doc,false])
    else 
      ## Bulk insert / insert array
      @logger.info("Using Pseudo-data (#{KeyValueNum} key-value data) on Bulk Insert/ Array Insert.")
      doc.to_i.times do
        hash = {}
        KeyValueNum.times do
          hash[@utils.create_string(ByteSize)] = @utils.create_string(ByteSize)
        end
        if(noString)then
          result.push([key,hash,true])
        else
          result.push([key,@utils.convert_json(hash),true])
        end
      end
    end
    return result
  end
  def UPDATE(args,noString)
    result = {
      "key"    => nil,
      "query"  => nil,
      "update" => nil,
      "multi"  => true,
      "upsert" => false,
    }
    _data = args.split("updates: [")
    result["key"] = _data[0].gsub(/\"/,"").gsub(/\s/,"").sub(/,\Z/,"")
    data = _data[1].split("]")[0]
    begin
      hash = @utils.parse_json(data)
    rescue => e
      @logger.error(e.message)
      @logger.error(__FILE__)
      return nil
    end
    result["query"]  = hash["q"]
    result["update"] = hash["u"]
    result["multi"]  = hash["multi"]
    result["upsert"] = hash["upsert"]
    return result
  end
  def COUNT(args,noString)
    result = {
      "key"   => nil,
      "query" => nil,
      "fields" => nil
    }
    ## key
    result["key"] = args.split(",")[0].gsub("\"","").gsub(/\s/, "")
    str = "{" + args.sub(",","").sub(result["key"],"").sub(/\"/,"").sub(/\"/,"")
    ## query
    hash = @utils.parse_json(str)
    result["query"] = hash["query"]
    ## field
    result["fields"] = hash["fields"]
    return result
  end
  def GROUP(args,noString)
    ## Unimplements
    return nil
  end
  def FIND(args,noString)
    result = {
      "key"    => nil,
      "filter" => nil
    }
    ## key
    result["key"] = args.split(",")[0].gsub("\"","").gsub(/\s/, "")
    str = "{" + args.sub(",","").sub(result["key"],"").sub(/\"/,"").sub(/\"/,"")
    ## filter
    hash = @utils.parse_json(str)
    if(hash["filter"])then
      result["filter"] = hash["filter"]
    elsif(hash["deletes"] and hash["deletes"][0]["q"])then
      result["filter"] = hash["deletes"][0]["q"]
    end
    return result
  end
  def DELETE(args,noString)
    return FIND(args,noString)
  end
  def AGGREGATE(args,noString)
    result = {
      "key"    => nil,
      "match"  => nil,
      "group"  => nil,
      "unwind" => nil,
    }
    result["key"] = args.split(",")[0].gsub("\"","").gsub(/\s/, "")
    str = "{" + args.sub(",","").sub(result["key"],"").sub(/\"/,"").sub(/\"/,"")
    @utils.parse_json(str)["pipeline"].each{|elem|
      MONGODB_AGGREGATE_OPERATORS.each{|ope|
        if(elem[ope])then
          case ope
          when "$match" then
            result["match"] = elem["$match"].to_json
          when "$group" then
            result["group"]  = elem["$group"].to_json
          when "$unwind" then
            result["unwind"]  = "{\"path\": \"" + elem["$unwind"] + "\"}"
          end
        end
      }
    }
    return result
  end
  
  def MAPREDUCE(args,noString)
    @logger.warn("Unsupported MapReduce")
    return Hash.new
  end
end
