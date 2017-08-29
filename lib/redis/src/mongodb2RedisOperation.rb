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


module MongoDB2RedisOperation
  MONGODB_NUMERIC_QUERY = ["$gt","$gte","$lt","$lte"]
  MONGODB_STRING_QUERY  = ["$eq","$ne","$in","$nin"]
  private
  # @conv {"INSERT" => ["SET"]}
  ## args[0] --> skip, args[1] = {_id => xx, value => xx, ...}
  def MONGODB_INSERT(args)
    v = "NG"
    if(@options[:datamodel] == "DOCUMENT")then
      ## Documents [SADD]
      if(args[0] and args[0][0] and args[0][1][0])then
        doc = {
          "key"  => args[0][0],
          "args" => args[0][1][0].to_json
        }
        doc["args"].gsub!("'","")
        doc["args"].gsub!(/\s/,"")
      end
      v = SADD(doc)
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    return v
  end
  # @conv {"UPDATE" => ["SMEMBERS","QUERY@client","DEL","SADD"]}
  def MONGODB_UPDATE(args)
    results = []
    if(@options[:datamodel] == "DOCUMENT")then
      ## Documents
      if(args["update"] and args["update"]["$set"])then
        newVals = args["update"]["$set"]
        data = GET([args["key"]])
        docs = eval("["+data+"]")
        replaceFlag = true
        docs.each_index{|index|
          if(replaceFlag)then
            doc = parseJSON(docs[index])
            if(args["query"] == nil or args["query"] = {} or mongodbQuery(doc,args["query"]))then
              newVals.each{|k,v|
                doc[k.to_sym] = v.gsub(/\s/,"")
              }
            end
            results.push(doc)
            if(!args["multi"])then
              replaceFlag = false
            end
          else
            results.push(docs[index])
          end
        }
      else
        @logger.error("Not Set update $set query @ mongodb2redis")
        return "NG"
      end
      r = SET([args["key"], results.to_json])
      return r
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    return "NG"
  end
  # @conv {"FIND" => ["GET,"QUERY@client"]}
  def MONGODB_FIND(args)
    results = []
    case @options[:datamodel]
    when "DOCUMENT" then
      ## Documents
      data = SMEMBERS([args["key"]],true)
      docs = []
      if(data)then
        docs = eval("["+data+"]")
      end
      results =[]
      if(args["filter"] == nil or args["filter"] == {})then
        results = docs
      else
        docs.each{|doc|
          if(mongodbQuery(doc,args["filter"]))then
            results.push(doc)
          end
        }
      end
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
      return "NG"
    end
    return results
  end
  # @conv {"DELETE" => ["SMEMBERS","QUERY@client","SREM"]}
  def MONGODB_DELETE(args)
    v = "NG"
    case @options[:datamodel]
    when "DOCUMENT"
      if(args["filter"].size == 0)then
        v = DEL([args["key"]])
      else
        data = GET([args["key"]])
        newDocs = []
        docs = eval("["+data+"]")
        docs.each_index{|index|
          doc = parseJSON(docs[index])
          if(!mongodbQuery(doc,args["filter"]))then
            newDocs.push(convJSON(doc))
          end
        }
        if(newDocs.size == 0)then
          v = DEL(args["key"])
        else
          value = newDocs.to_json
          v = SET([args["key"],value])
        end
      end
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    return v
  end
  # @conv {"FINDANDMODIFY" => ["undefined"]}
  def MONGODB_FINDANDMODIFY(args)
    @logger.debug("MONGODB_FINDANDMODIFY is not implemented")
    return "NG"
  end
  # @conv {"COUNT" => ["SMEMBERS","QUERY@client","COUNT@client"]}
  def MONGODB_COUNT(args)
    count  = 0
    case @options[:datamodel] 
    when "DOCUMENT" then
      docs = SMEMBERS([args["key"]],true)
      monitor("client","count")
      args["query"].each_key{|k|
        query = ""
        case args["query"][k].class.to_s 
        when "FalseClass" then
          query = '"'+k.split(".").last+'":false'
        when "TrueClass" then
          query = '"'+k.split(".").last+'":true'
        when "String" then
          query = '"'+k.split(".").last+'":"'+args["query"][k]+'"'
        when "Hash" then
          query = '"'+k.split(".").last+'":'+ args["query"][k].to_json
        when "Integer" then
          query = '"'+k.split(".").last+'":'+args["query"][k].to_s
        when "Float" then
          query = '"'+k.split(".").last+'":'+args["query"][k].to_s
        end
        _count_ =  docs.scan(query).size
        if(_count_ == 0)then
          count = 0
          break
        else
          if(count == 0  or count > _count_)then
            count = _count_
          end
        end
      }
      monitor("client","count")
    else
      @logger.error("Unsupported Data Model @ mongodb2redis #{@options[:datamodel]}")
    end
    return count
  end
  # @conv {"AGGREGATE" => ["SMEMBERS","ACCUMULATION@client"]}
  def MONGODB_AGGREGATE(args)
    @logger.debug("MONGODB_AGGREGATE")
    docs = SMEMBERS([args["key"]],true)
    result = {}
    params = @queryParser.getParameter(args)
    docs = eval("["+docs+"]")
    firstFlag = true
    key2realkey = nil
    docs.each{|doc|
      monitor("client", "match")
      flag = mongodbQuery(doc,args["match"])
      monitor("client", "match")
      if(flag)then
        if(firstFlag)then
          key2realkey = @queryParser.createKey2RealKey(doc,params["cond"])
          firstFlag = false
        end
        # create group key
        key = @queryParser.createGroupKey(doc,params["cond"])
        if(result[key] == nil)then
          result[key] = {}
        end
        # do aggregation
        params["cond"].each{|k,v|
          monitor("client","aggregate")
          result[key][k] = @queryProcessor.aggregation(result[key][k],doc,v,key2realkey)
          monitor("client","aggregate")
        }
      end
    }
    return result
  end
  # @conv {"MAPREDUCE" => ["undefined"]}
  def MONGODB_MAPREDUCE(args)
    @logger.warn("Unsupported MapReduce")
    return "NG"
  end
  ###################
  ## QUERY PROCESS ##
  ###################
  def mongodbQuery(doc,query)
    if(query and query.class == Hash and query.keys.size > 0)then
      query.each{|_key, cond|
        key = _key.to_sym
        if(doc[key])then
          value = doc[key]
          if(cond.kind_of?(Hash))then
            if(!@queryProcessor.query(cond,value))then
              return false
            end
          elsif(value.class == String)then
            ## Field Matching
            tt = value.gsub("\"","").gsub(" ","")
            conds = cond.gsub(" ","")
            if(tt != conds)then
              return false
            end
          elsif(value.class == Integer)then
            return value == cond.to_i
          elsif(value.class == Float)then
            return value == cond.to_f
          end
        else
          return false
        end
        return true
      }
    end
    return false
  end

  #############
  ## PREPARE ##
  #############
  def prepare_MONGODB(operand, args)
    result = {"operand" => "MONGODB_#{operand}", "args" => nil}
    result["args"] = @parser.exec(operand,args)
    return result
  end
end

