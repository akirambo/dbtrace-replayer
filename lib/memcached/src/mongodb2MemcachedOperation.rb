# coding: utf-8

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

module MongoDB2MemcachedOperation
  private
  # @conv {"INSERT" => ["GET","SET"]}
  def MONGODB_INSERT(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      return mongodbInsertKeyvalue(args)
    when "DOCUMENT" then
      return mongodbInsertDocument(args)
    end
    return false
  end
  # @conv {"UPDATE" => ["query@client","GET","REPLACE"]}
  def MONGODB_UPDATE(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      return mongodbUpdateKeyvalue(args)
    when "DOCUMENT" then
      return mongodbUpdateDocument(args)
    end
    return false
  end
  # @conv {"FIND" => ["mongodbQuery@client","GET"]}
  ## args = {"key"=>key, "filter"=>filter} 
  def MONGODB_FIND(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      docs = GET([args["key"]+args["filter"]["_id"]])
      return documentNormalize(docs)
    when "DOCUMENT" then
      return mongodbFindDocument(args)
    end
    return []
  end
  # @conv {"DELETE" => ["mongodbQuery@client","GET","DELETE","REPLACE"]}
  def MONGODB_DELETE(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      col = args["key"]+args["filter"]["_id"]
      return DELETE([col])
    when "DOCUMENT" then
      return mongodbDeleteDocument(args)
    end
    return false
  end

  # @conv {"COUNT" => ["query@client","GET"]}
  def MONGODB_COUNT(args)
    case @options[:datamodel]
    when "KEYVALUE" then
      data = GET([args["key"]+args["query"]["_id"]],false)
      if(data.size > 0)then
        return 1
      end
    when "DOCUMENT" then
      return mongodbCountDocument(args)
    end
    return 0
  end
  # @conv {"AGGREGATE" => ["query@client","accumulationclient","GET"]}
  ## args = {"key"=>key, "match"=>{"colname1"=>"STRING"}, "group"=>"{}", "unwind"=>"{}"}
  def MONGODB_AGGREGATE(args)
    result    = {}
    parameter = {}
    newhash   = {}
    matchDuration  = 0.0
    aggregateDuration  = 0.0
    add_count(:match)
    add_count(:aggregate)
    data = GET([args["key"]],false)
    docs = @utils.symbolhash2stringhash(eval(data))
    params = @queryParser.getParameter(args)
    firstFlag = true
    key2realkey = nil
    docs.each{|doc|
      startTime = Time.now
      matchFlag = mongodbQuery(doc,args["match"],"match")
      matchDuration = startTime - Time.now
      if(matchFlag)then
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
          startTime = Time.now
          result[key][k] = @queryProcessor.aggregation(result[key][k],doc,v,key2realkey)
          aggregateDuration += Time.now - startTime
        }
      end
    }
    add_duration(matchDuration,"client","match")
    add_duration(aggregateDuration,"client","aggregate")
    return result
  end

  ###################
  ## QUERY PROCESS ##
  ###################
  ### Supported   :: Single Query
  def mongodbQuery(doc,query,type)
    if(query)then
      query = documentSymbolize(query)
      query.each{|key, cond|
        if(doc[key])then
          value = doc[key]
          if(!cond.kind_of?(Hash))then
            value = doc[key].gsub("\"","")
            ## Field Matching
            tt = value.gsub("\"","").gsub(" ","")
            cond = cond.gsub(" ","")
            if(tt != cond)then
              return false
            end
          else
            if(!@queryProcessor.query(cond,value))then
              return false
            end
          end
        else
          return false
        end
      }
    end
    return true
  end

  def MONGODB_REPLACE(doc,args)
    hashedDoc = parse_json(doc)
    args["update"].each{|operation, values|
      case operation
      when "$set" then
        values.each{|key,value|
          hashedDoc[key] = value
        }
      else
        ## check colum_name
        if(!values.kind_of?(Hash))then
          hashedDoc[operation] = values
        end
        #@logger.warn("Unsupported UPDATE Operation '#{operation}' !!")
      end
    }
    return convert_json(hashedDoc)
  end
  #############
  ## PREPARE ##
  #############
  def prepare_MONGODB(operand, args)
    result = {"operand" => "MONGODB_#{operand}", "args" => nil}
    result["args"] = @parser.exec(operand,args)
    return result
  end
  ####################
  ## Private Method ##
  ####################

  ## INSERT ##
  def mongodbInsertKeyvalue(args)
    args.each{|arg|
      if(!arg[1].instance_of?(Array))then
        key = arg[0]+arg[1]["_id"]
        value = ""
        arg[1].each{|k,v|
          if(k != "_id")then
            value = v
          end
        }
        if(!SET([key,value]))then
          return false
        end
      else
        arg[1].each{|kv|
          key = arg[0]+kv["_id"]
          value = ""
          kv.each{|k,v|
            if(k != "_id")then
              value = v
            end
          }
          if(!SET([key,value]))then
            return false
          end
        }
      end
    }
    return true
  end
  def mongodbInsertDocument(args)
    ## Create New Data
    key = args[0][0]
    docs = @utils.stringhash2symbolhash(args[0][1])
    docs.each{|doc|
      doc[:_id].sub!(/ObjectId\(\'(\w+)\'\)/,'\1')
    }
    ## GET exists data
    preDocs = ""
    if(preDocs__ = GET([key]) and
        preDocs__.size > 0)then
      preDocs = documentNormalize(preDocs__)
    end
    if(preDocs and preDocs.class == Array and 
        preDocs.size > 0)then
      docs.concat(preDocs)
    end
    ## Commit
    if(!SET([key,docs.to_json]))then
      return false
    end
    return true
  end
  ## UPDATE ##
  def mongodbUpdateKeyvalue(args)
    col = args["key"]+args["query"]["_id"]
    newVal = ""
    args["update"]["$set"].each{|k,v|
      newVal = v
    }
    return REPLACE([col, newVal])
  end
  def mongodbUpdateDocument(args)
    results = []
    if(args["update"] and args["update"]["$set"])then
      newVals = documentSymbolize(args["update"]["$set"])
      data = GET([args["key"]])
      docs = documentNormalize(data)
      replaceFlag = true
      docs.each_index{|index|
        if(replaceFlag)then
          doc = docs[index]
          if(args["query"] == nil or args["query"] = {} or 
              mongodbQuery(doc,args["query"],"query"))then
            newVals.each{|k,v|
              doc[k] = v
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
      return false
    end
    value = results.to_json
    return REPLACE([args["key"], value])
  end

  ## FIND ##
  def mongodbFindDocument(args)
    data = GET([args["key"]])
    docs = []
    if(data)then
      docs = documentNormalize(data)
    end
    results =[]
    if(args["filter"] == nil or args["filter"] == {})then
      docs.each{|doc|
        results.push(@utils.symbolhash2stringhash(doc))
      }
    else
      docs.each_index{|index|
        doc = docs[index]
        if(doc.class == Array)then
          doc.each_index{|idx|
            if(mongodbQuery(doc[idx],args["filter"],"filter"))then
              results.push(@utils.symbolhash2stringhash(doc[idx]))
            end
          }
        else
          if(mongodbQuery(doc,args["filter"],"filter"))then
            results.push(@utils.symbolhash2stringhash(doc))
          end
        end
      }
    end
  end
  ## DELETE ##
  def mongodbDeleteDocument(args)
    if(args["filter"].size == 0)then
      return DELETE([args["key"]])
    else
      data = GET([args["key"]])
      newDocs = []
      docs = documentNormalize(data)
      docs.each_index{|index|
        if(!mongodbQuery(docs[index],args["filter"],"filter"))then
            newDocs.push(docs[index])
        end
      }
      if(newDocs.size == 0)then
        return DELETE(args["key"])
      else
        return REPLACE([args["key"],newDocs.to_json])
      end
    end
  end
  ## COUNT ##
  def mongodbCountDocument(args)
    count = 0
    docs = ""
    docs__ = GET([args["key"]])
    if(args["query"].keys.size > 0 and docs__.size > 0)then
      docs = documentNormalize(docs__)
      docs.each{|doc|
        flag = true
        filters = documentSymbolize(args["query"])
        filters.each{|key,value|
          if(doc[key] != value)then
            flag = false
            next
          end
          if(flag)then
            count += 1
          end
        }
      }
    end
    return count 
  end
  def documentSymbolize(docs)
    if(docs.class == Array)then
      symbolDocs = Array.new
      docs.each{|__doc__|
        doc = Hash[__doc__.map{|k,v| [k.to_sym,v]}]
        symbolDocs.push(doc)
      }
    elsif(docs.class == Hash)then
      symbolDocs = Hash[docs.map{|k,v| [k.to_sym,v]}]
    end
    return symbolDocs
  end
  def documentNormalize(data)
    docs = nil
    if(data[0] == "[")then
      docs = eval(data)
    else
      docs = eval("["+data+"]")
    end
    return docs
  end
end

