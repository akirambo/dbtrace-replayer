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

module Mongodb2CassandraOperation
  MONGODB_NUMERIC_QUERY = ["$gt","$gte","$lt","$lte"]
  MONGODB_STRING_QUERY  = ["$eq","$ne","$in","$nin"]
  private
  # @conv {"INSERT" => ["INSERT"]}
  def MONGODB_INSERT(args)
    args.each{|arg|
      name = arg[0]
      keyValue = nil
      if(arg[1] == nil)then
        return false
      elsif(arg[1].class == String)then
        begin
          keyValue  = parse_json(arg[1])
        rescue => e
          @logger.error(e.message)
          return false
        end
      elsif(arg[1].class == Hash)then
        keyValue = arg[1]
      elsif(arg[1].class == Array)then
        keyValue = parse_json(arg[1][0])
      end
      
      if(keyValue == nil or keyValue.keys.size == 0)then
        return false
      end
      command = ""
      ## convert _id --> monogoid ( only @schema Hash _id)
      if(keyValue.keys().include?("_id") and
         @schemas[name] and @schemas[name].fields)then
        keyValue["mongoid"] = keyValue["_id"].gsub("-","")
        keyValue.delete("_id")
      end
      if(@schemas[name] != nil)then
        ## args[2] == true means bulk 
        if(@schemas[name].check(keyValue,arg[2]))then
          filteredKeyValue = @schemas[name].extractKeyValue(keyValue)
          command = "INSERT INTO #{name} "
          command +=  "(" + filteredKeyValue["key"] + ") VALUES "
          command +=  "(" + filteredKeyValue["value"] + ");"
          begin
            DIRECT_EXECUTER(command)
          rescue => e
            @logger.error("Cannot Execute Command #{command}")
            @logger.error(" - Error Message #{e.message}")
            return false
          end
        else
          @logger.warn("Not Found Table [#{name}]. Please Create Table @ INSERT .") 
          return false
        end
      else
        return false
      end
    }
    return true
  end

  # @conv {"UPDATE" => ["UPDATE"]}
  def MONGODB_UPDATE(arg)
    name = arg["key"]
    command = "UPDATE #{name} SET"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    updates = []
    if(arg["update"] and arg["update"]["$set"])then
      arg["update"]["$set"].each{|key,value|
        if(@schemas[name].stringType(key))then
          updates.push(" #{key}='#{value}'")
        end
      }
      command += updates.join(",")
    end
    ## EXTRACT QUERY
    if(arg["query"] and query = mongodbParseQuery(arg["query"]))then
      if(query != "")then
        command +=  " WHERE " + query
      end
    end
    begin
      DIRECT_EXECUTER(command + ";")
    rescue => e
      @logger.error(e.message)
      @logger.error("Execute Command -- #{command}")
      return false
    end
    return true
  end

  # @conv {"FIND" => ["SELECT"]}
  def MONGODB_FIND(arg)
    name = arg["key"]
    command = "SELECT * FROM #{name}"
    result = true
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    if(arg["filter"] and query = mongodbParseQuery(arg["filter"]))then
      if(query.size > 0)then
        command += " WHERE " + query + " ALLOW FILTERING"
      end
    end
    begin
      result = DIRECT_EXECUTER(command + ";")
    rescue => e
      @logger.error("QUERY :: #{command}")
      @logger.error(e.message)
      result = ""
    end
    return result
  end
  # @conv {"COUNT" => ["SELECT"]}
  def MONGODB_COUNT(arg)
    command = "SELECT count(*) FROM #{arg["key"]}"
    ## EXTRACT NEW VALUE FOR EACH FIELD
    ## EXTRACT FILTER
    if(arg["filter"] and query = mongodbParseQuery(arg["filter"]))then
      command += " WHERE " + query
    end
    @logger.debug("Execute Command -- #{command}")
    begin
      result = DIRECT_EXECUTER(command+";")
      if(result.class == Hash)then
        return 0
      end
      return result.to_i
    rescue => e
      @logger.error("QUERY :: #{command}")
      @logger.error(e.message)
    end
    return 0
  end

  # @conv {"DELETE" => ["DELETE"]}
  def MONGODB_DELETE(arg)
    name = arg["key"]
    if(arg["filter"] == nil or arg["filter"].size == 0)then
      command = "TRUNCATE #{name};"
      begin
        DIRECT_EXECUTER(command)
      rescue => e
        return false
      end
    else
      command = "DELETE FROM #{name}"
      ## EXTRACT QUERY
      if(arg["filter"] and query = mongodbParseQuery(arg["filter"]))then
        where = " WHERE " + query
      end
      @logger.debug("Execute Command -- #{command} #{where}")
      exec = "#{command}#{where};"
      begin
        DIRECT_EXECUTER(exec)
      rescue => e
=begin
        if(e.message.include?("PRIMARY KEY"))then
          ## 1.SELECT DATA
          __ope__  = where.split(" ")
          newCom = "SELECT #{__ope__[1]} FROM #{name} "
          result = DIRECT_SELECT(newCom)
          ## 2.CREATE WHERE STATEMENTS
          newWhere = []
          result.each{|key,values|
            values.each{|elem|
              newWhere.push(" #{key} = #{elem} ")
            }
          }
          exec = "#{command} WHERE #{newWhere.join(" AND ")}"
          begin
            DIRECT_EXECUTER(exec)
          rescue => e
            @logger.error(e.message)
            @logger.debug("Execute Command -- #{command} WHERE #{newWhere}")
            return false
          end
        end
=end
        @logger.error(e.message)
        @logger.error(command)
        return false
      end
    end
    return true
  end

  # @conv {"AGGREGATE" => ["SELECT"]}
  def MONGODB_AGGREGATE(arg)
    name = @options[:keyspace]+"."+arg["key"]
    if(arg["key"].include?("."))then
      name = arg["key"]
    end
    arg.delete("key")
    targetKeys = @queryParser.targetKeys(arg)
    if(targetKeys.size > 0)then
      command  = "SELECT " + targetKeys.join(",") + " FROM #{name}"
    else
      command  = "SELECT * FROM #{name}"
    end
    if(arg["match"])then
      where = []
      arg["match"].each{|k,v|
        ## primary key ? 
        if(@schemas[name].primaryKeys.include?(k))then
          where.push("#{k} = '#{v}'")
        end
      }
      if(where.size > 0)
        command += " WHERE #{where.join(" AND ")}"
      end
    end
    begin
      ans = DIRECT_EXECUTER(command+";")
      docs = @queryParser.csv2docs(targetKeys, ans)
      params = @queryParser.getParameter(arg)
      result = {}
      firstFlag = true
      docs.each{|doc|
        ## create group key
        key = @queryParser.createGroupKey(doc,params["cond"])
        if(result[key] == nil)then
          result[key] = {}
        end
        params["cond"].each{|k,v|
          monitor("client","aggregate")
          result[key][k] = @queryProcessor.aggregation(result[key][k],doc,v)
          monitor("client","aggregate")
        }
      }
    rescue => e
      @logger.error(command)
      @logger.error(e.message)
    end
    return result
  end
  #############
  ## PREPARE ##
  #############
  def prepare_mongodb(operand, args)
    result = {"operand" => "MONGODB_#{operand}", "args" => nil}
    result["args"] = @parser.exec(operand,args)
    return result
  end

  def mongodbParseQuery(hash)
    where = []
    idWhere = []
    hash.each{|col, queries|
      if(queries.class == Hash)then
        queries.each{|operand,value|
          case operand
          when "$gt" then
            where.push("#{col} > #{value}")
          when "$gte" then
            where.push("#{col} >= #{value}")
          when "$lt" then
            where.push("#{col} < #{value}")
          when "$lte" then
            where.push("#{col} <= #{value}")
          else
            @logger.warn("Unsupported #{operand}")
          end
        }
      else
        if(col == "_id")then
          idWhere.push("mongoid = '#{queries.gsub("-","")}'")
        else
          where.push("#{col} = '#{queries}'")
        end
      end
    }
    query = where.join(" AND ")
    if(query.size > 0 and idWhere.size > 0)then
      query += " AND (" + idWhere.join(" OR ") + ")"
    elsif(query.size == 0 and idWhere.size > 0)then
      return idWhere.join(" OR ")
    end
    return where.join(" AND ")
  end
end

