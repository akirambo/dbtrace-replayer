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

module MongodbOperation
  private
  def INSERT(args)
    v = false
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    if(args.size == 0)then
      ## do nothing
      return true
    end
    connect
    falseFlag = false
    args.each{|arg|
      names = arg[0].split(".")
      if(names.size == 1)then
        names.unshift("dummy")
      end
      @client.setDatabaseName(names[0])
      @client.setCollectionName(names[1])
      json = @utils.add_doublequotation(arg[1])
      if(@option[:async])then
        v = @client.commitDocument(json)
        add_count("INSERT")
      else
        v = @client.syncExecuter("#{__method__}","#{json}")
        add_duration(@client.getDuration(),"database",__method__)
      end
      if(!v)then
        close
        return v
      end
    }
    close
    return v
  end
  def UPDATE(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    query = @utils.add_doublequotation(args["query"])
    doc   = @utils.add_doublequotation(args["update"])
    v     = @client.update(query,doc,args["multi"])
    close
    return v
  end
  def FIND(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    results = []
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    json = @utils.add_doublequotation(args["filter"])
    v = @client.find("#{json}")
    add_duration(@client.getDuration(),"database",__method__)
    close
    if(v)then
      rows = reply2rows(@client.getReply())
      results = []
      if(rows.size > 0)then
        rows.each{|row|
          row.gsub(":","=>")
          results.push(eval(row.gsub(":","=>")))
        }
      end
    end
    close
    return results
  end
  def DELETE(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    filter = @utils.add_doublequotation(args["filter"])
    v = @client.deleteExecuter(filter,true)
    add_duration(@client.getDuration(),"database",__method__)
    close
    return v
  end
  def COUNT(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect 
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    filter = @utils.add_doublequotation(args["query"])
    count = @client.count(filter)
    add_duration(@client.getDuration(),"database",__method__)
    close
    return count
  end
  def AGGREGATE(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    ["match","group","unwind"].each{|type|
      if(args[type])then
        @client.setAggregateCommand(type,args[type])
      end
    }
    v = @client.aggregate()
    add_duration(@client.getDuration(),"database",__method__)
    close
    if(v)then
      return @client.getReply()
    end
    return ""
  end
  def MAPREDUCE(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    @logger.warn("Unimplemented..")
  end
  def DROP(args,initFlag=false)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    r = false
    if(args.size == 1)then
      names = args[0].split(".")
      if(names[0] and names[1])then
        @client.setDatabaseName(names[0])
        @client.setCollectionName(names[1])
      elsif(names[0])then
        @client.setDatabaseName(names[0])
        @client.clearCollectionName()
      end
      r = @client.drop()
      if(@metrics and !initFlag)then
        add_duration(@client.getDuration(),"database",__method__)
      end
    end
    close
    return r
  end
  def reply2rows(str)
    rows = str.gsub(/\"/,'"').split("\n")
    return rows
  end
  ###########
  # PREPARE #
  ###########
  def prepare_MONGODB(operand,args)
    result = {}
    result["operand"] = operand
    result["args"] = @parser.exec(operand,args,true)
    return result
  end
end
