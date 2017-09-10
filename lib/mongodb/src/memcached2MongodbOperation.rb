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

module Memcached2MongodbOperation
=begin
  collecition = "testCol"
  doc {"key" => key, "value" => value}
=end
  private
  # @conv {"SET" => ["INSERT"]}
  def MEMCACHED_SET(args)
    r = false
    value = change_numeric_when_numeric(args[1])
    if(args.size == 3 )then
      r = INSERT([["testdb.col",{"_id" => args[0],"value"=>value,"expire"=>args[2]}]])
    elsif(args.size == 2)then
      r = INSERT([["testdb.col",{"_id" => args[0],"value"=>value}]])
    else
      @logger.fatal("Unsupprted Arguments [#{args}] @ #{__method__} ")
    end
    return r
  end
  # @conv {"GET" => ["FIND"]}
  def MEMCACHED_GET(args)
    cond = {
      "key" => "testdb.col",
      "filter" => {"_id" => args[0]}
    }
    v = FIND(cond)
    ret = []
    v.each{|doc|
      ret.push(doc["value"])
    }
    return ret.join(",")
  end
  # @conv {"ADD" => ["SET"]}
  def MEMCACHED_ADD(args)
    return MEMCACHED_SET(args)
  end
  # @conv {"REPLACE" => ["UPDATE"]}
  def MEMCACHED_REPLACE(args)
    value = change_numeric_when_numeric(args[1])
    cond = {
      "key" => "testdb.col",
      "query" => {"_id" => args[0]},
      "update" => {"$set" => {"value" => value}},
    }
    return UPDATE(cond)
  end
  # @conv {"GETS" => ["GET"]}
  def MEMCACHED_GETS(args)
    return MEMCACHED_GET(args)
  end
  # @conv {"APPEND" => ["FIND","UPDATE"]}
  def MEMCACHED_APPEND(args)
    cond = {
      "key" => "testdb.col",
      "filter" => {"_id" => args[0]},
      "query" => {"_id" => args[0]}
    }
    str = FIND(cond)[0]["value"].to_s
    str += args.last.to_s
    cond["update"] = {"$set" => {"value" => str}}
    return UPDATE(cond)
  end
  # @conv {"PREPEND" => ["FIND","UPDATE"]} 
  def MEMCACHED_PREPEND(args)
    cond = {
      "key" => "testdb.col",
      "filter" => {"_id" => args[0]},
      "query" => {"_id" => args[0]}
    }
    str = FIND(cond)[0]["value"].to_s
    str = args.last.to_s + str
    cond["update"] = {"$set" => {"value" => str}}
    return UPDATE(cond)
  end
  # @conv {"CAS" => ["SET"]}
  def MEMCACHED_CAS(args)
    args.pop
    return MEMCACHED_SET(args)
  end
  # @conv {"INCR" => ["UPDATE"]}
  def MEMCACHED_INCR(args)
    value = change_numeric_when_numeric(args[1])
    cond = {
      "key" => "testdb.col",
      "query" => {"_id" => args[0]},
      "update" =>  {"$inc" => {"value" => value}},
      "multi"  => true
    }
    return UPDATE(cond)
  end
  # @conv {"DECR" => ["UPDATE"]}
  def MEMCACHED_DECR(args)
    value = change_numeric_when_numeric(args[1])*-1
    cond = {
      "key" => "testdb.col",
      "query" => {"_id" => args[0]},
      "update" =>  {"$inc" => {"value" => value}},
      "multi"  => true
    }
    return UPDATE(cond)
  end
  # @conv {"DELETE" => ["DELETE"]}
  def MEMCACHED_DELETE(args)
    cond = {
      "key" => "testdb.col",
      "filter" => {"_id" => args[0]}
    }
    return DELETE(cond)
  end
  # @conv {"FLUSH" => ["DROP"]}
  def MEMCACHED_FLUSH(args)
    return DROP(["testdb.col"])
  end
  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand,args)
    result = {}
    ## PREPARE SPECIAL OPERATION
    operand.upcase!
    if(["FLUSHALL"].include?(operand))then
      result["operand"] = operand
      return result
    end
    
    ## PREPARE OPERATION & ARGS
    result["operand"] = "MEMCACHED_#{operand.upcase}"
    result["args"] = @parser.exec(operand.upcase, args)
    return result
  end
end
