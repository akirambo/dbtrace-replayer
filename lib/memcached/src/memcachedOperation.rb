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

module MemcachedOperation
  private
  def SET(args)
    return executeQuery("#{__method__}",args[0].to_s,args[1].to_s,expireTime(args))
  end
  #def GET(args, stdout=true,asyncable=true)
  def GET(args,asyncable=true)
    @logger.debug("GENERATED QUERY: #{__method__} #{args[0]}")
    value = ""
    connect
    if(@options[:async] and asyncable)then
      @mget = true
      add_count(__method__)
      @client.commitGetKey(args[0])
    else
      ret = @client.syncExecuter("#{__method__}",args[0].to_s,"",0)
      add_duration(@client.getDuration(),"database",__method__)
      if(ret)then
        value = @client.getReply()
      end
    end
    close
    return value 
  end
  def ADD(args)
    #[previous version] :: r = @client.add(args[0],args[1],0,:raw => true)
    return executeQuery("#{__method__}",args[0].to_s,args[1].to_s)
  end
  def REPLACE(args)
    return executeQuery("#{__method__}",args[0].to_s,args[1].to_s,expireTime(args))
  end
  def APPEND(args)
    return executeQuery("#{__method__}",args[0].to_s,args[1].to_s)
  end
  def PREPEND(args)
    return executeQuery("#{__method__}",args[0].to_s,args[1].to_s)
  end
  def CAS(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    @logger.warn("Unimplemented..")
    return false
  end
  def INCR(args)
    return executeQuery("#{__method__}",args[0],args[1])
  end
  def DECR(args)
    return executeQuery("#{__method__}",args[0],args[1])
  end
  def DELETE(args)
    return executeQuery("#{__method__}",args[0],args[1])
  end
  def FLUSH (args,initFlag=false)
    return executeQuery("#{__method__}","","",0,initFlag)
  end
  def KEYLIST()
    connect 
    keys = @client.keys()
    close
    return keys.split(",")
  end
  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand,args)
    result = {}
    result["operand"] = operand.upcase
    result["args"]    = @parser.exec(operand.upcase,args)
    return result
  end
  def expireTime(args)
    if(args.size == 3)then
      return args[2].to_i
    end
    ## Not Set Expire Time (ttl = 0 )
    return 0
  end
  def executeQuery(operand,arg0,arg1,ttl=0,initFlag=false)
    if(ttl != 0)then
      @logger.debug("GENERATED QUERY: #{__method__} #{arg0} #{arg1} #{ttl}")
    else
      @logger.debug("GENERATED QUERY: #{__method__} #{arg0} #{arg1}")
    end
    connect
    r = @client.syncExecuter(operand,arg0.to_s,arg1.to_s,ttl)
    if(operand != "FLUSH" or (@metrics and !initFlag))then
      add_duration(@client.getDuration(),"database",__method__)
    end
    close
    return r
  end
end
