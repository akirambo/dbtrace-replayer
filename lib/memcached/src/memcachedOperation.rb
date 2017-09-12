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
    execute_query(__method__.to_s, args[0].to_s, args[1].to_s, expiretime(args))
  end

  def GET(args, asyncable = true)
    @logger.debug("GENERATED QUERY: #{__method__} #{args[0]}")
    value = ""
    connect
    if @option[:async] && asyncable
      @mget = true
      add_count(__method__)
      @client.commitGetKey(args[0])
    else
      ret = @client.syncExecuter(__method__.to_s, args[0].to_s, "", 0)
      add_duration(@client.getDuration, "database", __method__)
      if ret
        value = @client.getReply
      end
    end
    close
    value
  end

  def ADD(args)
    execute_query(__method__.to_s, args[0].to_s, args[1].to_s)
  end

  def REPLACE(args)
    execute_query(__method__.to_s, args[0].to_s, args[1].to_s, expiretime(args))
  end

  def APPEND(args)
    execute_query(__method__.to_s, args[0].to_s, args[1].to_s)
  end

  def PREPEND(args)
    execute_query(__method__.to_s, args[0].to_s, args[1].to_s)
  end

  def CAS(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    @logger.warn("Unimplemented..")
    false
  end

  def INCR(args)
    execute_query(__method__.to_s, args[0], args[1])
  end

  def DECR(args)
    execute_query(__method__.to_s, args[0], args[1])
  end

  def DELETE(args)
    execute_query(__method__.to_s, args[0], args[1])
  end

  def FLUSH(_, initflag = false)
    execute_query(__method__.to_s, "", "", 0, initflag)
  end

  def KEYLIST
    connect
    keys = @client.keys
    close
    keys.split(",")
  end

  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand, args)
    result = {}
    result["operand"] = operand.upcase
    result["args"] = @parser.exec(operand.upcase, args)
    result
  end

  def expiretime(args)
    if args.size == 3
      return args[2].to_i
    end
    ## Not Set Expire Time (ttl = 0 )
    0
  end

  def execute_query(operand, arg0, arg1, ttl = 0, initflag = false)
    if ttl != 0
      @logger.debug("GENERATED QUERY: #{__method__} #{arg0} #{arg1} #{ttl}")
    else
      @logger.debug("GENERATED QUERY: #{__method__} #{arg0} #{arg1}")
    end
    connect
    r = @client.syncExecuter(operand, arg0.to_s, arg1.to_s, ttl)
    if operand != "FLUSH" || (@metrics && !initflag)
      add_duration(@client.getDuration, "database", __method__)
    end
    close
    r
  end
end
