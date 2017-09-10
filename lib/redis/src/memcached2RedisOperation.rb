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

module Memcached2RedisOperation
  private

  # @conv {"SET(args x3)" => ["SETEX"]}
  # @conv {"SET(args x2)" => ["SET"]}
  def MEMCACHED_SET(args)
    v = "NG"
    if args.size == 3
      v = SETEX(args)
    elsif args.size == 2
      v = SET(args)
    else
      @logger.error("Unsupported Arguments #{args} @#{__method__}")
    end
    v
  end

  # @conv {"GET" => ["GET"]}
  def MEMCACHED_GET(args)
    GET(args)
  end

  # @conv {"ADD(args x3)" => ["GET","SETEX"]}
  # @conv {"ADD(args x2)" => ["GET","SET"]}
  def MEMCACHED_ADD(args)
    v = "NG"
    if GET([args[0]]).nil?
      v = MEMCACHED_SET(args)
    end
    v
  end

  # @conv {"REPLACE" => ["GET","SET"]}
  def MEMCACHED_REPLACE(args)
    v = "NG"
    unless GET([args[0]]).nil?
      v = MEMCACHED_SET(args)
    end
    v
  end

  # @conv {"GETS" => ["GET"]}
  def MEMCACHED_GETS(args)
    GET(args)
  end

  # @conv {"APPEND(args x3)" => ["GET","SETEX"]}
  # @conv {"APPEND(args x2)" => ["GET","SET"]}
  def MEMCACHED_APPEND(args)
    str = GET(args) + args.last.to_s
    args[args.length - 1] = str
    MEMCACHED_SET(args)
  end

  # @conv {"PREPEND(args x3)" => ["GET","SETEX"]}
  # @conv {"PREPEND(args x2)" => ["GET","SET"]}
  def MEMCACHED_PREPEND(args)
    str = GET(args)
    pos = args.length - 1
    args[pos] = args[pos].to_s + str
    MEMCACHED_SET(args)
  end

  # @conv {"CAS(args x4)" => ["SETEX"]}
  # @conv {"CAS(args x3)" => ["SET"]}
  def MEMCACHED_CAS(args)
    args.pop
    MEMCACHED_SET(args)
  end

  # @conv {"INCR" => ["INCRBY"]}
  def MEMCACHED_INCR(args)
    INCRBY(args)
  end

  # @conv {"DECR" => ["DECRBY"]}
  def MEMCACHED_DECR(args)
    DECRBY(args)
  end

  # @conv {"DELETE" => ["DEL"]}
  def MEMCACHED_DELETE(args)
    DEL(args)
  end

  # @conv {"FLUSH" => ["FLUSHALL"]}
  def MEMCACHED_FLUSH(args)
    FLUSHALL(args)
  end

  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand, args)
    result = {}
    ## PREPARE SPECIAL OPERATION
    if ["FLUSHALL"].include?(operand)
      result["operand"] = operand
      return result
    end
    ## PREPARE OPERATION & ARGS
    result["operand"] = "MEMCACHED_#{operand.upcase}"
    result["args"] = @parser.exec(operand.upcase, args)
    result
  end
end
