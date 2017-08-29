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


class MemcachedArgumentParser
  def initialize(logger, options)
    @logger = logger
    @options = options
    @utils   = Utils.new
  end
  def exec(operand,args)
    return send("prepareArgs_#{operand}", args)
  end
  def structureType(operand,args)
    return "keyValue"
  end
  #################
  ## prepareArgs ##
  #################
  ## 1.SETTYPE :: SET, ADD, REPLACE, APEEND, PREPEND
  def prepareArgs_SETTYPE(args)
    case @options[:inputFormat] 
    when "basic" then
      ## CHECK ARGUMENTS
      ### 1.create
      value = @utils.createNumberValue(args[5])
      ### 2. setup arguments considering EXPIRE_TIME
      if(args[4] == "0")then
        ## EXPIRE_TIME == 0
        return [args[2],value]
      else
        return [args[2],args[4],value]
      end
    when "binary" then
      return args
    end
  end
  ## 2.GETTYPE :: GET, GETS, DELETE
  def prepareArgs_GETTYPE(args)
    case @options[:inputFormat]
    when "basic" then
      return [args[2]]
    when "binary" then
      return [args[0]]
    end
  end
  ## 3.CALCTYPE :: INCR, DECR
  def prepareArgs_CALCTYPE(args)
    case @options[:inputFormat]
    when "basic" then
      return [args[2],args[3]]
    when "binary" then
      return args
    end
  end
  def prepareArgs_SET(args)
    return prepareArgs_SETTYPE(args)
  end
  def prepareArgs_ADD(args)
    prepareArgs_SETTYPE(args)
  end
  def prepareArgs_REPLACE(args)
    prepareArgs_SETTYPE(args)
  end
  def prepareArgs_APPEND(args)
    prepareArgs_SETTYPE(args)
  end
  def prepareArgs_PREPEND(args)
    prepareArgs_SETTYPE(args)
  end

  def prepareArgs_GET(args)
    prepareArgs_GETTYPE(args)
  end
  def prepareArgs_GETS(args)
    prepareArgs_GETTYPE(args)
  end
  def prepareArgs_DELETE(args)
    prepareArgs_GETTYPE(args)
  end

  def prepareArgs_CAS(args)
    @logger.warn("Unsupported CAS Operation.")
    return nil
  end
  def prepareArgs_INCR(args)
    prepareArgs_CALCTYPE(args)
  end
  def prepareArgs_DECR(args)
    prepareArgs_CALCTYPE(args)
  end
  def prepareArgs_FLUSH(args)
    ## do nothing
    return []
  end

end
