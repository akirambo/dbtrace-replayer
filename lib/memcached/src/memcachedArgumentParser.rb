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
  def initialize(logger, option)
    @logger = logger
    @option = option
    @utils = Utils.new
  end

  def exec(operand, args)
    send("prepare_args_#{operand}", args)
  end

  def structure_type(_operand, _args)
    "keyValue"
  end

  #################
  ## prepareArgs ##
  #################
  ## 1.SETTYPE :: SET, ADD, REPLACE, APEEND, PREPEND
  def prepare_args_settype(args)
    case @option[:inputFormat]
    when "basic" then
      ## CHECK ARGUMENTS
      ### 1.create
      value = @utils.create_numbervalue(args[5])
      ### 2. setup arguments considering EXPIRE_TIME
      if args[4] == "0"
        ## EXPIRE_TIME == 0
        return [args[2], value]
      else
        return [args[2], args[4], value]
      end
    when "binary" then
      args
    end
  end

  ## 2.GETTYPE :: GET, GETS, DELETE
  def prepare_args_gettype(args)
    case @option[:inputFormat]
    when "basic" then
      [args[2]]
    when "binary" then
      [args[0]]
    end
  end

  ## 3.CALCTYPE :: INCR, DECR
  def prepare_args_calctype(args)
    case @option[:inputFormat]
    when "basic" then
      [args[2], args[3]]
    when "binary" then
      args
    end
  end

  def prepare_args_set(args)
    prepare_args_settype(args)
  end

  def prepare_args_add(args)
    prepare_args_settype(args)
  end

  def prepare_args_replace(args)
    prepare_args_settype(args)
  end

  def prepare_args_append(args)
    prepare_args_settype(args)
  end

  def prepare_args_prepend(args)
    prepare_args_settype(args)
  end

  def prepare_args_get(args)
    prepare_args_gettype(args)
  end

  def prepare_args_gets(args)
    prepare_args_gettype(args)
  end

  def prepare_args_delete(args)
    prepare_args_gettype(args)
  end

  def prepare_args_cas(_args)
    @logger.warn("Unsupported CAS Operation.")
    nil
  end

  def prepare_args_incr(args)
    prepare_args_calctype(args)
  end

  def prepare_args_decr(args)
    prepare_args_calctype(args)
  end

  def prepare_args_flush(_args)
    ## do nothing
    []
  end
end
