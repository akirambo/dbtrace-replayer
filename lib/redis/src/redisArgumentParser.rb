
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

class RedisArgumentParser
  def initialize(logger)
    @logger = logger
  end

  ## supported method
  def extract_z_x_store_args(args)
    result = {}
    result["args"] = []
    ## example is ["dst0_sum" "2" "set" "set2" "WEIGHTS" "2.0" "1.0" "AGGREGATE" "sum"]
    result["key"] = args.shift ## shift dst_key
    keynumber = args.shift.to_i ## shift keynumber

    ## Extract KEYS [arg_index = 1 is represents KEY_NUMBER]
    keynumber.times do
      result["args"].push(args.shift)
    end
    result["option"] = {}
    until args.empty?
      option_name = args.shift.downcase
      if option_name == "weights"
        ## Extract wheights option
        result["option"][:weights] = []
        keynumber.times do
          result["option"][:weights].push(args.shift)
        end
      elsif option_name == "aggregate"
        ## Extract aggregate option
        result["option"][:aggregate] = args.shift
      else
        @logger.error("[ERROR] :: unsupported option Name #{option_name}")
      end
    end
    result
  end

  def args2hash(args)
    hash = {}
    args.each_index do |index|
      if index.odd?
        hash[args[index - 1]] = args[index]
      end
    end
    hash
  end

  def args2key_args(args)
    result = {}
    result["key"] = args.shift
    result["args"] = args
    result
  end

  def args2key_hash(args)
    result = {}
    result["key"] = args.shift
    result["args"] = args2hash(args)
    result
  end
end
