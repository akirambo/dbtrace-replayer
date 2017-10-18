
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

module RedisUtils
  def prepare_redis(operand, args)
    result = {}
    result["operand"] = "redis_#{operand}"
    result["args"] = if %w[zunionstore zinterstore].include?(operand)
                       @parser.extract_z_x_store_args(args)
                     elsif %w[mset mget msetnx].include?(operand)
                       @parser.args2hash(args)
                     elsif %w[hmget].include?(operand)
                       @parser.args2key_args(args)
                     elsif %w[hmset].include?(operand)
                       @parser.args2key_hash(args)
                     else
                       args
                     end
    result
  end

  def sorted_array_get_range(start_index, end_index, members)
    result = []
    i = start_index
    if i == -1
      i = 0
    end
    if end_index == -1
      end_index = members.size - 1
    end
    while i <= end_index
      result.push(members[i])
      i += 1
    end
    result
  end

  def aggregate_score(operation, v0, v1, weight)
    score = 0
    unless operation.nil?
      score = case operation.upcase
              when "SUM" then
                v0 + v1 * weight
              when "MAX" then
                [v0, v1 * weight].max
              when "MIN" then
                [v0, v1 * weight].min
              else
                0
              end
    end
    score
  end
end
