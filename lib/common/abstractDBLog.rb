
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

class AbstractDBLog
  def initialize(command2basic, option, logger)
    @operation_count = 0
    @basic_metrics = {}
    @commands = []
    @command2basic = command2basic
    @basic_commands = []
    @option = option
    @logger = logger
    setup
  end

  def push(hash)
    @commands.push(hash)
  end

  def ycsb_format
    ## Calculation
    case @option[:analysisMode]
    when "original" then
      original_operation_count
    when "primitive" then
      primitive_operation_count
    else
      @logger.error("Unsupported ANALYSIS MODE #{@option[:analysisMode]}")
    end
    calc_total_operation_count
    ## Update
    case @option[:ycsbOutputFormat]
    when "basic" then
      ### Only For Updated Parameters
      result = {}
      result["operationcount"] = @operation_count
      %w[read update scan insert].each do |type|
        result["#{type}proportion"] = proportion(type)
      end
    when "full" then
      @logger.error("Not Implemented :: #{@option[:ycsbOutputFormat]}")
    else
      @logger.error("Unsupported OUTPUT FORMAT #{@option[:ycsbOutputFormat]}")
    end
    result
  end

  def proportion(type)
    calc_proportion(type)
  end

  def log
    @commands
  end

  protected

  ## Initialize
  def setup
    @command2basic.each do |_, value|
      unless @basic_commands.include?(value)
        @basic_commands.push(value)
      end
    end
    @basic_commands.each do |command|
      @basic_metrics[command] = 0
    end
  end

  ## Primitive Operation Analysis
  def primitive_operation_count
    multi_access_command = @primitive_operation_for_multidata.keys
    @commands.each do |command_with_args|
      command_with_args.each do |command, args|
        if @command2basic[command].is_a?(Array) && multi_access_command.include?(command)
          @command2basic[command].each do |com|
            @basic_metrics[com] +=
              calc_primitiveoperation_for_multidata(command, args, com)
          end
        elsif @command2basic[command].is_a?(Array) && !multi_access_command.include?(command)
          @command2basic[command].each do |com|
            @basic_metrics[com] += 1
          end
        elsif multi_access_command.include?(command)
          @basic_metrics[@command2basic[command]] +=
            calc_primitiveoperation_for_multidata(command, args)
        else
          @basic_metrics[@command2basic[command]] += 1
        end
      end
    end
  end

  def calc_primitiveoperation_for_multidata(command, args, com = nil)
    operation_count = 0
    unless @primitive_operation_for_multidata.key?(command)
      @logger.error("Unsupported Multi Command #{command}")
      abort
    end
    if @primitive_operation_for_multidata[command]["operation"] == "range"
      first_place = args[@primitive_operation_for_multidata[command]["arg0"]].to_i
      end_place = args[@primitive_operation_for_multidata[command]["arg1"]].to_i
      operation_count = end_place + 1 - first_place
    elsif @primitive_operation_for_multidata[command]["operation"] == "fromArgument"
      operation_count = args[@primitive_operation_for_multidata[command]["arg"]].to_i
    else
      keyvalue_count = args.size
      keyvalue_count -= @primitive_operation_for_multidata[command]["prefixCount"]
      keyvalue_count -= @primitive_operation_for_multidata[command]["postfixCount"]
      operation_count = if com
                          keyvalue_count / @primitive_operation_for_multidata[command]["argNumEachPrimitiveCommand"][com].to_i
                        else
                          keyvalue_count / @primitive_operation_for_multidata[command]["argNumEachPrimitiveCommand"]
                        end
    end
    operation_count
  end

  ## Database Original Operation Analysis
  def original_operation_count
    @commands.each do |command_with_args|
      command_with_args.each do |command__, _|
        command = @command2basic[command__]
        @basic_metrics[command] += 1
      end
    end
  end

  def calc_total_operation_count
    @operation_count = 0
    @basic_metrics.each do |_, value|
      @operation_count += value.to_i
    end
  end

  def calc_proportion(type)
    if @basic_metrics.include?(type.upcase)
      result = @basic_metrics[type.upcase].to_f / @operation_count
      if result.nan?
        return 0.0
      end
      result
    else
      total = 0.0
      if type == "all"
        @basic_metrics.each do |key, value|
          if @basic_commands.include?(key)
            total += value.to_f
          end
        end
        return total / @operation_count
      end
      total
    end
  end
end
