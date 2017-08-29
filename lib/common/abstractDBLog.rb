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
    @operationCount = 0
    @basicMetrics = {}
    @commands         = []
    @command2basic = command2basic
    @basicCommands = []
    @option = option
    @logger = logger
    setup()
  end
  def push(hash)
    @commands.push(hash)
  end
  def ycsbFormat
    ## Calculation
    case @option[:analysisMode]
    when "original" then
      originalOperationCount()
    when "primitive" then
      primitiveOperationCount()
    else
      @logger.error("Unsupported ANALYSIS MODE #{@option[:analysisMode]}")
    end
    calcTotalOperationCount()

    ## Update 
    case @option[:ycsbOutputFormat]
    when "basic" then
      ### Only For Updated Parameters
      result = {}
      result["operationcount"] = @operationCount
      ["read","update","scan","insert"].each{|type|
        result["#{type}proportion"] = proportion(type)
      }
    when "full" then
      @logger.error("Not Implemented :: #{@option[:ycsbOutputFormat]}")
    else
      @logger.error("Unsupported OUTPUT FORMAT #{@option[:ycsbOutputFormat]}")
    end
    return result
  end
  def proportion(type)
    calcProportion(type)
  end
  def log
    @commands
  end
protected
  ## Initialize
  def setup
    @command2basic.each{|key,value|
      if(!@basicCommands.include?(value))then
        @basicCommands.push(value)
      end
    }
    @basicCommands.each{|command|
      @basicMetrics[command] = 0
    }
  end

  ## Primitive Operation Analysis
  def primitiveOperationCount
    multiAccessCommand = @primitiveOperationForMultiData.keys()
    @commands.each{|commandWithArgs|
      commandWithArgs.each{|command, args|
        if(multiAccessCommand.include?(command))then
          ## Multi Accesses In One Operation
          if(@command2basic[command].kind_of?(Array))then
            @command2basic[command].each{|com|
              @basicMetrics[com] += 
              calcPrimitiveOperationForMultiData(command, args, com)
            }
          else
            @basicMetrics[@command2basic[command]] += 
              calcPrimitiveOperationForMultiData(command, args)
          end
        else
          ## Single Access In Oon Operation 
          if(@command2basic[command].kind_of?(Array))then
            @command2basic[command].each{|com|
              @basicMetrics[com] += 1
            }
          else
            @basicMetrics[@command2basic[command]] += 1
          end
        end
      }
    }
  end
  def calcPrimitiveOperationForMultiData(command, args, com=nil)
    operationCount = 0
    if(@primitiveOperationForMultiData.key?(command))then
      if(@primitiveOperationForMultiData[command]["operation"] == "range")then
        firstPlace =  args[@primitiveOperationForMultiData[command]["arg0"]].to_i
        endPlace =  args[@primitiveOperationForMultiData[command]["arg1"]].to_i
        operationCount = endPlace + 1 - firstPlace
      elsif(@primitiveOperationForMultiData[command]["operation"] == "fromArgument")then
        operationCount = args[@primitiveOperationForMultiData[command]["arg"]].to_i
      else
        keyValueCount  = args.size 
        keyValueCount -=  @primitiveOperationForMultiData[command]["prefixCount"] 
        keyValueCount -=  @primitiveOperationForMultiData[command]["postfixCount"]
        if(com)then
          operationCount = 
            keyValueCount / @primitiveOperationForMultiData[command]["argNumEachPrimitiveCommand"][com].to_i
        else
 operationCount = 
            keyValueCount / @primitiveOperationForMultiData[command]["argNumEachPrimitiveCommand"]
        end
      end
    else
      @logger.error("Unsupported Multi Command #{command}")
      abort()
    end
    return operationCount
  end
  
  ## Database Original Operation Analysis
  def originalOperationCount
    @commands.each{|commandWithArgs|
      commandWithArgs.each{|command__, args|
        command = @command2basic[command__]
        @basicMetrics[command] += 1
      }
    }
  end
  def calcTotalOperationCount()
    @operationCount = 0
    @basicMetrics.each{|command, value|
      @operationCount += value.to_i
    }
  end
  def calcProportion(type)
    if(@basicMetrics.include?(type.upcase()))then
      result = @basicMetrics[type.upcase()].to_f / @operationCount
      if(result.nan?)then
        return 0.0
      end      
      return result
    else
      if(type == "all")then
        total = 0.0
        @basicMetrics.each{|key, value|
          if(@basicCommands.include?(key))then
            total += value.to_f
          end
        }
        return total / @operationCount
      else
        return 0.0
      end
    end
  end
end
