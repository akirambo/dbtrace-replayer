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

require_relative "./utils"
require_relative "./metrics"

class AbstractRunner
  def initialize(dbname,logger, option)
    @logDBName = dbname.upcase
    ## REMOVE START
    @logger    = logger
    @options   = option
    ## REMOVE END
    @utils     = Utils.new()
    @monitorName = nil
    @metrics   = Metrics.new(logger,@options)
  end
  def exec(workload)
    init
    workload.each{|ope|
      ope.keys().each{|command|
        cmd = command.upcase().sub("_*","")
        @logger.debug( " -- #{cmd} --")
        operation(cmd, ope[command])
      }
    }
    if(@options[:async])then
      asyncExec()
    end
    @metrics.output()
    finish
  end
  def init
    @logger.warn("Database Init Function is NOT implemented.")
  end
  def refresh
    @logger.warn("Database Reset Function is NOT implemented.")
  end
  def finish
    @logger.warn("Database Finish Function is NOT implemented.")
  end
  def operation(operand, args)
    ## prepare
    begin 
      conv = send("prepare_#{@logDBName}",operand,args)
    rescue => e
      @logger.fatal("Crash @ prepare_#{@logDBName}")
      @logger.error(e.message)
      @logger.error(args)
    end
    ## run
    begin 
      @monitorName = operand.downcase()
      @metrics.start_monitor("database",@monitorName)
      @metrics.start_monitor("client",@monitorName)
      send(conv["operand"], conv["args"])
      @metrics.end_monitor("database",@monitorName)
      @metrics.end_monitor("client",@monitorName)
    rescue => e
      @logger.error("[#{operand}] Operation([#{@options[:sourceDB]}] TO [#{@options[:targetDB]}] is not supported @ #{__FILE__}")
      if(conv != nil and conv["operand"] != nil)then
        @logger.error("Operator :: #{conv["operand"]}")
      end
      @logger.error(e.message)
      @logger.error(args)
    end
  end
  def fatal(operand,args)
    @logger.fatal("Illegal Arguments @ #{operand} --> #{args}")
  end
  def parseJSON(data)
    @utils.parseJSON(data)
  end
  def convJSON(hash)
    @utils.convJSON(hash)
  end
  def createNumberValue(bytesize)
    @utils.createNumberValue(bytesize)
  end
  def createString(bytesize)
    @utils.createString(bytesize)
  end 
  def changeNumericWhenNumeric(input)
    @utils.changeNumericWhenNumeric(input)
  end
  #########################
  ## Performance Monitor ##
  #########################
  def monitor(type,targetQuery="all")
    @metrics.monitor(type, targetQuery)
  end
  def addDuration(duration,type,targetQuery="all")
    @metrics.addDuration(duration,type,targetQuery)
  end
  def addTotalDuration(duration,type)
    @metrics.addTotalDuration(duration,type)
  end
  def addCount(targetQuery="all")
    @metrics.addCount(targetQuery)
  end
  ###################
  # Async Execution #
  ###################
  def asyncExec()
    puts "Please Implement #{__method__}"
  end
end
