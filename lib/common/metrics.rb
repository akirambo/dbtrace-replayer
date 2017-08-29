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

class Metrics
  def initialize(logger,options)
    @logger = logger
    @options = options
    ## type => { query => []}
    @time  = {}
    ## queryType => count
    @query = {}
    @processing = {}
    @timeTrace = []
    @queriesOnTargetDB = {}
    @status = "end"
  end
  def output()
    @logger.info("=========== Metrics Output ===========")
    @logger.info("#{@options[:sourceDB]} => #{@options[:targetDB]}")
    if(@options[:async])then
      @logger.info(" API :: ASYNC MODE")
    else
      @logger.info(" API ::  SYNC MODE")
    end
    totalTime = 0.0
    totalCount = {"all" => 0.0}
    
    buf = []
    buf1 = []
    
    buf1.push("-- Metrics Detail --")
    @queriesOnTargetDB.each{|query,count|
      if(totalCount[query] == nil)then
        totalCount[query] = 0
      end
      totalCount[query] += count
      totalCount["all"] += count
      buf1.push("\t#{query} #{count}")
    }
    
    
    buf.push("-- GENERATED QUERY --")      
    @time.each{|proc,queryTime|
      #@logger.info("Time @ #{proc}")
      queryTime.each{|query, times|
        if(times.size > 0 and times.inject(:+) > 0)then
          buf.push("QUERY : #{query} @#{proc}")
          buf.push(" TOTAL[count] #{times.size}")
          buf.push(" TOTAL[sec]   #{times.inject(:+)}")
          buf.push(" AVG  [sec]   #{times.inject(:+)/times.size}")
          buf.push(" MAX  [sec]   #{times.max}")
          buf.push(" MIN  [sec]   #{times.min}")
          totalTime  += times.inject(:+)
        end
      }
    }
    
    @logger.info("-- TOTAL METRICS --")
    @logger.info("TOTAL LATENCY[sec] #{totalTime}")
    @logger.info("TOTAL OPERATIONS   #{totalCount["all"].to_i}")
    @logger.info("THROUGHPUT[Operations/sec] #{totalCount["all"].to_f/totalTime.to_f}")
    buf1.each{|line|
      @logger.info(line)
    }
    buf.each{|line|
      @logger.info(line)
    }
    @logger.info("======================================")
    ## reset
    reset
  end
  def start_monitor(type,querytype)
    if(!@time[type])then
      @time[type] = {}
    end
    @processing[type] = 0
    query(querytype)
  end
  def end_monitor(type,querytype)
    if(!@time[type])then
      @time[type] = {}
    end
    if(!@time[type][querytype])then
      @time[type][querytype] = []
    end
    if(@processing[type])then
      @time[type][querytype].push(@processing[type])
      @processing[type] = nil
    end
  end
  def addDuration(duration,type,targetQuery)
    if(@processing[type] == nil)then
      @processing[type] = 0
    end
    @processing[type] += duration
    if(!@queriesOnTargetDB[targetQuery])then
      @queriesOnTargetDB[targetQuery] = 0
    end
    @queriesOnTargetDB[targetQuery] += 1
  end
  def addCount(targetQuery)
    if(!@queriesOnTargetDB[targetQuery])then
      @queriesOnTargetDB[targetQuery] = 0
    end
    @queriesOnTargetDB[targetQuery] += 1
  end
  def addTotalDuration(duration,type)
    if(@processing[type] == nil)then
      @processing[type] = 0
    end
    @processing[type] += duration
  end

  def monitor(type,targetQuery)
    case @status 
    when "end" then
      ## "START Timer"
      @status = "start"
      @startTime = Time.now
    when "start" then
      ## "END Timer"
      @endTime = Time.now
      duration = @endTime - @startTime
      if(@processing[type] == nil)then
        @processing[type] = 0
      end
      @processing[type] += duration
      if(!@queriesOnTargetDB[targetQuery])then
        @queriesOnTargetDB[targetQuery] = 0
      end
      @queriesOnTargetDB[targetQuery] += 1
      @status = "end"
    end
  end
  private
  def reset
    @time = {}
    @query = {}
    @queriesOnTargetDB = {}
    @processing = {}
    @timeTrace = []
    @status = "end"
  end

  ###############
  ## For Query ##
  ###############
  def query(query)
    if(@query[query] == nil)then
      @query[query] = 0
    end
    @query[query] += 1
  end
end
