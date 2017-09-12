
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
  def initialize(logger, option)
    @logger = logger
    @option = option
    ## type => { query => []}
    @time  = {}
    ## queryType => count
    @query = {}
    @processing = {}
    @time_trace = []
    @queries_on_targetdb = {}
    @status = "end"
  end

  def output
    @logger.info("=========== Metrics Output ===========")
    @logger.info("#{@option[:sourceDB]} => #{@option[:targetDB]}")
    if @option[:async]
      @logger.info(" API :: ASYNC MODE")
    else
      @logger.info(" API ::  SYNC MODE")
    end
    total_time = 0.0
    total_count = { "all" => 0.0 }
    buf = []
    buf1 = ["-- Metrics Detail --"]
    @queries_on_targetdb.each do |query, count|
      if total_count[query].nil?
        total_count[query] = 0
      end
      total_count[query] += count
      total_count["all"] += count
      buf1.push("\t#{query} #{count}")
    end
    buf.push("-- GENERATED QUERY --")
    @time.each do |proc, querytime|
      # @logger.info("Time @ #{proc}")
      querytime.each do |query, times|
        if !times.empty? && times.inject(:+) > 0
          buf.push("QUERY : #{query} @#{proc}")
          buf.push(" TOTAL[count] #{times.size}")
          buf.push(" TOTAL[sec]   #{times.inject(:+)}")
          buf.push(" AVG  [sec]   #{times.inject(:+) / times.size}")
          buf.push(" MAX  [sec]   #{times.max}")
          buf.push(" MIN  [sec]   #{times.min}")
          total_time += times.inject(:+)
        end
      end
    end
    @logger.info("-- TOTAL METRICS --")
    @logger.info("TOTAL LATENCY[sec] #{total_time}")
    @logger.info("TOTAL OPERATIONS   #{total_count["all"].to_i}")
    @logger.info("THROUGHPUT[Operations/sec] #{total_count["all"].to_f / total_time.to_f}")
    buf1.each do |line|
      @logger.info(line)
    end
    buf.each do |line|
      @logger.info(line)
    end
    @logger.info("======================================")
    ## reset
    reset
  end

  def start_monitor(type, querytype)
    if @time[type].nil?
      @time[type] = {}
    end
    @processing[type] = 0
    query(querytype)
  end

  def end_monitor(type, querytype)
    if @time[type].nil?
      @time[type] = {}
    end
    if @time[type][querytype].nil?
      @time[type][querytype] = []
    end
    if @processing[type]
      @time[type][querytype].push(@processing[type])
      @processing[type] = nil
    end
  end

  def add_duration(duration, type, targetquery)
    if @processing[type].nil?
      @processing[type] = 0
    end
    @processing[type] += duration
    if @queries_on_targetdb[targetquery].nil?
      @queries_on_targetdb[targetquery] = 0
    end
    @queries_on_targetdb[targetquery] += 1
  end

  def add_count(targetquery)
    if @queries_on_targetdb[targetquery].nil?
      @queries_on_targetdb[targetquery] = 0
    end
    @queries_on_targetdb[targetquery] += 1
  end

  def add_total_duration(duration, type)
    if @processing[type].nil?
      @processing[type] = 0
    end
    @processing[type] += duration
  end

  def monitor(type, targetquery)
    case @status
    when "end" then
      ## "START Timer"
      @status = "start"
      @start_time = Time.now
    when "start" then
      ## "END Timer"
      @end_time = Time.now
      duration = @end_time - @start_time
      if @processing[type].nil?
        @processing[type] = 0
      end
      @processing[type] += duration
      if @queries_on_targetdb[targetquery].nil?
        @queries_on_targetdb[targetquery] = 0
      end
      @queries_on_targetdb[targetquery] += 1
      @status = "end"
    end
  end

  private

  def reset
    @time = {}
    @query = {}
    @queries_on_targetdb = {}
    @processing = {}
    @time_trace = []
    @status = "end"
  end

  ###############
  ## For Query ##
  ###############
  def query(query)
    if @query[query].nil?
      @query[query] = 0
    end
    @query[query] += 1
  end
end
