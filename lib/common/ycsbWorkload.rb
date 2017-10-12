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

class YCSBWorkload
  def initialize(log)
    @config = nil
    @log = log
    init_config
    updates(@log.ycsb_format)
  end

  def updates(hash)
    if check
      hash.each do |key, value|
        @config[key] = value
      end
    end
  end

  def exec
    @primitive_configs.each do |config|
      puts "#{config}=#{@config[config]}"
    end
  end

  private

  def init_config
    ## Keys
    @primitive_configs = %w[workload recordcount operationcount readallfields readproportion updateproportion scanproportion insertproportion requestdistribution].freeze
    ## Unsupported HTrace
    @config = {
      "workload" => "com.yahoo.ycsb.workloads.CoreWorkload",
      "recordcount" => 1000,
      "operationcount" => 3_000_000,
      "insertcount" => nil,
      "insertstart" => 0,
      "fieldcount" => 10,
      "fieldlength" => 100,
      "readallfields" => true,
      "writeallfields" => false,
      ## uniform,zipfian
      "fieldlengthdistribution" => "constant",
      "readproportion" => 0.95,
      "updateproportion" => 0.05,
      "insertproportion" => 0.0,
      "readmorifywriteproportion" => 0.0,
      "scanproportion" => 0.0,
      "mascanlength" => 1000,
      ## zipfian
      "scanlengthdistribution" => "uniform",
      ## ordered
      "insertorder" => "hashed",
      # requestdistribution=uniform or latest
      "requestdistribution" => "zipfian",
      "hotspotdatafraction" => 0.2,
      "hotspotopnfraction" => 0.8,
      "maxexecutiontime" => nil,
      "table" => "usertable",
      "columnfamily" => nil,
      # measurementtype=timeseries or raw
      "measurementtype" => "histogram",
      "measurement.raw.output_file" => nil,
      "measurement.trackjvm" => false,
      "histogram.buckets" => 1000,
      "timeseries.granularity" => 1000,
      "reportlatencyforeacherror" => false,
      "latencytrackederrors" => [],
      "core_workload_insertion_retry_limit" => 0,
      ## integer
      "core_workload_insertion_retry_interval" => nil,
    }
  end

  def check
    if @log.proportion("all") != 1.0
      p "[ERROR]:: Total Propartion is not 1.0 :: #{@log.proportion("all")}"
      return false
    end
    true
  end
end
