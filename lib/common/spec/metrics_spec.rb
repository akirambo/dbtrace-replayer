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


# -*- coding: utf-8 -*-


require_relative "../../../spec/spec_helper"
require_relative "../metrics"

RSpec.describe 'Unit Class TEST' do
  before (:each) do
    logger = Logger.new(STDOUT)
    logger.level = Logger::FATAL
    option = {:async     => true}
    @metrics = Metrics.new(logger,option)
  end
  context "output" do
    it "Case :: Async" do
      @metrics.start_monitor("test","query")
      @metrics.add_duration(0.4,"test","query")
      @metrics.end_monitor("test","query")
      @metrics.output()
    end
    it "Case :: Sync" do
      @metrics.instance_variable_set("@option",{:async =>false})
      @metrics.start_monitor("test","query")
      @metrics.add_duration(0.4,"test","query")
      @metrics.end_monitor("test","query")
      @metrics.output()
    end
  end
  context "start_monitor" do
    it "Case :: Simple" do
      @metrics.start_monitor("test","query")
      time = @metrics.instance_variable_get("@time")["test"]
      proc = @metrics.instance_variable_get("@processing")["test"]
      expect(time.keys()).to eq []
      expect(proc).to eq 0
    end
  end
  context "end_monitor" do
    it "Case :: Simple" do
      @metrics.start_monitor("test","query")
      @metrics.add_duration(0.4,"test","query")
      @metrics.end_monitor("test","query")
      time = @metrics.instance_variable_get("@time")["test"]["query"][0]
      proc = @metrics.instance_variable_get("@processing")["test"]
      expect(time).to eq 0.4
      expect(proc).to eq nil
    end
    it "Case :: Error" do
      @metrics.add_duration(0.4,"test","query")
      @metrics.end_monitor("test","query")
      time = @metrics.instance_variable_get("@time")["test"]["query"][0]
      proc = @metrics.instance_variable_get("@processing")["test"]
      expect(time).to eq 0.4
      expect(proc).to eq nil
    end
  end
  context "add_duration" do
    it "Case :: Simple" do
      ## #1
      @metrics.add_duration(0.1,"test","query")
      ans = {"test" => 0.1}
      expect(@metrics.instance_variable_get("@processing")).to eq ans
      ans = {"query" => 1}
      expect(@metrics.instance_variable_get("@queries_on_targetdb")).to eq ans
      ## #2
      @metrics.add_duration(0.1,"test","query")
      ans = {"test" => 0.2}
      expect(@metrics.instance_variable_get("@processing")).to eq ans
      ans = {"query" => 2}
      expect(@metrics.instance_variable_get("@queries_on_targetdb")).to eq ans
    end
  end
  context "add_count" do
    it "Case :: Simple" do
      ## #1
      @metrics.add_count("query")
      ans = {"query" => 1}
      expect(@metrics.instance_variable_get("@queries_on_targetdb")).to eq ans
      ## #2
      @metrics.add_count("query")
      ans = {"query" => 2}
      expect(@metrics.instance_variable_get("@queries_on_targetdb")).to eq ans
    end
  end
  context "add_total_duration" do
    it "Case :: Simple" do
      #1 
      @metrics.add_total_duration(0.1,"test")
      ans = {"test" => 0.1}
      expect(@metrics.instance_variable_get("@processing")).to eq ans
      #2
      @metrics.add_total_duration(0.1,"test")
      ans = {"test" => 0.2}
      expect(@metrics.instance_variable_get("@processing")).to eq ans
    end
  end
  context "monitor" do
    it "Case :: Simple" do
      ## START
      @metrics.monitor("test","query")
      ## END
      @metrics.monitor("test","query")
      ## Check
      val = @metrics.instance_variable_get("@processing")["test"]
      expect(val).to be > 0
      val = @metrics.instance_variable_get("@queries_on_targetdb")["query"]
      expect(val).to eq 1
    end
  end
end
  
