# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/memcachedRunner"
require_relative "./mock"

module MemcachedRunnerUnitTest
  RSpec.describe 'Memcached Runner  Unit TEST' do
    context "Memcached Trace" do
      it "For Memcacehd Trace" do
        ## setup
        @logger = DummyLogger.new()
        @option = {
          :sourceDB => "MEMCACHED"
        }
        @runner = MemcachedRunner.new("MEMCACHED", @logger, @option)
      end
    end
  end
end

