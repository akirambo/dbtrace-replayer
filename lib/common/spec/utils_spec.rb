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
require_relative "../utils"

RSpec.describe 'Unit Class TEST' do
  before (:all) do
    @utils = Utils.new()
  end
  context "createString" do
    it "64byte String" do
      val = @utils.createString(64)
      expect(val.size).to be >= 64
    end
  end
  context "createNumberValue" do
    it "64byte Number" do
      val = @utils.createNumberValue(10)
      expect(val.size).to be <= 64
    end
  end
  context "addDoubleQuotation Test" do 
    it "Case :: Single-Layer Document" do
      hash = {"a" => "b", "c" => 100}
      ans = "{\"a\":\"b\",\"c\":100}"
      expect(@utils.addDoubleQuotation(hash)).to eq ans
    end
    it "Case ::Multi-Layer Document" do
      hash = {"a" => "b", "c" => 100, "d" => {"e" => "f", "g" => 100}}
      ans = "{\"a\":\"b\",\"c\":100,\"d\":{\"e\":\"f\",\"g\":100}}"
      expect(@utils.addDoubleQuotation(hash)).to eq ans
    end
    it "Case :: Return {}" do
      hash = {}
      expect(@utils.addDoubleQuotation(hash)).to eq "{}"
    end
    it "Case :: ':' is included in values" do
      hash = {"a" => "b:c"}
      expect(@utils.addDoubleQuotation(hash)).to eq hash.to_json
    end
  end
  context "stringHash2symbolHash" do
    it "Case :: Single-Layer Hash" do
      stringHash = {"a" => "b"}
      symbolHash = {:a => "b"}
      expect(@utils.stringHash2symbolHash(stringHash)).to eq symbolHash
    end
    it "Case :: Multi-Layer Hash" do
      stringHash = {"a" => {"b" => "c"}}
      symbolHash = {:a => {:b => "c"}}
      expect(@utils.stringHash2symbolHash(stringHash)).to eq symbolHash
    end
    it "Case :: Single-Layer Hash Array" do
      stringHash = [{"a" => "b"},{"c" => "d"}]
      symbolHash = [{:a => "b"},{:c => "d"}]
      expect(@utils.stringHash2symbolHash(stringHash)).to eq symbolHash
    end
    it "Case :: Multi-Layer Hash Array" do
      stringHash = [{"a0" => {"b0" => "c0"}},{"a1" => {"b1" => "c1"}}]
      symbolHash = [{:a0 => {:b0 => "c0"}},{:a1 => {:b1 => "c1"}}]
      expect(@utils.stringHash2symbolHash(stringHash)).to eq symbolHash
    end
  end
  context "symbolHash2stringHash" do
    it "Case :: Single-Layer Hash" do
      stringHash = {"a" => "b"}
      symbolHash = {:a => "b"}
      expect(@utils.symbolHash2stringHash(symbolHash)).to eq stringHash
    end
    it "Case :: Multi-Layer Hash" do
      stringHash = {"a" => {"b" => "c"}}
      symbolHash = {:a => {:b => "c"}}
      expect(@utils.symbolHash2stringHash(symbolHash)).to eq stringHash
    end
    it "Case :: Single-Layer Hash Array" do
      stringHash = [{"a0" => "b0"},{"a1" => "b1"}]
      symbolHash = [{:a0 => "b0"},{:a1 => "b1"}]
      expect(@utils.symbolHash2stringHash(symbolHash)).to eq stringHash
    end
    it "Case :: Multi-Layer Hash Array" do
      stringHash = [{"a0" => {"b0" => "c0"}},{"a1" => {"b1" => "c1"}}]
      symbolHash = [{:a0 => {:b0 => "c0"}},{:a1 => {:b1 => "c1"}}]
      expect(@utils.symbolHash2stringHash(symbolHash)).to eq stringHash
    end
  end
  context "changeNumericWhenNumeric" do
    it "Case :: Input Number (simple)" do
      value = @utils.changeNumericWhenNumeric("1324")
      if (value.class == Integer or value.class == Fixnum)then
        expect(1).to eq 1
      else
        expect(1).to eq 0
      end
    end
    it "Case :: Input Number (+)" do
      value = @utils.changeNumericWhenNumeric("+1324")
      if (value.class == Integer or value.class == Fixnum)then
        expect(1).to eq 1
      else
        expect(1).to eq 0
      end
    end
    it "Case :: Input Number (-)" do
      value = @utils.changeNumericWhenNumeric("-1324")
      if (value.class == Integer or value.class == Fixnum)then
        expect(1).to eq 1
      else
        expect(1).to eq 0
      end
    end
    it "Case :: Input Big Number " do
      value = @utils.changeNumericWhenNumeric("2147483649")
      expect(value.class).to match String
    end
    it "Case :: Input String " do
      value = @utils.changeNumericWhenNumeric("a+1324")
      expect(value.class).to match String
    end
  end
  context "parseJSON" do
    it "Case :: Simple #1 " do
      str = "{\"a\":\"b\"}"
      hash = {"a" => "b"}
      expect(@utils.parseJSON(str)).to eq hash
    end
    it "Case :: Simple #2" do
      str = "{a:\"b\"}"
      hash = {"a" => "b"}
      expect(@utils.parseJSON(str)).to eq hash
    end
    it "Case :: Hash" do
      hash = {"a" => "b"}
      expect(@utils.parseJSON(hash)).to eq hash
    end
    it "Case :: Array" do
      array = [{"a" => "b"}]
      expect(@utils.parseJSON(array)).to eq array
    end
  end
  context "convJSON" do
    it "Case :: Simple #1" do
      input = {"a" => "b"}
      ans   = "{ a:\"b\"}"
      expect(@utils.convJSON(input)).to eq ans
    end
    it "Case :: Simple #2" do
      input = {:a => "b"}
      ans   = "{ a:\"b\"}"
      expect(@utils.convJSON(input)).to eq ans
    end
    it "Case :: Simple #3" do
      input = {:a => 1,:c =>"newVal"}
      ans   = "{ a:1, c:\"newVal\"}"
      expect(@utils.convJSON(input)).to eq ans
    end
  end
end
  
