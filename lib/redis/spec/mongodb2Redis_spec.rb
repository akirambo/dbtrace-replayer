# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/redisRunner"

RSpec.describe 'Mongodb TO Redis Unit Test' do
  before do
    @logger = DummyLogger.new
    @option = {
      :sourceDB => "mongodb",
      :collection    => "collection"
    }
    @runner = RedisRunner.new("mongodb", @logger,@option)
  end
  context 'Document(One value) Test' do
    before(:each) do
      @option[:datamodel] = "KEYVALUE"
      @runner.send("FLUSHALL",[])
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],
              {"_id"=>"key00","value"=>"val00"},true]])).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {"_id"=>"key00"}})).to eq "val00"
    end
    it "MONGODB_INSERT(bulk)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00"},true],
            [@option[:collection],{"_id"=>"key01","value"=>"val01"},true]])).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {"_id"=>"key00"}})).to eq "val00"
      
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {"_id"=>"key01"}})).to eq "val01"
      
    end
    it "MONGODB_UPDATE" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_UPDATE",
          {"key" => @option[:collection],
            "query" => {"_id" =>"key00"},
            "update" => {"$set" => { "value" => "valXX"}},
            "multi" => true})).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq "valXX"
    end
    it "MONGODB_DELETE" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key01","value"=>"val01"},false]])).to eq true
      expect(@runner.send("MONGODB_DELETE",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq nil
      expect(@runner.send("MONGODB_DELETE",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key01"}})).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key01"}})).to eq nil
    end
    it "MONGODB_COUNT" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key01","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_COUNT",
          {"key" => @option[:collection],
            "filter" => {"_id" => "key00"}})).to eq 1
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(projection)" do
      skip "未実装"
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(sort)" do
      skip "未実装"
    end
    it "MONGODB_FINDANDMODIFY" do
      skip "未実装"
    end
    it "MONGODB_QUERY" do
      skip "未実装"
    end
    it "MONGODB_GROUP" do
      skip "未実装"
    end
    it "MONGODB_AGGREGATE" do
      skip "未実装"
    end
    it "MONGODB_MAPREDUCE" do
      skip "未実装"
    end
  end
  context 'Document(multi values) Test' do
    before(:each) do
      @option[:datamodel] = "DOCUMENT"
      @runner.send("FLUSHALL",[])
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],
              {"_id"=>"key00","value0"=>"val00", "value1"=>"val01"},true]])).to eq true
      ans = [{"_id"=>"key00", "value0"=>"val00", "value1"=>"val01"}] 
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {}})).to eq ans 
    end
    it "MONGODB_INSERT(bulk)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00", "value1"=>"val10"},true],
            [@option[:collection],{"_id"=>"key01","value"=>"val01", "value1"=>"val11"},true]])).to eq true
      ans = [
        {"_id"=>"key00", "value"=>"val00","value1"=>"val10"},
        {"_id"=>"key01", "value"=>"val01","value1"=>"val11"}
      ] 
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {}})).to eq ans 
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(filter)" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],
              {"_id"=>"key00","value"=>"val00","value1"=>"val10"},true]])).to eq true
      ans = [{"_id"=>"key00", "value"=>"val00","value1"=>"val10"}] 
      expect(@runner.send("MONGODB_FIND",
          {"key"     => @option[:collection],
            "filter" => {"_id" => "key00"}})).to eq ans 
    end
    it "MONGODB_UPDATE" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00","value1"=>"val10"},false]])).to eq true
      expect(@runner.send("MONGODB_UPDATE",
          {"key" => @option[:collection],
            "query" => {"_id" =>"key00"},
            "update" => {"$set" => { "value" => "valXX"}},
            "multi" => true})).to eq true
      ans = [{"_id"=>"key00", "value"=> "valXX","value1"=>"val10"}]
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq ans 
    end
    it "MONGODB_DELETE" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00","value1"=>"val10"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key01","value"=>"val01","value1"=>"val10"},false]])).to eq true
      expect(@runner.send("MONGODB_DELETE",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq true
      ans = []
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key00"}})).to eq ans
      expect(@runner.send("MONGODB_DELETE",
          {"key" => @option[:collection], 
            "filter" => {}})).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key" => @option[:collection], 
            "filter" => {"_id" => "key01"}})).to eq ans
    end
    it "MONGODB_COUNT" do
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key00","value"=>"val00","value1"=>"val10"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [[@option[:collection],{"_id"=>"key01","value"=>"val00","value1"=>"val10"},false]])).to eq true
      expect(@runner.send("MONGODB_COUNT",
          {"key" => @option[:collection],
            "filter" => {"_id" => "key00"}})).to eq 1
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(projection)" do
      skip "未実装"
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(sort)" do
      skip "未実装"
    end
    it "MONGODB_FINDANDMODIFY" do
      skip "未実装"
    end
    it "MONGODB_QUERY" do
      skip "未実装"
    end
    it "MONGODB_GROUP" do
      skip "未実装"
    end
    it "MONGODB_AGGREGATE" do
      skip "未実装"
    end
    it "MONGODB_MAPREDUCE" do
      skip "未実装"
    end
  end
end
