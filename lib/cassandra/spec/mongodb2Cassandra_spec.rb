# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/cassandraRunner"

RSpec.describe 'MongoDB TO Cassandra Unit Test' do
  before do
    @logger = DummyLogger.new
    @options = {
      :keyspace     => "testdb",
      :columnfamily => "mongodb",
      :schemaFile => "#{File.dirname(__FILE__)}/input/testSchema.schema",
      :sourceDB => "mongodb",
      :api => "ruby"
    }
    @runner = CassandraRunner.new("mongodb", @logger,@options)
  end
  context 'Document Test' do
    before(:each) do
      @runner.send("DIRECT_EXECUTER","TRUNCATE #{@options[:keyspace]}.#{@options[:columnfamily]};")    
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},false]])).to eq true
      ans = {"mongoid"=>["key00"], "value"=> ["val00"]}
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {}})).to eq ans 
    end
    it "MONGODB_INSERT(bulk)/MONGODB_FIND(simple)" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},true]])).to eq true
      ans = {"mongoid"=>["key00"], "value"=> ["val00"]}
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {}})).to eq ans 
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(filter)" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},false]])).to eq true
      ans = {"mongoid"=>["key00"], "value"=> ["val00"]}
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}", 
            "filter" => {"_id" => "key00"}})).to eq ans 
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(projection)" do
      skip "未実装"
    end
    it "MONGODB_INSERT(simple)/MONGODB_FIND(sort)" do
      skip "未実装"
    end
    it "MONGODB_UPDATE" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_UPDATE",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "query" => {"_id" =>"key00"},
            "update" => {"$set" => { "value" => "valXX"}},
            "multi" => true})).to eq true
        
      ans = {"mongoid"=>["key00"], "value"=> ["valXX"]}
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {"_id" => "key00"}})).to eq ans 
    end
    it "MONGODB_DELETE" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key01","value"=>"val01"},false]])).to eq true
      expect(@runner.send("MONGODB_DELETE",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}", 
            "filter" => {"_id" => "key00"}})).to eq true
      ans = {}
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {"_id" => "key00"}})).to eq ans
      expect(@runner.send("MONGODB_DELETE",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {}})).to eq true
      expect(@runner.send("MONGODB_FIND",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {"_id" => "key01"}})).to eq ans
    end
    it "MONGODB_COUNT" do
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key00","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_INSERT",
          [["#{@options[:keyspace]}.#{@options[:columnfamily]}",
              {"_id"=>"key01","value"=>"val00"},false]])).to eq true
      expect(@runner.send("MONGODB_COUNT",
          {"key" => "#{@options[:keyspace]}.#{@options[:columnfamily]}",
            "filter" => {"_id" => "key00"}})).to eq 1
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
