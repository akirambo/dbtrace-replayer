# -*- coding: utf-8 -*-

require "mongo"

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbRunner"
require_relative "../../common/utils"

RSpec.describe 'MongodbOperation Unit TEST (Ruby API)' do
  before do
    @logger = DummyLogger.new
    @option = {
      :api => "ruby"
    }
    @option[:sourceDB] = "mongodb"
    @runner = MongodbRunner.new("mongodb",@logger,@option)
  end
  context " > Mongodb Operation" do
    before (:each) do
      @runner.send("DROP",[])
    end
    it "INSERT (empty)" do
      expect(@runner.send("INSERT", [])).to eq true
    end
    it "INSERT (single)" do
      doc = {"val" => 'a', "_id" => 'key00'}
      expect(@runner.send("INSERT", [["test",doc]])).to eq true
    end
    it "INSERT (multi) " do
      doc00 = {:val => 'a', :_id => 'key00'}
      doc01 = {:val => 'a', :_id => 'key01'}
      expect(@runner.send("INSERT", [["test",doc00],["test",doc01]])).to eq 2
    end
    it "UPDATE (single)" do
      ## Before
      doc00 = {"val" => 'a0', "_id" => "key00"}
      doc01 = {"val" => 'a0', "_id" => "key01"}
      doc02 = {"val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      ## Exec
      cond = {
        "key"    => "test",
        "multi"  => false,
        "query"  => {"val" =>  'a0'},
        "update" => {'$set' => {"val" => 'b'}}
      }
      expect(@runner.send("UPDATE", cond)).to eq true
    end
    it "UPDATE (multi)" do
      ## Before
      doc00 = {"val" => 'a0', "_id" => "key00"}
      doc01 = {"val" => 'a0', "_id" => "key01"}
      doc02 = {"val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      ## Exec
      cond = {
        "key"    => "test",
        "multi"  => true,
        "query"  => {"val" =>  'a0'},
        "update" => {'$set' => {"val" => "b"}}
      }
      expect(@runner.send("UPDATE", cond)).to eq true
    end
    it "FIND (basic)" do
      ## Before
      doc00 = {"val" => 'a0', "_id" => "key00"}
      doc01 = {"val" => 'a0', "_id" => "key01"}
      doc02 = {"val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      cond = {
        "key"  => "test",
        "sort" => nil,
        "projection" => nil,
        "filter" => {"_id" => "key01"}
      }
      expect(@runner.send("FIND", cond, false)).to eq [{"_id"=>"key01", "val"=>"a0"}]
    end
    it "FIND (sort)" do
      ## Before
      doc00 = {"num" => 0, "val" => 'a0', "_id" => "key00"}
      doc01 = {"num" => 1, "val" => 'a0', "_id" => "key01"}
      doc02 = {"num" => 2, "val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      cond = {
        "key"  => "test",
        "sort" => {"num" => 1},
        "projection" => nil,
        "filter" => {"val" => "a0"}
      }
      expect(@runner.send("FIND", cond, false)).to eq [{"_id"=>"key00", "num"=>0, "val"=>"a0"}, {"_id"=>"key01", "num"=>1, "val"=>"a0"}]
    end
    it "FIND (projection)" do
      ## Before
      doc00 = {"number" => 10, "val" => 'a0', "_id" => "key00"}
      doc01 = {"number" => 20, "val" => 'a0', "_id" => "key01"}
      @runner.send("INSERT", [["test",doc00],["test",doc01]])
      cond = {
        "key"  => "test",
        "sort" => nil,
        "projection" => {"val" => 1},
        "filter" => nil
      }
      expect(@runner.send("FIND", cond, false)).to eq [{"_id"=>"key00", "val"=>"a0"},{"_id"=>"key01", "val"=>"a0"}]
    end
    it "FIND (sort && projection)" do
      ## Before
      doc00 = {"val" => 'a0', "_id" => "key00"}
      doc01 = {"val" => 'a0', "_id" => "key01"}
      doc02 = {"val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      cond = {
        "key"  => "test",
        "sort" => nil,
        "projection" => {"val" => 1},
        "filter" => {"val" => "a0"}
      }
      expect(@runner.send("FIND", cond, false)).to eq [{"_id"=>"key00", "val"=>"a0"}, {"_id"=>"key01", "val"=>"a0"}]
    end
    it "DELETE" do
      ## Before
      doc00 = {"val" => 'a0', "_id" => "key00"}
      doc01 = {"val" => 'a0', "_id" => "key01"}
      doc02 = {"val" => 'a1', "_id" => "key02"}
      @runner.send("INSERT", [["test",doc00],["test",doc01],["test",doc02]])
      cond = {
        "key"  => "test",
        "filter" => {"val" => 'a0'}
      }
      expect(@runner.send("DELETE", cond)).to eq 2
      end
    it "AGGEREGATE" do
      skip "未実装"
    end
    it "GROUP" do
      skip "未実装"
    end
    it "MAPREDUCE" do
      skip "未実装"
    end
  end
end

