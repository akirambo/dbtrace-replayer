# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbArgumentParser"

RSpec.describe 'Mongodb Unit TEST' do
  before do
    @parser = MongodbArgumentParser.new(DummyLogger.new)
  end
  context " Argument Parser" do
    it "structure_type" do
      expect(@parser.send(:structure_type,"operand",[])).to eq "others"
    end
    it "parseLog (basic) " do
      log = ' "a", "insert" : "test", "documents" : [{"b":"A"}]  }'
      ans = {"key"=>"a", "insert"=>"test", "documents"=>[{"b"=>"A"}]}
      expect(@parser.send(:parse_log,log)).to eq ans
    end
    it "parseLog (error) " do
      log = '{ insert: "test", documents: [ { a: "S8cC1/kUY9" } ], writeConcern: { j: false } }'
      expect(@parser.send(:parse_log,log)).to eq nil
    end
    it "INSERT (basic)" do
      args = ' "db", "insert" : "test", "documents" : [{"b":"A"}]  }'
      ans = ["db", [{"b"=>"A"}], false]
      result = @parser.exec("insert",args)[0]
      expect(result[0]).to eq "db"
      expect(result[1]).to match_array [{"b"=>"A"}]
      expect(result[2]).to eq false
    end
    it "INSERT (bulk #1)" do
      args = ' "db", "insert" : "test", "documents" : 100  }'
      result = @parser.exec("insert",args)
      expect(result[0][0]).to eq "db"
      expect(result.size).to eq 100
      expect(result[0][2]).to eq true
    end
    it "INSERT (bulk #2)" do
      args = ' "db", "insert" : "test", "documents" : 100  }'
      result = @parser.exec("insert",args,true)
      expect(result[0][0]).to eq "db"
      expect(result.size).to eq 100
      expect(result[0][2]).to eq true
    end
    it "UPSERT (bulk #2)" do
      args = ' "db", "insert" : "test", "documents" : 100  }'
      result = @parser.exec("upsert",args,true)
      expect(result[0][0]).to eq "db"
      expect(result.size).to eq 100
      expect(result[0][2]).to eq true
    end
    it "UPDATE (basic)" do
      args = ' "db", updates: [{"q":"Q","u":"U","multi":"M","upsert":"U"}]'
      ans = {"key"=>"db", "query"=>"Q", "update"=>"U", "multi"=>"M", "upsert"=>"U"}
      expect(@parser.exec("update",args)).to eq ans
    end
    it "UPDATE (error)" do
      args = ' "db", "updates: [{"b":"A"a}] }'
      expect(@parser.exec("update",args)).to eq nil
    end
    it "COUNT" do
      args = ' "test", query: { group: "A" }, fields: {} }'
      ans = {"key" => "test", "fields" => {}, "query"=>{"group"=>"A"}}
      expect(@parser.exec("count",args)).to eq ans
    end
    it "GROUP" do
      args = ' group { group: { key: { group: 1.0 }'
      expect(@parser.exec("group",args)).to eq nil
    end
    it "FIND (basic)" do
      args = ' "test", filter: { group: "X" } }'
      ans = {"key"=>"test", "filter"=>{"group"=>"X"}}
      expect(@parser.exec("find",args)).to eq ans
    end
    it "DELETE" do
      args = ' "test", deletes: [ { q: {}, limit: 0.0 } ], ordered: true }'
      ans = {"key"=>"test", "filter"=>{}}
      expect(@parser.exec("delete",args)).to eq ans
    end
    it "AGGREGATE(match,group)" do
      args = ' "test", pipeline: [ { $match: { group: "X" } }, { $group: { _id: "$name", total: { $sum: "$age" } } } ] }'
      ans = {"key"=>"test",
             "group"=>"{\"_id\":\"$name\",\"total\":{\"$sum\":\"$age\"}}",
             "match" => "{\"group\":\"X\"}",
             "unwind" => nil}
      expect(@parser.exec("aggregate",args)).to eq ans
    end
    it "AGGREGATE(match,group,unwind)" do
      args = ' "test", pipeline: [ { $match: { group: "X" } }, { $unwind: "aaa"} ,{ $group: { _id: "$name", total: { $sum: "$age" } } } ] }'
      ans = {"key"=>"test",
             "group"=>"{\"_id\":\"$name\",\"total\":{\"$sum\":\"$age\"}}",
             "match" => "{\"group\":\"X\"}",
             "unwind" => "{\"path\": \"aaa\"}"}
      expect(@parser.exec("aggregate",args)).to eq ans
    end
    it "MAPREDUDE" do
      expect(@parser.send(:mapreduce,nil,nil).class).to eq Hash
    end
  end
end
