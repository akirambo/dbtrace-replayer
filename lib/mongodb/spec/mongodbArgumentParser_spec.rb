# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbArgumentParser"

RSpec.describe 'Mongodb Unit TEST' do
  before do
    @parser = MongdbArgumentParser.new(DummyLogger.new)
  end
  context " > Argument Parser" do
    it "INSERT (basic) " do
      log = '{ insert: "test", documents: [ { a: "S8cC1/kUY9" } ], writeConcern: { j: false } }'
      #skip "未実装"
    end
    it "INSERT (bulk)" do
      skip "未実装"
    end
    it "UPDATE" do
      skip "未実装"
    end
    it "COUNT" do
      skip "未実装"
    end
    it "GROUP" do
      skip "未実装"
    end
    it "FIND(simple)" do
      skip "未実装"
    end
    it "FIND(projection)" do
      skip "未実装"
    end
    it "FIND(filter)" do
      skip "未実装"
    end
    it "FIND(sort)" do
      skip "未実装"
    end
    it "DELETE" do
      skip "未実装"
    end
    it "AGGREGATE" do
      skip "未実装"
    end
    it "MAPREDUCE" do
      skip "未実装"
    end
  end
end
