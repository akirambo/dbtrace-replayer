# -*- coding: utf-8 -*-

require_relative "../../../spec/spec_helper"
require_relative "../src/mongodbParser"

RSpec.describe 'Mongodb Unit TEST' do
  before do
    filename = ""
    option = {}
    logger = DummyLogger.new
    @parser = MongodbParser.new(filename,option,logger)
  end
  context "Parser" do
    it "parser(D COMMAND)" do
      trace = "2016-12-08T16:12:23.170+0000 D COMMAND  [conn1] run command test.$cmd { insert: \"test\", documents: [ { _id: ObjectId('58498667f462a8a9eadb73ff'), name: \"BB\", age: 20.0, gender: \"m\", group: \"B\", ratings: [ 1.0, 2.0, 3.0, 4.0 ] } ], ordered: true }"
      ans =  {"insert"=>" \"test.test\", documents: [ { _id: ObjectId('58498667f462a8a9eadb73ff'), name: \"BB\", age: 20.0, gender: \"m\", group: \"B\", ratings: [ 1.0, 2.0, 3.0, 4.0 ] } ], ordered: true }"}
      expect(@parser.parse(trace)).to eq ans
    end
    it "parser(D COMMAND + skip)" do
      trace = '2016-12-08T16:12:21.125+0000 D COMMAND  [TTLMonitor] BackgroundJob starting: TTLMonitor'
      expect(@parser.parse(trace)).to eq nil
    end
    it "parser(D COMMAND + skip)" do
      trace = "2016-12-08T16:12:23.170+0000 D COMMAND  [conn1] run command test.$cmd { error: \"test\", documents: [ { _id: ObjectId('58498667f462a8a9eadb73ff'), name: \"BB\", age: 20.0, gender: \"m\", group: \"B\", ratings: [ 1.0, 2.0, 3.0, 4.0 ] } ], ordered: true }"
      expect(@parser.parse(trace)).to eq nil
    end
  end
end
