
require_relative "../../../spec/spec_helper"
require_relative "../src/redisParser"

RSpec.describe 'Redis Parser Unit Test' do
  before(:each) do
    logger = DummyLogger.new()
    dir = File.dirname(__FILE__)
    filename = "#{dir}/input/redis_all_command.log"
    option = {:mode => "run"}
    @tester = RedisParser.new(filename,option,logger)
  end
  context 'Parse Method' do
    it 'Case #1' do
      ans = {"SET" => ["A","B","C"]}
      expect(@tester.parse("A \"SET\" \"A\" \"B\" \"C\"")).to eq ans
    end
    it 'Case #2 (error)' do
      expect(@tester.parse("A \"ER ")).to eq nil
    end
    it 'Case #3 (error)' do
      expect(@tester.parse("A COMMAND")).to eq nil
    end
  end
end
