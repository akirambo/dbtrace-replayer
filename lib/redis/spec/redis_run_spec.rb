
require_relative "../../../spec/spec_helper"

RSpec.describe 'Redis TEST [RUN MODE]' do
  ## Common
  inputDir    = "lib/redis/spec/input/"
  expectedDir = "lib/redis/spec/expected/"
  inputFiles  = ["redis_all_command"]
  options     = [" -T redis -l DEBUG "]
=begin

  testSets    = buildTestSet("Redis Run Test ", inputFiles, options,inputDir,expectedDir)
  testSets.each{|name, config|
    it name do
      exec("redis", config, "run")
    end
  }
  inputFiles = ["memcached_all_command"]
  testSets    = buildTestSet("Memcached Run Test ", inputFiles, options,inputDir,expectedDir)
  testSets.each{|name, config|
    it name do
      ## arg 4 represents randomValue or not
      exec("memcached", config, "run", true)
    end
  }
=end
  inputFiles = ["mongodb_all_command"]
  testSets    = buildTestSet("MongoDB Run Test ", inputFiles, options,inputDir,expectedDir)
  testSets.each{|name, config|
    it name do
      ## arg 4 represents randomValue or not
      exec("mongodb", config, "run", true)
    end
  }

end
