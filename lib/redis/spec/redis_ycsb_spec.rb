
require_relative "../../../spec/spec_helper"

RSpec.describe 'Redis TEST [YCSB MODE]' do
  ## Common
  inputDir    = "lib/redis/spec/input/"
  expectedDir = "lib/redis/spec/expected/"
  options = [ "-a original", "-a primitive"]
  testSet = {}

  ## Redis Benchmark Set
  testNamePrefix = "Redis Benckmark Set "
  workloads = [ "redis_benchmarkset"]
  addedTestSet = buildTestSet(testNamePrefix,workloads,options,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)

  ## Redis Full Command Set
  testNamePrefix = "Redis Full Command Set "
  workloads = [ "redis_all_command"]
  addedTestSet = buildTestSet(testNamePrefix,workloads,options,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)
  
  ## YCSB Workload Set
  testNamePrefix = "Redis YCSB Workload "
  workloads = [
    "ycsb_load_workloada","ycsb_load_workloadb",
    "ycsb_load_workloadc","ycsb_load_workloadd",
    "ycsb_run_workloada","ycsb_run_workloadb",
    "ycsb_run_workloadc","ycsb_run_workloadd"
  ]
  addedTestSet = buildTestSet(testNamePrefix,workloads,options,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)
  testSet.each{|name, config|
    it name do
      exec("redis",config,"ycsb") 
    end
  }
end
