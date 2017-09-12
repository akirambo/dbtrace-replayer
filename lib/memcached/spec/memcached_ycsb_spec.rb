
require_relative "../../../spec/spec_helper"

RSpec.describe 'Memcached TEST [YCSB MODE]' do
  ## Common
  inputDir    = "lib/memcached/spec/input/"
  expectedDir = "lib/memcached/spec/expected/"
  option = [ "-a original", "-a primitive"]
  testSet = {}

  testNamePrefix = "Memcached YCSB Workload "
  workloads = ["memcached_all_command"]
  addedTestSet = buildTestSet(testNamePrefix,workloads,option,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)
  
  ## YCSB Workload Set
  testNamePrefix = "Memcached YCSB Workload "
  workloads = [
    "ycsb_load_workloada","ycsb_load_workloadb",
    "ycsb_load_workloadc","ycsb_load_workloadd",
    "ycsb_run_workloada","ycsb_run_workloadb",
    "ycsb_run_workloadc","ycsb_run_workloadd"
  ]
  addedTestSet = buildTestSet(testNamePrefix,workloads,option,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)
  testSet.each{|name, config|
    it name do
      exec("memcached",config,"ycsb") 
    end
  }
end

