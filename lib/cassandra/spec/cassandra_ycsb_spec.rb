
require_relative "../../../spec/spec_helper"

RSpec.describe 'Cassandra TEST [YCSB MODE]' do
  ## Common
  inputDir    = "lib/cassandra/spec/input/"
  expectedDir = "lib/cassandra/spec/expected/"
  option = [ "-a original", "-a primitive"]
  testSet = {}
  
  ## YCSB Workload Set
  testNamePrefix = "Cassandra YCSB Workload "
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
      exec("cassandra",config,"ycsb") 
    end
  }
end

