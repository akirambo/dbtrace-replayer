
require_relative "../../../spec/spec_helper"

RSpec.describe 'Mongodb TEST [YCSB MODE]' do
    ## Common
  inputDir    = "lib/mongodb/spec/input/"
  expectedDir = "lib/mongodb/spec/expected/"
  option = [ "-a original", "-a primitive"]
  testSet = {}

  ## MongoDB Basic Query
  testNamePrefix = "MongoDB Basic Query " 
  workloads = [ "basic_query","mongodb_all_command"]
  addedTestSet = buildTestSet(testNamePrefix,workloads,option,inputDir,expectedDir)
  testSet = testSet.merge(addedTestSet)
  
  ## YCSB Workload Set
  testNamePrefix = "MongoDB YCSB Workload "
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
      exec("mongodb",config,"ycsb") 
    end
  }
end

