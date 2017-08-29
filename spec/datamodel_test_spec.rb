
require_relative "./spec_helper"

RSpec.describe 'Data Model TEST ' do
  describe "Key-Value" do
    tests = {
      "redis"     =>  {"SET" => 5000, "GET" => 5000},
      "memcached" =>  {"SET" => 5000, "GET" => 5000},
      "mongodb"   =>  {"INSERT" => 5000, "FIND" => 5000},
      "cassandra" =>  {"INSERT" => 5000, "SELECT" => 5000}
    }
    tests.each{|db,expected|
      describe "> Replay Traces on #{db.capitalize}" do
        ## Check Exec Query Counter
        config = {
          :testSet => ["redis","memcached","mongodb","cassandra"],
          :traceSet => [
            "spec/input/datamodel/keyvalue_redis.trace",
            "spec/input/datamodel/keyvalue_memcached.trace",
            "spec/input/datamodel/keyvalue_mongodb.trace",
            "spec/input/datamodel/keyvalue_cassandra.trace"
          ],
          :schema    => "spec/input/datamodel/keyvalue_test.schema",
          :expected  => expected,
          :datamodel => "KEYVALUE",
          :runDB  => db,
          :clean  => false
        }
        replayMultiTracesOnDB(config)
      end
    }
  end

  describe "Array (redis)" do
  end

  describe "Sorted Array (redis)" do
  end

  describe "Hash (redis)" do
  end

  describe "List (redis)" do
  end

  describe "Doc (mongodb)" do
  end

  describe "Table (Cassandra)" do
  end

end
