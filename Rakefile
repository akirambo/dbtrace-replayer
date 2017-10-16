
###########
## SETUP ##
###########
require_relative "./lib/tools/runner"

desc "Run bundle install"
namespace :setup do
  task :bundle do
    sh "bundle install --path vender/bundle"
  end
  task :redis do
    sh "cd lib/redis/src/cxx; rake build"
  end
  task :memcached do
    sh "cd lib/memcached/src/cxx; rake build"
  end
  task :mongodb do
    sh "cd lib/mongodb/src/cxx; rake build"
  end
  task :cassandra do
    sh "cd lib/cassandra/src/cxx; rake build"
  end
end

desc "Install Packages"
namespace :install do
  task :cassandra do
    sh "cd lib/cassandra/src/cxx; rake init"
  end
  task :memcached do
    sh "cd lib/memcached/src/cxx; rake init"
  end
  task :mongodb do
    sh "cd lib/mongodb/src/cxx; rake init"
  end
  task :redis do
    sh "cd lib/redis/src/cxx; rake init"
  end
end

desc "Clean"
task :clean do
  sh "rm -rf vender output *.log *.query *.summary"
end

############
## RUNNER ##
############

#--------------------------------#
# Replay Trace On Which Database #
#--------------------------------#
desc "Replay Trace On Database (traceType,runType,trace,time,async,datamodel,schema,keyspace(only for Cassandra))"
task :run, [:traceType, :runType, :trace, :times, :async, :datamodel, :schema, :keyspace,:key_of_keyvalue]  do |env, args|
  include Runner
  exec(args, false)
end

####################
## ANALYZING TOOL ##
####################
desc "Make CSV from LOG Files"
task :log2csv, [:targetDir, :outfile] do |env, args|
  sh "bundle exec ruby ./bin/resultAnalysisMultiTimes.rb #{args[:targetDir]} #{args[:outfile]}"
end
desc "Check Code Format with Rubocop"
task :rubocop do
  sh "bundle exec rubocop"
end


#############
## CLEANER ##
#############
desc "Clean files " 
task :flush do
  sh "rm -f *.log *.csv *.png"
end

####################
## VISUALIZE TOOL ##
####################
desc "Visualize Coverage"
task :cov do
    sh "ruby spec/webrick.rb"
  end

desc "Visualize the result of replay"
task :plot do
  sh "ruby ./lib/tools/visualize/summarize.rb ./ "
  sh "gnuplot ./lib/tools/visualize/summarize.plt"
end

desc "Visualize the relationship between operations (Graphviz)"
task :graphVis do
  sh "bundle exec ruby bin/visualize.rb"
  Dir.glob("./output/*.rb").each{|exec|
    sh "bundle exec ruby #{exec}"
  }
  sh "mv ./*.dot ./output/."
  sh "mv ./*.png ./output/."
end

###############
## UNIT TEST ##
###############
namespace :unitTest do
  desc "Run All Unit Tests" 
  task :all do
    sh "bundle exec rspec lib/**/spec/*Unit_spec.rb lib/common/spec/"
  end
  desc "lib/common [utils,metircs]" 
  task :common do
    sh "bundle exec rspec lib/common/spec/"
  end
  desc "Redis Unit Test" 
  task :redis do
    sh "bundle exec rspec lib/redis/spec/*Unit_spec.rb"
  end
  desc "Mongodb Unit Test" 
  task :mongodb do
    sh "bundle exec rspec lib/mongodb/spec/*Unit_spec.rb"
  end
  desc "Memcached Unit Test"
  task :memcached do
    sh "bundle exec rspec lib/memcached/spec/*Unit_spec.rb"
  end
  desc "Cassandra Unit Test"
  task :cassandra do
    sh "bundle exec rspec lib/cassandra/spec/*Unit_spec.rb"
  end
  desc "Show Coverage http://IP_ADDRESS:8000"

  task :coverage do
    sh "bundle exec ruby bin/webPageForCoverage.rb"
  end
end

#####################
## TEST DATA MODEL ##
#####################
namespace :test do
  desc "Test All"
  task :all => [:redis,:memcached,:mongodb,:cassandra] do
    echo "Finish"
  end
  desc "Redis Test With Database"
  task :redis do
    sh "bundle exec rspec lib/redis/spec/redisOperation_spec.rb"
    sh "bundle exec rspec lib/redis/spec/redisOperationCxx_spec.rb"
    sh "bundle exec rspec lib/redis/spec/memcached2Redis_spec.rb"
    sh "bundle exec rspec lib/redis/spec/mongodb2Redis_spec.rb"
    sh "bundle exec rspec lib/redis/spec/cassandra2Redis_spec.rb"
  end

  desc "Memcached Test With Database"
  task :memcached => [:setup] do
    sh "bundle exec rspec lib/memcached/spec/memcachedOperationCxx_spec.rb"
    sh "bundle exec rspec lib/memcached/spec/memcachedOperation_spec.rb"
    sh "bundle exec rspec lib/memcached/spec/redis2Memcached_spec.rb"
    sh "bundle exec rspec lib/memcached/spec/mongodb2Memcached_spec.rb"
    sh "bundle exec rspec lib/memcached/spec/cassandra2Memcached_spec.rb"
  end

  desc "Mongodb Test With Database"
  task :mongodb => [:setup] do
    sh "bundle exec rspec lib/mongodb/spec/mongodbOperation_spec.rb"
    sh "bundle exec rspec lib/mongodb/spec/mongodbOperationCxx_spec.rb"
    sh "bundle exec rspec lib/mongodb/spec/redis2Mongodb_spec.rb"
    sh "bundle exec rspec lib/mongodb/spec/memcached2Mongodb_spec.rb"
    sh "bundle exec rspec lib/mongodb/spec/cassandra2Mongodb_spec.rb"
  end

  desc "Cassandra Test With Database"
  task :cassandra => [:setup] do
    sh "bundle exec rspec lib/cassandra/spec/cassandraOperation_spec.rb"
    sh "bundle exec rspec lib/cassandra/spec/cassandraOperationCxx_spec.rb"
    sh "bundle exec rspec lib/cassandra/spec/redis2Cassandra_spec.rb"
    sh "bundle exec rspec lib/cassandra/spec/memcached2Cassandra_spec.rb"
    sh "bundle exec rspec lib/cassandra/spec/mongodb2Cassandra_spec.rb"
  end
end



#######################################
## TEST REDIS RUN MODE (with docker) ##
#######################################
desc "TEST for Redis"
task :test_redis => [
  :test_redis_e2e,
  :test_redis_runner,
  :test_memcached2redis,
  :test_mongodb2redis
] do
end

desc "End to End TEST for redis"
task :test_redis_e2e => [:setup] do
  sh "bundle exec rspec lib/redis/spec/redis_run_spec.rb "
end

desc "UNIT TEST for Redis Runner"
task :test_redis_runner => [:setup] do
  sh "bundle exec rspec lib/redis/spec/redis_runner_spec.rb"
end

desc "UNIT TEST for Memcached to Redis Converter"
task :test_memcached2redis => [:setup] do
  sh "bundle exec rspec lib/redis/spec/redis_memcached_converter_spec.rb"
end

desc "UNIT TEST for MongoDB to Redis Converter"
task :test_mongodb2redis => [:setup] do
  sh "bundle exec rspec lib/redis/spec/redis_mongodb_converter_spec.rb"
end

######################################

desc "TEST for memcached"
task :test_memcached => [:setup] do
  sh "bundle exec rspec lib/memcached/spec/memcached_run_spec.rb"
end

desc "TEST for mongodb"
task :test_mongodb => [:setup] do
  sh "bundle exec rspec lib/mongodb/spec/mongodb_spec.rb"
end

desc "TEST for cassandra"
task :test_cassandra => [:setup] do
  sh "bundle exec rspec lib/cassandra/spec/cassandra_spec.rb"
end

desc "TEST for hbase10"
task :test_hbase10 => [:setup] do
  sh "bundle exec rspec lib/hbase10/spec/hbase10_spec.rb"
end

###########################
## TEST INTEGRATION TEST ##
###########################
desc "INTEGRATION TEST for all" 
task :int_test => [:int_memcached,:int_mongodb,:int_redis,:int_cassandra] do
  sh "echo 'Run All Integration Test' "
end

desc "INTEGRATION TEST for memcached" 
task :int_memcached do
  ## Redis     TO Memcached
  sh "bundle exec ruby ./bin/parser.rb redis -m run  -t memcached -l DEBUG  lib/redis/spec/input/redis_all_command.log"
  ## Memcached(binary_protocol) TO Memcached
  sh "bundle exec ruby ./bin/parser.rb memcached -m run  -t memcached -i binary -l DEBUG lib/memcached/spec/input/memcached_all_command_binary_protocol.log"
  ## CQL       TO Memcached
  sh "bundle exec ruby ./bin/parser.rb cassandra -m run  -t memcached -i cql3 -l DEBUG  lib/redis/spec/input/cql3.log --schema lib/redis/spec/input/cql3.schema"
  ## Mongodb   TO Memcached
  sh "bundle exec ruby ./bin/parser.rb mongodb -m run  -t memcached -l DEBUG  lib/redis/spec/input/mongodb_all_command.log"
end

desc "INTEGRATION TEST for mongodb"
task :int_mongodb do
  ## Mongodb                     TO Mongodb
  sh  "bundle exec ruby ./bin/parser.rb mongodb -m run  -t mongodb -i basic -l DEBUG lib/mongodb/spec/input/all_command.log"
  ## Memcached (binary_protocol) TO Mongodb
  sh "bundle exec ruby ./bin/parser.rb memcached -m run  -t mongodb -i binary -l DEBUG lib/memcached/spec/input/memcached_all_command_binary_protocol.log"
  ## CQL                         TO Mongodb
  sh "bundle exec ruby ./bin/parser.rb cassandra -m run  -t mongodb -i cql3 -l DEBUG lib/redis/spec/input/cql3.log --schema lib/redis/spec/input/cql3.schema"
  ## Mongodb                     TO Mongodb
  sh "bundle exec ruby ./bin/parser.rb mongodb -m run  -t mongodb -i basic -l DEBUG lib/mongodb/spec/input/all_command.log"
end

desc "INTEGRATION TEST for redis"
task :int_redis do
  ## Redis TO Redis
  sh "bundle exec ruby ./bin/parser.rb redis -m run  -t redis -i basic -l DEBUG lib/redis/spec/input/redis_all_command.log"
  ## Memcached(binary_protocol) TO Redis
  sh "bundle exec ruby ./bin/parser.rb memcached -m run  -t redis -i binary -l DEBUG lib/memcached/spec/input/memcached_all_command_binary_protocol.log"
  ## Memcached(basic) TO Redis
  sh "bundle exec ruby ./bin/parser.rb memcached -i basic -m run  -t redis -l DEBUG  lib/memcached/spec/input/memcached_all_command.log"
  ## Mongodb TO Redis
  sh "bundle exec ruby ./bin/parser.rb  mongodb -m run  -t redis -l DEBUG  lib/redis/spec/input/mongodb_all_command.log"
  ## CQL tO Redis
  sh "bundle exec ruby ./bin/parser.rb cassandra -m run  -t redis -i cql3 -l DEBUG lib/redis/spec/input/cql3.log --schema lib/redis/spec/input/cql3.schema" 
  
end

desc "INTEGRATION TEST for cassandra"
task :int_cassandra do
  ## CQL   TO CQL
  sh 'bundle exec ruby ./bin/parser.rb cassandra -m run  -t cassandra -i cql3 -l DEBUG --keyspace "testdb" lib/redis/spec/input/cql3.log --schema lib/redis/spec/input/cql3.schema'
  ## Redis TO CQL
  sh 'bundle exec ruby ./bin/parser.rb redis -m run  -t cassandra  -l DEBUG --keyspace "testdb" --schema lib/redis/spec/input/redis_all_command.schema lib/redis/spec/input/redis_all_command.log'
  ## Memcached(binary_protocol) TO CQL
  sh 'bundle exec ruby ./bin/parser.rb memcached -m run -i binary -t cassandra  -l DEBUG --keyspace "testdb" lib/memcached/spec/input/memcached_all_command_binary_protocol.log --schema lib/memcached/spec/input/memcached_all_command.schema'
  ## Mongodb TO CQL
  sh 'bundle exec ruby ./bin/parser.rb mongodb -m run  -t cassandra  -l DEBUG --keyspace "testdb" --schema lib/mongodb/spec/input/mongodb_all_command.schema lib/mongodb/spec/input/mongodb_all_command.log'
end

####################
## BENCHMARK TEST ##
####################
desc "BENCHNARK x100 TEST for rdis" 
task :bench100_test => [:setup] do
  ## Redis TO Redis
  system("bundle exec ruby ./bin/parser.rb redis -m run  -t redis -i basic -b -n 100 lib/redis/spec/input/redis_all_command.log")
  ## Memcached(binary_protocol) TO Redis
  system("bundle exec ruby ./bin/parser.rb memcached -m run  -t redis -i binary -b -n 100 lib/memcached/spec/input/memcached_all_command_binary_protocol.log")
  ## Memcached(basic) TO Redis
  system("bundle exec ruby ./bin/parser.rb memcached -i basic -m run  -t redis -b -n 100 lib/memcached/spec/input/memcached_all_command.log")
  ## Mongodb TO Redis
  system("bundle exec ruby ./bin/parser.rb  mongodb -m run  -t redis -b -n 100 lib/redis/spec/input/mongodb_all_command.log")
  ## CQL tO Redis
  system("bundle exec ruby ./bin/parser.rb cassandra -m run  -t redis -i cql -b -n 100 lib/redis/spec/input/cql_all_command.log")
end


desc "BENCHNARK TEST for all" 
task :bench_test => [:bench_memcached,:bench_mongodb,:bench_redis,:bench_cassandra] do
  system ("echo 'Run All Integration Test' ")
end

desc "BENCHMARK TEST for memcached" 
task :bench_memcached => [:setup] do
  ## Memcached(binary_protocol) TO Memcached
  system("bundle exec ruby ./bin/parser.rb memcached -m run  -t memcached -i binary -b lib/memcached/spec/input/memcached_all_command_binary_protocol.log")
  ## Redis     TO Memcached
  system("bundle exec ruby ./bin/parser.rb redis -m run  -t memcached -b lib/redis/spec/input/redis_all_command.log")
  ## CQL       TO Memcached
  system("bundle exec ruby ./bin/parser.rb cassandra -m run  -t memcached -i cql -b lib/redis/spec/input/cql_all_command.log")
  ## Mongodb   TO Memcached
  system("bundle exec ruby ./bin/parser.rb mongodb -m run  -t memcached -b  lib/redis/spec/input/mongodb_all_command.log")
end

desc "BENCHMARK TEST for mongodb"
task :bench_mongodb => [:setup] do
  ## Mongodb                     TO Mongodb
  system("bundle exec ruby ./bin/parser.rb mongodb -m run  -t mongodb -i basic -b lib/mongodb/spec/input/all_command.log")
  ## Memcached (binary_protocol) TO Mongodb
  system("bundle exec ruby ./bin/parser.rb memcached -m run  -t mongodb -i binary -b lib/memcached/spec/input/memcached_all_command_binary_protocol.log")
  ## CQL                         TO Mongodb
  system("bundle exec ruby ./bin/parser.rb cassandra -m run  -t mongodb -i cql -b lib/redis/spec/input/cql_all_command.log")
  ## Redis                     TO Mongodb
  system("bundle exec ruby ./bin/parser.rb redis -m run  -t mongodb -i basic -b lib/redis/spec/input/redis_all_command.log")
end

desc "BENCHMARK TEST for redis"
task :bench_redis => [:setup] do
  ## Redis TO Redis
  system("bundle exec ruby ./bin/parser.rb redis -m run  -t redis -i basic -b lib/redis/spec/input/redis_all_command.log")
  ## Memcached(binary_protocol) TO Redis
  system("bundle exec ruby ./bin/parser.rb memcached -m run  -t redis -i binary -b lib/memcached/spec/input/memcached_all_command_binary_protocol.log")
  ## Memcached(basic) TO Redis
  system("bundle exec ruby ./bin/parser.rb memcached -i basic -m run  -t redis -b  lib/memcached/spec/input/memcached_all_command.log")
  ## Mongodb TO Redis
  system("bundle exec ruby ./bin/parser.rb  mongodb -m run  -t redis -b  lib/redis/spec/input/mongodb_all_command.log")
  ## CQL tO Redis
  system("bundle exec ruby ./bin/parser.rb cassandra -m run  -t redis -i cql -b lib/redis/spec/input/cql_all_command.log")
  
end

desc "BENCHMARK TEST for cassandra"
task :bench_cassandra => [:setup] do
  ## CQL TO CQL
  system('bundle exec ruby ./bin/parser.rb cassandra -m run  -t cassandra -i cql -b --keyspace "testdb" --schema lib/redis/spec/input/cql_all_command.schema lib/redis/spec/input/cql_all_command.log')
  ## Redis TO CQL
  system('bundle exec ruby ./bin/parser.rb redis -m run  -t cassandra  -b --keyspace "testdb" --schema lib/redis/spec/input/redis_all_command.schema lib/redis/spec/input/redis_all_command.log')
  ## Memcached(binary_protocol) TO CQL
  system('bundle exec ruby ./bin/parser.rb memcached -m run -i binary -t cassandra  -b --keyspace "testdb" lib/memcached/spec/input/memcached_all_command_binary_protocol.log --schema lib/memcached/spec/input/memcached_all_command.schema')
  ## Mongodb TO CQL
  system('bundle exec ruby ./bin/parser.rb mongodb -m run  -t cassandra  -b --keyspace "testdb" --schema lib/mongodb/spec/input/mongodb_all_command.schema lib/mongodb/spec/input/mongodb_all_command.log')
end


####################
## TEST YCSB MODE ##
####################
=begin
desc "Test YCSB MODE [All]"
task :test_ycsb => [:test_ycsb_redis, :test_ycsb_memcached, :test_ycsb_mongodb, :test_ycsb_cassandra]

desc "Test YCSB MODE for Redis"
task :test_ycsb_redis => [:setup] do
  system("bundle exec rspec lib/redis/spec/redis_ycsb_spec.rb")
end

desc "Test YCSB MODE for memcached"
task :test_ycsb_memcached => [:setup] do
  system("bundle exec rspec lib/memcached/spec/memcached_ycsb_spec.rb")
end

desc "Test YCSB MODE for mongodb"
task :test_ycsb_mongodb => [:setup] do
  system("bundle exec rspec lib/mongodb/spec/mongodb_ycsb_spec.rb")
end

desc "Test YCSB for cassandra"
task :test_ycsb_cassandra => [:setup] do
  system("bundle exec rspec lib/cassandra/spec/cassandra_ycsb_spec.rb")
end
=end

