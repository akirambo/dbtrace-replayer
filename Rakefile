
###########
## SETUP ##
###########
require_relative "./lib/tools/runner"

desc "Install Packages"
task :install do
  sh "cd lib/cassandra/src/cxx; rake init"
  sh "cd lib/memcached/src/cxx; rake init"
  sh "cd lib/mongodb/src/cxx; rake init"
  sh "cd lib/redis/src/cxx; rake init"
end

desc "Bundle install"
task :bundle do
  sh "bundle install --path vender/bundle"
end

desc "Build Cxx Drivers"
task :build do
  sh "cd lib/redis/src/cxx; rake build"
  sh "cd lib/memcached/src/cxx; rake build"
  sh "cd lib/mongodb/src/cxx; rake build"
  sh "cd lib/cassandra/src/cxx; rake build"
end

#############
## CLEANER ##
#############
desc "Remove files " 
task :clean do
  sh "rm -f *.log *.csv *.png"
end
desc "Distclean"
task :distclean do
  sh "rm -rf vender output *.log *.query *.summary *.csv *.png"
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
desc "Run All Unit Tests" 
task :unit_test do
  sh "bundle exec rspec lib/**/spec/*Unit_spec.rb lib/common/spec/"
end

desc "Show Coverage http://IP_ADDRESS:8000"
task :coverage do
  sh "bundle exec ruby bin/webPageForCoverage.rb"
end


## Hide Command ##
namespace :unitTest do
  task :common do
    sh "bundle exec rspec lib/common/spec/"
  end
  #desc "Redis Unit Test" 
  task :redis do
    sh "bundle exec rspec lib/redis/spec/*Unit_spec.rb"
  end
  #desc "Mongodb Unit Test" 
  task :mongodb do
    sh "bundle exec rspec lib/mongodb/spec/*Unit_spec.rb"
  end
  #desc "Memcached Unit Test"
  task :memcached do
    sh "bundle exec rspec lib/memcached/spec/*Unit_spec.rb"
  end
  #desc "Cassandra Unit Test"
  task :cassandra do
    sh "bundle exec rspec lib/cassandra/spec/*Unit_spec.rb"
  end
end

######################
# TEST WITH DATABASE #
######################
namespace "test" do
  LOGTYPES = %w[redis memcached mongodb cassandra].freeze
  include TestRunner
  desc "TEST WITH ALL TARGET DATABASE" 
  task :all => [:memcached,:mongodb,:redis,:cassandra] do
    sh "echo 'Run All Test With A Target Database' "
  end
  desc "TEST WITH memcached" 
  task :memcached do
    test_multi_logs(LOGTYPES, "memcached")
  end
  desc "TEST WITH mongodb"
  task :mongodb do
    test_multi_logs(LOGTYPES, "mongodb")
  end
  desc "TEST WITH redis"
  task :redis do
    test_multi_logs(LOGTYPES, "redis")
  end
  desc "TEST WITH cassandra"
  task :cassandra do
    test_multi_logs(LOGTYPES, "cassandra")
  end
end

####################
## TEST YCSB MODE ##
####################
=begin
desc "Test YCSB MODE [All]"
task :test_ycsb => [:test_ycsb_redis, :test_ycsb_memcached, :test_ycsb_mongodb, :test_ycsb_cassandra]

desc "Test YCSB MODE for Redis"
task :test_ycsb_redis do
  system("bundle exec rspec lib/redis/spec/redis_ycsb_spec.rb")
end

desc "Test YCSB MODE for memcached"
task :test_ycsb_memcached do
  system("bundle exec rspec lib/memcached/spec/memcached_ycsb_spec.rb")
end

desc "Test YCSB MODE for mongodb"
task :test_ycsb_mongodb do
  system("bundle exec rspec lib/mongodb/spec/mongodb_ycsb_spec.rb")
end

desc "Test YCSB for cassandra"
task :test_ycsb_cassandra do
  system("bundle exec rspec lib/cassandra/spec/cassandra_ycsb_spec.rb")
end
=end

