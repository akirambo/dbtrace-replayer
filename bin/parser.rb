#
# Copyright (c) 2017, Carnegie Mellon University.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the University nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
# HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

require "logger"
require "optparse"

## Require relative
path = File.expand_path(File.dirname(__FILE__))
@supported_databases = []

Dir.glob("#{path}/../lib/*").each do |fullpath|
  dbname = fullpath.split("/").last
  if dbname == "common" ||
     dbname == "tools" ||
     dbname == "cxx" ||
     dbname == "spec" ||
     dbname.include?(".rb")
    next
  else
    @supported_databases.push(dbname)
    require_relative "../lib/#{dbname}/src/#{dbname}Parser"
  end
end

require_relative "../lib/common/ycsbWorkload"
require_relative "../lib/redis/src/redisRunner"
require_relative "../lib/memcached/src/memcachedRunner"
require_relative "../lib/mongodb/src/mongodbRunner"
require_relative "../lib/cassandra/src/cassandraRunner"

SUPPORTED_MODE = %w[run clear ycsb].freeze
SUPPORTED_INPUT_FORMATS = {
  "redis"     => %w[basic],
  "memcached" => %w[binary basic],
  "hbase10"   => %w[ycsb],
  "cassandra" => %w[cql3 cql java basic],
  "mongodb"   => %w[basic],
}.freeze
SUPPORTED_DATAMODEL = %w[KEYVALUE DOCUMENT TABLE].freeze
PARSEMULTILINES = {
  "redis_basic"      => false,
  "memcached_basic"  => false,
  "memcached_binary" => true,
  "hbase10_ycsb"     => false,
  "cassandra_basic"  => false,
  "mongodb_basic"    => false,
}.freeze
SUPPORTED_ANALYSIS_MODES = %w[original primitive].freeze
SUPPORTED_YCSB_OUTPUT_FORMATS = %w[basic full].freeze
SUPPORTED_API_TYPES = %w[cxx].freeze
SUPPORTED_SYNC_MODES = %w[sync async].freeze
ARGV_CONFIG = %w[Database TargetFile].freeze

# Option
opt = OptionParser.new
opt.version = "0.2.0"
option = {
  :mode => SUPPORTED_MODE[0],
  :targetDB => nil,
  :inputFormat  => nil,
  :analysisMode => SUPPORTED_ANALYSIS_MODES[0],
  :ycsbOutputFormat => SUPPORTED_YCSB_OUTPUT_FORMATS[0],
  :logLevel => "INFO",
  :logFile => STDOUT,
  :sourceDB => nil,
  :parseMultiLines => false,
  :schemaFile => nil,
  :keyspace => "testdb",
  :columnfamily => "string",
  :collection => "collection",
  :times => 1,
  :logFileClean => false,
  :datamodel => "DOCUMENT",
  :key_of_keyvalue => "_id", # represent for key of keyvalue datamodel
  :api => "cxx",
  :async => false,
  :keepalive => true,
  :poolRequestMaxSize => 250,
}

def mode_setter(modes, option, error_code)
  unless modes.include?(option)
    puts error_code
    puts opt.banner
    abort
  end
end

opt.on("-a API", "--apitype API",
       "SET API TYPE [cxx(default)] ") do |v|
  option[:api] = v.downcase
  unless SUPPORTED_API_TYPES.include?(option[:api])
    puts "[ERROR] :: Unsupported API Type #{option[:api]}"
    puts opt.banner
    abort
  end
end
opt.on("-c", "--clearDB",
       "CLEAR DATABASE before & after run workload [false(default)] ") do |v|
  option[:clearDB] = v
end
opt.on("-d DATA_MODEL", "--data-model DATA_MODEL",
       "SELECT DATA_MODEL [KEYVALUE(default), DOCUMENT, TABLE]") do |v|
  model = v.upcase
  unless SUPPORTED_DATAMODEL.include?(model)
    abort("Please Set Correct Datamodel (#{model})")
  end
  option[:datamodel] = model
end
opt.on("-i INPUT_FORMAT",
       "--input-format INPUT_FORMAT",
       "SELECT INPUT FORMAT") do |v|
  option[:inputFormat] = v
  unless SUPPORTED_INPUT_FORMATS[option[:sourceDB].downcase].include?(option[:inputFormat])
    puts "[ERROR] :: Unsupported Input Format #{v} For  #{option[:sourceDB].downcase} -i / --input-format"
    puts opt.banner
    abort
  end
end
opt.on("--keyspace KEYSPACE_NAME",
       "SET KEYSPACE NAME FOR [cassandra]") do |v|
  option[:keyspace] = v
end
opt.on("-k", "--[no-]keep-connect",
       "KEEP CONNECT") do |v|
  option[:keepalive] = v
end
opt.on("--key-of-keyvalue KEYNAME", "SET KEY for KEYVALUE for [datamodel = keyvalue]") do |v|
  option[:key_of_keyvalue] = v
end
opt.on("-l LOG_LEVEL",
       "--log-level LOG_LEVEL",
       "SELECT LOG LEVEL [DEBUG,INFO(dafault),WARN,ERROR,FATAL]") do |v|
  option[:logLevel] = v.upcase
end
opt.on("--log-file LOG_FILE",
       "SELECT LOG FILE [STDOUT(default)]") do |v|
  option[:logFile] = v
end
opt.on("--log-file-clean",
       "LOG FILE CLEAN") do
  option[:logFileClean] = true
end
opt.on("-n TIMES", "--times TIMES",
       "Set Evaluation Time (default : 1)") do |v|
  option[:times] = v.to_i
end
opt.on("-m MODE", "--mode MODE",
       "SELECT MODE [run(default), clear(with target option), ycsb]") do |v|
  option[:mode] = v
  mode_setter(SUPPORTED_MODE, option[:mode], "[ERROR] :: Unsupported Mode #{option[:mode]}")
end
opt.on("--schema SCHEMA_FILENAME",
       "SET SCHEMA FILE FOR [cassandra]") do |v|
  option[:schemaFile] = v
end
opt.on("-s MODE", "--sync-mode MODE",
       "SET Synchronus Mode [sync(default), async]") do |v|
  unless SUPPORTED_SYNC_MODES.include?(v.downcase)
    abort("Please Set sync mode  #{SUPPORTED_SYNC_MODES.join(' or ')}")
  end
  option[:async] = if v == "async"
                     true
                   else
                     false
                   end  
end
opt.on("-t TARGET_DATABASE",
       "SELECT TARGET_DATABASE (only for TARGET DATABASE mode)") do |v|
  option[:targetDB] = v.downcase.capitalize
end
opt.on("-Y YCSB_OUTPUT_MODE",
       "--ycsb-output-mode YCSB_OUTPUT_MODE",
       "SELECT YCSB_OUTPUT_MODE (only for YCSB mode) [basic(default), full]") do |v|
  option[:ycsbOutputFormat] = v
  mode_setter(SUPPORTED_OUTPUT_FORMATS,
              option[:ycsbOutputFormat],
              "[ERROR] :: Unsupported YCSB OUTPUT FORMAT #{option[:ycsbOutputFormat]}")
end
opt.on("--parse-ycsb-mode PARSE_MODE",
       "SELECT ANALYSIS MODE (only for YCSB mode) [original(default),primitive]") do |v|
  option[:analysisMode] = v
  unless SUPPORTED_ANALYSIS_MODES.include?(option[:analysisMode])
    puts "[ERROR] :: Unsupported Analysis MODE #{option[:analysisMode]}"
    puts opt.banner
    abort
  end
end

opt.banner = "Usage: parser DATABASE [options]\n"
opt.banner += "Supported Databases:[#{@supported_databases.join(',')}]"

option[:sourceDB] = ARGV[0]
opt.parse!(ARGV)

## Argument Checker
if ARGV.size != ARGV_CONFIG.size &&
   option[:mode] != "clear"
  puts opt.banner
  abort
end
## Check parsemultiline
if option[:mode] != "clear"
  if option[:inputFormat].nil?
    option[:inputFormat] = SUPPORTED_INPUT_FORMATS[option[:sourceDB]][0]
  end
  option[:parseMultiLines] = PARSEMULTILINES[option[:sourceDB] + "_" +
                                             option[:inputFormat]]
end

def log_level(level)
  case level
  when "DEBUG" then
    Logger::DEBUG
  when "INFO" then
    Logger::INFO
  when "WARN" then
    Logger::WARN
  when "ERROR" then
    Logger::ERROR
  when "FATAL" then
    Logger::FATAL
  else
    abort("[ERROR]:: Unsupported Log Level #{level}")
  end
end

begin
  if !ARGV[0].nil? &&
     !@supported_databases.include?(ARGV[0].downcase)
    puts "[ERROR] :: Unsupported Database #{ARGV[0].downcase}"
    puts opt.banner
    abort
  end
  
  ###### LOGGER #######
  File.open(option[:logFile], "w") { |f| } if option[:logFileClean]
  logger = Logger.new(option[:logFile])
  logger.level = log_level(option[:logLevel])
  #####################

  ##### CLEAR DB #####
  if option[:mode] == "clear"
    runner_name = "#{option[:targetDB]}Runner"
    runner = Object.const_get(runner_name).new("", logger, option)
    runner.refresh
    exit
  end
  #####################

  unless option[:inputFormat]
    option[:inputFormat] = SUPPORTED_INPUT_FORMATS[ARGV[0].downcase][0]
  end

  ## PARSE
  parser_name = ARGV[0].downcase.capitalize + "Parser"
  target_file = ARGV[1]
  puts "Warming Workloads With #{parser_name} ..."
  parser = Object.const_get(parser_name).new(target_file, option, logger)
  parser.exec

  ## CONVERT/RUN
  if option[:mode] == "ycsb"
    converter = YCSBWorkload.new(parser.log)
    converter.exec
  elsif option[:mode] == "run"
    option[:targetDB] = ARGV[0].downcase.capitalize unless option[:targetDB]
    # Setup Runner
    runner_name = "#{option[:targetDB]}Runner"
    runner = Object.const_get(runner_name).new(ARGV[0], logger, option)
    # Run
    option[:times].times do |index|
      puts "Running Workloads ... ##{index + 1}"
      runner.exec(parser.workload)
    end
  end
  logger.close
end
