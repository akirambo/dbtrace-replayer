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
@supportedDatabases = Array.new()

Dir.glob("#{path}/../lib/*").each{|fullpath|
  dbname = fullpath.split("/").last
  if(dbname != "common" and
      dbname != "tools"  and
      dbname != "cxx" and
      dbname != "spec" and
      !dbname.include?(".rb"))then
    @supportedDatabases.push(dbname)
    require_relative "../lib/#{dbname}/src/#{dbname}Parser"  
  end
}

require_relative "../lib/common/ycsbWorkload"
require_relative "../lib/redis/src/redisRunner"
require_relative "../lib/memcached/src/memcachedRunner"
require_relative "../lib/mongodb/src/mongodbRunner"
require_relative "../lib/cassandra/src/cassandraRunner"

SUPPORTED_MODE = ["run","clear","ycsb"]
SUPPORTED_INPUT_FORMATS = {
  "redis"     => ["basic"],
  "memcached" => ["binary","basic"],
  "hbase10"   => ["ycsb"],
  "cassandra" => ["cql3","cql","java","basic"],
  "mongodb"   => ["basic"]
}
SUPPORTED_DATAMODEL = ["KEYVALUE","DOCUMENT","TABLE"]
PARSEMULTILINES = {
  "redis_basic"      => false,
  "memcached_basic"  => false,
  "memcached_binary" => true,
  "hbase10_ycsb"     => false,
  "cassandra_basic"  => false,
  "mongodb_basic"    => false
}
SUPPORTED_ANALYSIS_MODES = ["original", "primitive"]
SUPPORTED_YCSB_OUTPUT_FORMATS = ["basic", "full"]
SUPPORTED_API_TYPES =["cxx"]
SUPPORTED_SYNC_MODES =["sync","async"]
ARGV_CONFIG = ["Database","TargetFile"]



# Option
opt = OptionParser.new
opt.version = "0.2.0"
option = {
  :mode   => SUPPORTED_MODE[0],
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
  :datamodel => "KEYVALUE",
  :api => "cxx",
  :async => false,
  :keepalive => true,
  :poolRequestMaxSize => 250,
}

opt.on("-a API","--apitype API", "SET API TYPE [cxx(default)] "){|v|
  option[:api] = v.downcase()
  if(!SUPPORTED_API_TYPES.include?(option[:api])) then
    puts "[ERROR] :: Unsupported API Type #{option[:api]}"
    puts opt.banner()
    abort()
  end
}
opt.on("-c","--clearDB", "CLEAR DATABASE before & after run workload [false(default)] "){|v|
  option[:clearDB] = v
}
opt.on("-d DATA_MODEL","--data-model DATA_MODEL","SELECT DATA_MODEL [KEYVALUE(default), DOCUMENT, TABLE]"){|v|
  model = v.upcase()
  if(!SUPPORTED_DATAMODEL.include?(model))then
    abort("Please Set Correct Datamodel (#{model})")
  end
  option[:datamodel] = model
}
opt.on("-i INPUT_FORMAT",
  "--input-format INPUT_FORMAT",
  "SELECT INPUT FORMAT"){|v|
  option[:inputFormat] = v 
  if(!SUPPORTED_INPUT_FORMATS[option[:sourceDB].downcase()].include?(option[:inputFormat])) then
    puts "[ERROR] :: Unsupported Input Format #{v} For  #{option[:sourceDB].downcase()} -i / --input-format"
    puts opt.banner()
    abort()
  end
}

opt.on("--keyspace KEYSPACE_NAME","SET KEYSPACE NAME FOR [cassandra]"){|v|
  option[:keyspace] = v
}
opt.on("-k","--[no-]keep-connect","KEEP CONNECT"){|v|
  option[:keepalive] = v
}
opt.on("-l LOG_LEVEL","--log-level LOG_LEVEL","SELECT LOG LEVEL [DEBUG,INFO(dafault),WARN,ERROR,FATAL]"){|v|
  option[:logLevel] = v.upcase()
}
opt.on("--log-file LOG_FILE","SELECT LOG FILE [STDOUT(default)]"){|v|
  option[:logFile] = v
}
opt.on("--log-file-clean","LOG FILE CLEAN"){|v|
  option[:logFileClean] = true
}
opt.on("-n TIMES","--times TIMES", "Set Evaluation Time (default : 1)"){|v|
  option[:times] = v.to_i 
}
opt.on("-m MODE", "--mode MODE","SELECT MODE [run(default), clear(with target option), ycsb]"){|v| option[:mode] = v
  if(!SUPPORTED_MODE.include?(option[:mode]))then
    puts "[ERROR] :: Unsupported Mode #{option[:mode]}"
    puts opt.banner()
    abort()
  end
}
opt.on("--schema SCHEMA_FILENAME","SET SCHEMA FILE FOR [cassandra]"){|v|
  option[:schemaFile] = v
}
opt.on("-s MODE","--sync-mode MODE", "SET Synchronus Mode [sync(default), async]"){|v|
  if(!SUPPORTED_SYNC_MODES.include?(v.downcase()))then
    abort("Please Set sync mode  #{SUPPORTED_SYNC_MODES.join(" or ")}")
  end
  if(v.downcase() == "async")then
    option[:async] = true
  end
}
opt.on("-t TARGET_DATABASE", "SELECT TARGET_DATABASE (only for TARGET DATABASE mode)"){|v|
  option[:targetDB] = (v.downcase()).capitalize 
}
opt.on("-Y YCSB_OUTPUT_MODE","--ycsb-output-mode YCSB_OUTPUT_MODE", "SELECT YCSB_OUTPUT_MODE (only for YCSB mode) [basic(default), full]"){|v| 
  option[:ycsbOutputFormat] = v
  if(!SUPPORTED_OUTPUT_FORMATS.include?(option[:ycsbOutputFormat]))then
    puts "[ERROR] :: Unsupported YCSB OUTPUT FORMAT #{option[:ycsbOutputFormat]}"
    puts opt.banner()
    abort()
  end
}
opt.on("--parse-ycsb-mode PARSE_MODE", "SELECT ANALYSIS MODE (only for YCSB mode) [original(default),primitive]"){|v| 
  option[:analysisMode] = v
  if(!SUPPORTED_ANALYSIS_MODES.include?(option[:analysisMode]))then
    puts "[ERROR] :: Unsupported Analysis MODE #{option[:analysisMode]}"
    puts opt.banner()
    abort()
  end
}


opt.banner  = "Usage: parser DATABASE [options]\n"
opt.banner += "Supported Databases:[#{@supportedDatabases.join(",")}]"


option[:sourceDB] = ARGV[0]
opt.parse!(ARGV)
## Argument Checker
if(ARGV.size != ARGV_CONFIG.size and option[:mode] != "clear")then
  puts opt.banner()
  abort()
end
## Check parsemultiline
if(option[:mode] != "clear")then
  if(option[:inputFormat] == nil)then
    option[:inputFormat] = SUPPORTED_INPUT_FORMATS[option[:sourceDB]][0]
  end
  option[:parseMultiLines] = PARSEMULTILINES[option[:sourceDB]+"_"+option[:inputFormat]]
end

def logLevel(level)
  case level
  when "DEBUG" then
    return Logger::DEBUG
  when "INFO" then
    return Logger::INFO
  when "WARN" then
    return Logger::WARN
  when "ERROR" then
    return Logger::ERROR
  when "FATAL" then
    return Logger::FATAL
  else
    abort("[ERROR]:: Unsupported Log Level #{level}")
  end
end

begin
  if(ARGV[0] != nil and !@supportedDatabases.include?(ARGV[0].downcase()))then
    puts "[ERROR] :: Unsupported Database #{ARGV[0].downcase()}"
    puts opt.banner()
    abort()
  end

  ###### LOGGER #######
  if(option[:logFileClean])then
    File.open(option[:logFile],"w"){|f|}
  end
  logger = Logger.new(option[:logFile])
  logger.level = logLevel(option[:logLevel])
  #####################


  ##### CLEAR DB #####
  if(option[:mode] == "clear")then
    runnerName  = "#{option[:targetDB]}Runner"
    runner = Object.const_get(runnerName).new("",logger, option)
    runner.refresh
    exit()
  end
  #####################

  if(!option[:inputFormat])then
    option[:inputFormat] = SUPPORTED_INPUT_FORMATS[ARGV[0].downcase()][0]
  end
  
  ## PARSE
  parserName = (ARGV[0].downcase()).capitalize + "Parser"
  targetFile = ARGV[1]
  puts  "Warming Workloads With #{parserName} ..."
  parser = Object.const_get(parserName).new(targetFile, option, logger)
  parser.exec()

  ## DATAMODEL
  if(option[:sourceDB] == "mongodb")then
    option[:datamodel] = "DOCUMENT"
  end

  
  ## CONVERT/RUN
  if(option[:mode] == "ycsb")then
    converter = YCSBWorkload.new(parser.log)
    converter.exec()
  elsif(option[:mode] == "run")then
    if(!option[:targetDB])then
      option[:targetDB] = ARGV[0].downcase().capitalize
    end
    
    # Setup Runner
    runnerName  = "#{option[:targetDB]}Runner"
    runner = Object.const_get(runnerName).new(ARGV[0],logger, option)
    
    # Run
    option[:times].times do |index|
      puts "Running Workloads ... ##{index+1}"
      runner.exec(parser.workload)
    end
  end
  logger.close
end
