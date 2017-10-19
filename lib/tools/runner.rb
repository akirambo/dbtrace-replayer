
## Runner Module 
module Runner
  def createOption(args,debugmode)
    options = []
    traceType = args[:traceType].downcase
    runType = args[:runType].downcase
    ## traceType
    #!! Attention traceType must push at first!!
    options.push(traceType)
    
    ## mode 
    options.push("-m run")

    ## times
    if(!args[:times])then
      args[:times] = 1
    end
    options.push("--times #{args[:times]}")

    ## runType
    options.push("-t #{runType}")
    if runType == "mongodb"
      options.push("-c")
    end
    ## logName
    if(args[:async] == "true")then
      options.push("--log-file #{traceType}2#{runType}_async.log")
      options.push("--sync-mode=async")
    else
      options.push("--log-file #{traceType}2#{runType}_sync.log")
      options.push("--sync-mode=sync")
    end

    ## trace
    options.push(args[:trace])
    
    ## output Query
    if(args[:datamodel])then
      options.push("-d #{args[:datamodel]}")
    end
    
    if args[:key_of_keyvalue]
      options.push("--key-of-keyvalue #{args[:key_of_keyvalue]}")
    end
    
    ## debug mode
    if(debugmode)then
      options.push("-l DEBUG")
    end
    
    ## schema
    if(traceType == "cassandra" or runType == "cassandra")then
      schema = nil
      if(args[:schema] and 
          (args[:schema] == "nil" or args[:schema].size == 0))then
        schema = args[:schema]
      else
        schema = args[:trace].sub(".trace",".schema")
      end
      options.push("--schema #{schema}")
      if(args[:keyspace])then
        options.push("--keyspace #{args[:keyspace].downcase}")
      end
    end
    return options.join(" ")
  end

  def exec(args,debugmode=false) 
    ## Executer
    executer = "bundle exec ruby ./bin/parser.rb "
    ## Create options
    options = createOption(args,debugmode)
    ## Exec
    sh "#{executer} #{options}"
  end
end

module TestRunner  
  def test_multi_logs(logtypes, dbtype)
    logtypes.each do |logtype|
      test(logtype, dbtype)
    end
  end

  def test(logtype, dbtype)
    executer = "bundle exec ruby ./bin/parser.rb "
    begin
      sh "#{executer} #{test_option(logtype, dbtype)}"
    rescue => e
      puts "#{executer} #{test_option(logtype, dbtype)}"
      puts "[ERROR] :: #{e}"
    end
  end
  
  def test_option(logtype, dbtype)
    files = getlog(logtype)
    options = []
    options.push(logtype)
    options.push("-m run")
    options.push("-t #{dbtype}")
    options.push("-l DEBUG")
    options.push(files)
    if dbtype == "cassandra"
      options.push("--keyspace testdb")
    end
    options.join(" ")
  end
  
  def getlog(logtype)
    ret = []
    prefix_dir = "lib/#{logtype}/spec/input"
    case logtype
    when "cassandra"
      ret.push("-i cql3")
      ret.push("#{prefix_dir}/cql3.log")
      ret.push("--schema #{prefix_dir}/cql3.schema")
    when "memcached"
      ret.push("-i binary")
      ret.push("#{prefix_dir}/memcached_all_command_binary_protocol.log")
      ret.push("--schema #{prefix_dir}/memcached_all_command.schema")
    when "mongodb"
      ret.push("#{prefix_dir}/all_command.log")
      ret.push("--schema #{prefix_dir}/mongodb_all_command.schema")
    when "redis"
      ret.push("#{prefix_dir}/redis_all_command.log")
      ret.push("--schema #{prefix_dir}/redis_all_command.schema")
    end
    ret.join(" ")
  end
end
