
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
