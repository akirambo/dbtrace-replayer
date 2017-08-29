
begin
  sourceFile = ARGV[0]
  #targetFile = sourceFile.sub(/.log\Z/,".query")
  summaryFile = sourceFile.sub(/.log\Z/,".summary")
  buf = []
  commands = {}
  File.open(sourceFile,"r"){|f|
    while line = f.gets
      if(line.include?("INFO") and line.include?(" QUERY:"))then
        _data = line.chop
        data = _data.split(" INFO -- :  QUERY:")[1]
        command = data.split(" ")[0]
        if(!commands[command])then
          commands[command] = 0
        end
        commands[command] += 1
        buf.push(data)
      end
    end
  }
  ##File.open(targetFile,"w"){|f|
  ##  f.write(buf.join("\n"))
  ##}
  File.open(summaryFile,"w"){|f|
    commands.each{|com,num|
      f.write("#{com}:  #{num}\n")
    }
  }
end
