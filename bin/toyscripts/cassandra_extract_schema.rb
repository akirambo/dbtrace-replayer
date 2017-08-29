
begin
  bareStr = `sudo cqlsh -e "describe schema"`
  schemas = []
  buf = []
  keyspaces = []
  keyspace = nil
  keyspaceFlag =false
  bareStr.split("\n").each{|line|
    if(line.size > 0)then
      if(line.include?("CREATE KEYSPACE"))then
        if(buf.size > 0)then
          if(keyspaceFlag)then
            keyspaces.push(buf.join(" "))
          else
            schemas.push(buf.join(" "))
          end
        end
        keyspaceFlag = true
        buf = []
        buf.push(line)
      elsif(line.include?("USE"))then
        keyspace = line.sub(/\s*USE\s*/,"").sub(";","").gsub(/\s/,"")
      elsif(line.include?("CREATE TABLE"))then
        ## Store Buffer
        ##if(buf.size > 0 and !keyspaceFlag)then
        if(buf.size > 0)then
          if(keyspaceFlag)then
            keyspaces.push(buf.join(" "))
          else
            schemas.push(buf.join(" "))
          end
        end
        ## Init Buffer & Flag
        keyspaceFlag = false
        buf = []
        line.sub!("CREATE TABLE ","CREATE TABLE #{keyspace}.")
        buf.push(line)
      else
        buf.push(line)
      end
    end
  }
  if(keyspaceFlag)then
    keyspaces.push(buf.join(" "))
  else
    schemas.push(buf.join(" "))
  end
  keyspaces.each{|ks|
    print ks.gsub(/\"/,"") + "\n"
  }
  schemas.each{|schema|
    schema.gsub!(/\"/,"")
    schema.sub!(/WITH.*/,";")
    print schema + "\n"
  }
end

