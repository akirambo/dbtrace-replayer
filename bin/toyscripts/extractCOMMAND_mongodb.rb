
begin
  buf = []
  File.open(ARGV[0],"r"){|f|
    while line = f.gets
      if(line.include?("D COMMAND") or
          line.include?("D QUERY"))then
        buf.push(line)
      end
    end
  }
  File.open("new_#{ARGV[0]}","w"){|f|
    f.write(buf.join(""))
  }
end
