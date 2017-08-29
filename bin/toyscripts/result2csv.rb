

begin
  dir = ARGV[0]
  times = {}
  Dir.glob("#{dir}/*.log").each{|file|
    File.open(file,"r"){|f|
      # parseLog
      targetDB = nil
      sourceDB = nil
      targetQuery = nil
      while line = f.gets
        if(!sourceDB and !targetDB and line.include?("=>"))then
          _data_ = line.chop.split("=>")
          sourceDB = _data_[0].split(" : ")[1].gsub(/\s/,"").downcase
          targetDB = _data_[1].gsub(/\s/,"").downcase
          times["#{sourceDB}2#{targetDB}"] = []
        elsif(line.include?("TOTAL[sec]"))then
          
        end
      end
    }
  }
end
