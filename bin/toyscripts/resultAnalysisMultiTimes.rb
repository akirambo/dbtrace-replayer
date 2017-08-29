
def median(array)
  sorted = array.sort
  len = sorted.length
  (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
end

begin
  dir = ARGV[0]
  outfile = nil
  if(ARGV.size == 2)then
    outfile = ARGV[1]
  end
  buf_header = ["DB"]
  buf_max    = ["WorstTime"]
  buf_min    = ["BestTime"]
  buf_ave    = ["AverageTime"]
  buf_med    = ["MedianTime"]
  Dir.glob("#{dir}/*.log").each{|file|
    targetDB = nil
    sourceDB = nil
    totalTimes = []
    totalTime  = nil
    File.open(file,"r"){|f|
      # parseLog
      while line = f.gets
        if(line.include?("=>"))then
          if(totalTime)then
            totalTimes.push(totalTime)
          end
          _data_ = line.chop.split("=>")
          sourceDB = _data_[0].split(" : ")[1].gsub(/\s/,"").downcase
          targetDB = _data_[1].gsub(/\s/,"").downcase
          totalTime = 0.0
        elsif(line.include?("TOTAL[sec]"))then
          if(!line.include?("flush"))then
            totalTime += line.split(/\s/).last.to_f
          end
        end
      end
    }
    ## Register Data Buf
    buf_header.push("#{sourceDB}2#{targetDB}")
    buf_max.push(totalTimes.max)
    buf_min.push(totalTimes.min)
    buf_ave.push(totalTimes.inject(:+) / totalTimes.size)
    buf_med.push(median(totalTimes))
  }

  ## Output
  buf = []
  buf.push buf_header.join(",")
  buf.push buf_max.join(",")
  buf.push buf_min.join(",")
  buf.push buf_ave.join(",")
  buf.push buf_med.join(",")
  if(outfile)then
    puts "Output #{outfile} ... "
    File.open(outfile,"w"){|f|
      f.write(buf.join("\n"))
    }
  else
    puts buf.join("\n")
  end
end
