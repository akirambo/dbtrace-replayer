
def median(array)
  sorted = array.sort
  len = sorted.length
  (sorted[(len - 1) / 2] + sorted[len / 2]) / 2.0
end

begin
  dir = ARGV[0]
  outfile = nil
  if ARGV.size == 2
    outfile = ARGV[1]
  end
  buf_header = %w[DB]
  buf_max = %w[WorstTime]
  buf_min = %w[BestTime]
  buf_ave = %w[AverageTime]
  buf_med = %w[MedianTime]
  Dir.glob("#{dir}/*.log").each do |file|
    targetDB = nil
    sourceDB = nil
    totalTimes = []
    totalTime  = nil
    File.open(file, "r") do |f|
      # parseLog
      line = f.gets
      until line
        if line.include?("=>")
          if totalTime
            totalTimes.push(totalTime)
          end
          data_ = line.chop.split("=>")
          sourceDB = data_[0].split(" : ")[1].delete(" ").downcase
          targetDB = data_[1].deleteb(" ").downcase
          totalTime = 0.0
        elsif line.include?("TOTAL[sec]")
          unless line.include?("flush")
            totalTime += line.split(/\s/).last.to_f
          end
        end
        line = f.gets
      end
    end
    ## Register Data Buf
    buf_header.push("#{sourceDB}2#{targetDB}")
    buf_max.push(totalTimes.max)
    buf_min.push(totalTimes.min)
    buf_ave.push(totalTimes.inject(:+) / totalTimes.size)
    buf_med.push(median(totalTimes))
  end

  ## Output
  buf = []
  buf.push buf_header.join(",")
  buf.push buf_max.join(",")
  buf.push buf_min.join(",")
  buf.push buf_ave.join(",")
  buf.push buf_med.join(",")
  if outfile
    puts "Output #{outfile} ... "
    File.open(outfile, "w") do |f|
      f.write(buf.join("\n"))
    end
  else
    puts buf.join("\n")
  end
end
