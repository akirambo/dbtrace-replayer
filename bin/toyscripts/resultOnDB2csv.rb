

begin
  dir = ARGV[0]
  rows = {}
  targetDB = nil
  sourceDB = nil
  
  Dir.glob("#{dir}/*.log").each{|file|
    File.open(file,"r"){|f|
      # parseLog
      while line = f.gets
        if(line.include?("=>"))then
          _data_ = line.chop.split("=>")
          sourceDB = _data_[0].split(" : ")[1].gsub(/\s/,"").downcase
          targetDB = _data_[1].gsub(/\s/,"").downcase
          rows["#{sourceDB}_TOTAL[count]"] = 0
          rows["#{sourceDB}_TOTAL[sec]"]   = 0.0

        elsif(line.include?("TOTAL[count]"))then
          if(!line.include?("flush"))then
            rows["#{sourceDB}_TOTAL[count]"] += line.split(/\s/).last.to_i
          end
        elsif(line.include?("TOTAL[sec]"))then
          if(!line.include?("flush"))then
            rows["#{sourceDB}_TOTAL[sec]"] += line.split(/\s/).last.to_f
          end
        end
      end
    }
  }
  ## output
  name_buf = [targetDB]
  count_buf = ["Total Count"]
  sec_buf = ["Total Time [sec]"]
  ratio_buf = ["Ratio"]
  rows.each{|k,v|
    if(k.include?("sec"))then
      name_buf.push(k.sub("_TOTAL[sec]"," Trace"))
      sec_buf.push(v)
      ratio_buf.push(v / rows["#{targetDB}_TOTAL[sec]"])
    elsif(k.include?("count"))then
      count_buf.push(v)
    end
  }
  puts name_buf.join(",")
  puts count_buf.join(",")
  puts sec_buf.join(",")
  puts ratio_buf.join(",")
end
