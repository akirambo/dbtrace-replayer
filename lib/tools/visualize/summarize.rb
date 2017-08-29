
class RawDataSet
  def initialize(name)
    @name        = name
    @raw = {
      :latency    => [],
      :throughput => []
    }
    @metrics = {
      :latency    => { :max => 0.0 , :min => 0.0 , :median => 0.0},
      :throughput => { :max => 0.0 , :min => 0.0 , :median => 0.0},
      :count      => {}
    }
  end
  def push(metricType, value)
    case metricType
    when "LATENCY" then
      @raw[:latency].push(value)
    when "THROUGHPUT" then
      @raw[:throughput].push(value)
    when "COUNT" then
      vals = value.split(" ")
      count = vals.size
      @metrics[:count][vals[count-2]] = vals[count-1].to_i
    end
  end
  def getRaw(type)
    @raw[type]
  end
  def getMetrics(type)
    @metrics[type]
  end
  def calc
    [:latency,:throughput].each{|type|
      @raw[type].sort!
      @metrics[type][:max] = @raw[type].last
      @metrics[type][:min] = @raw[type][0]
      @metrics[type][:median] = median(@raw[type])
    }
  end
private
  def median(array)
    count = array.length 
    if(count > 0)then
      if(count.odd?)then
        return array[(count - 1) / 2]
      else
        ## even
        return ( array[count/2] + array[count/2 - 1] )/2.to_f
      end
    end
    return 0.0
  end
end

class Formatter
  def initialize(dir,data)
    @dir  = dir 
    @data = data
    @prefix = {
      :latency    => "latency",
      :throughput => "throughput"
    }
  end
  def output
    ## Raw Data 
    raw(:latency)
    raw(:throughput)
    ## Statistics Data
    statistics(:latency,"async")
    statistics(:latency,"sync")
    statistics(:throughput,"async")
    statistics(:throughput,"sync")
  end
  private
  # Output Raw Data
  def raw(type)
    labels = @data.keys
    data   = {}
    max   = 0
    labels.each{|k|
      data[k] = @data[k].getRaw(type)
      if(max < data[k].size)then
        max = data[k].size 
      end
    }
    values = {}
    max.times do |idx|
      values[idx] = []
      labels.each{|k|
        val = ""
        if(data[k][idx])then
          val = data[k][idx]
        end
        values[idx].push(val)
      }
    end
    buf = []
    buf.push(labels.join(","))
    values.each{|index, data|
      buf.push(data.join(","))
    }
    File.open("#{@dir}/#{@prefix[type]}_raw.csv","w"){|f|
      f.write(buf.join("\n"))
    }
  end

  def statistics(type,apiType)
    labels = @data.keys
    data   = {}
    labels.each{|label|
      if(label.include?("_#{apiType}"))then
        _data_ = @data[label].getMetrics(type)
        _name_ = (label.sub("_cxx_#{apiType}","")).split("2")
        yLabelPrefix = _name_[0]
        xLabel = _name_[1]
        if(data[xLabel] == nil)then
          data[xLabel] = {}
        end
        if(data[xLabel]["#{yLabelPrefix} Trace + #{apiType}"] == nil)then
          data[xLabel]["#{yLabelPrefix} Trace + #{apiType}"] = _data_[:median]
          data[xLabel]["#{yLabelPrefix}(min)"]    = _data_[:min]
          data[xLabel]["#{yLabelPrefix}(max)"]    = _data_[:max]
        end
      end
    }
    ## 
    if(data.keys.size > 0)then
      header = ["DATA"]
      
      ## SORT ##
      prefixedOrder = {
        "redis" => 0,
        "memcached" => 1,
        "mongodb" => 2,
        "cassandra" => 3
      }
      data = data.sort{|(k0,v0),(k1,v1)|
        prefixedOrder[k0] <=> prefixedOrder[k1]
      }
      data.each{|k,v|
        header.concat(v.keys)
        break
      }
      ###########
      rows = [header.join(",")]
      data.each{|xlabel,value|
        row = []
        row.push(xlabel)
        value.each{|k,v|
          row.push(v)
        }
        rows.push(row.join(","))
      }
      File.open("#{type}_#{apiType}_statictics.csv","w"){|f|
        f.write(rows.join("\n"))
      }
    end
  end
end

class Parser
  attr_reader :data
  def initialize(targetDir)
    @dir = targetDir
    @data = {}
  end
  def exec(filter=nil)
    Dir.glob("#{@dir}/*.log").each{|filename|
      if(filter == nil or filename.include?(filter))then
        name = filename.sub(".log","").sub("#{@dir}/","")
        @data[name] = RawDataSet.new(name)
        ## Metrics
        ["LATENCY","THROUGHPUT"].each{|type|
          stdout = `grep "#{type}" #{filename}`
          if(stdout)then
            stdout.split("\n").each{|_time_|
              @data[name].push(type,_time_.split(" ").last.to_f)
            }
          end
        }
        @data[name].calc
        ## Count
        File.open(filename,"r"){|f|
          flag = false
          while line_ = f.gets
            line = line_.chop
            if(line.include?("-- Metrics Detail --"))then
              flag = true
            elsif(line.include?("-- GENERATED QUERY --"))then
              break
            elsif(flag)then
              @data[name].push("COUNT",line)
            end
          end
        }
      end
    }
  end
end

begin
  dir = "./"
  if(ARGV.size != 0)then
    dir = ARGV[0]
  end
  parser = Parser.new(dir)
  parser.exec()
  formatter = Formatter.new(dir,parser.data)
  formatter.output
end
