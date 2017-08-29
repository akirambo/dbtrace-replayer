
require_relative "gvisGraph"

class Visualize
  def initialize()
    @targetFiles = {}
    @graphs      = GvisGraph.new()
  end
  def execute()
    extractTargetFiles()
    extractDependency()
    toPng()
  end
  
  private
  def extractTargetFiles
    path = File.expand_path(File.dirname(__FILE__))
    Dir.glob("#{path}/../../*").each{|fullpath|
      Dir.glob("#{fullpath}/src/*.rb").each{|target|
        if(target.include?("Operation"))then
          /lib\/(.+)\/src\// =~ target
          if(@targetFiles[$1] == nil)then
            @targetFiles[$1] = []
          end
          @targetFiles[$1].push(target)
        end
      }
    }
  end
  def extractDependency
    @targetFiles.each{|targetDB, files|
      files.each{|file|
        ## Create New GvisGraph
        File.open(file,"r"){|f|
          puts file
          while line = f.gets
            if(line.include?("@conv"))then
              line =~ /@conv(.*)/
              @graphs.push(eval("#{$1}"), file, targetDB)
            end
          end
        }
      }
    }
    @graphs.update
  end
  def toPng
    ## Generate gvis Files
    buf = @graphs.to_gviz("full_dependency")
    path = File.expand_path(File.dirname(__FILE__))
    dir = path + "/../../../output"
    FileUtils.mkdir_p(dir) unless FileTest.exist?(dir)
    File.open("#{dir}/full_dependency.rb", "w"){|f|
      f.write(buf)
    }
    ## Each Relationships
    @targetFiles.each{|db1,dbs2|
      target2 = db1.split("/").last.upcase()
      dbs2.each{|db2|
        target1 = db2.split("/").last.sub(/Operation.rb\Z/,"").upcase()
        if(target1 != target2)then
          name = "#{target1}_#{target2}"
          buf = @graphs.to_gviz(name, [target1,target2])
          File.open("#{dir}/#{name}_dependency.rb", "w"){|f|
            f.write(buf)
          }
        end
      }
    }
  end
end
