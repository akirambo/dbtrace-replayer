
class SubGraph
  attr_reader :name,:nodes
  def initialize(name)
    @name   = name
    @nodes  = {}
    @routes = {}
  end
  def push(route, type)
    ## Add Nodes & Routes
    route.each{|source, nodes|
      if(type == "target")then
        nodes.each{|node|
          nodeName = "#{@name}::#{node}"
          if(node != "" and !@nodes.include?(nodeName))then
            id = @nodes.keys.size + 1
            @nodes[nodeName] = ":#{@name}Node#{id}"
          end
        }
      elsif(type == "source")then
        id = @nodes.keys.size + 1
        label = ":#{@name}Node#{id}"
        @nodes["#{@name}::#{source}"] = label
      end
    }
  end
  def pushRoute(target, route)
    route.each{|source, targets|
      label = @nodes["#{@name}::#{source}"]
      @routes[label] = []
      targets.each{|node|
        if(node != "")then
          @routes[label].push("#{target}::#{node}")
        end
      }
    }
  end
  def updateTargetNodes(subGraphs)
    @routes.each{|label, nodes|
      nodes.each_index{|index|
        dbName = nodes[index].split("::").first
        if(targetNodeLabel = subGraphs[dbName].nodes[nodes[index]])then
          nodes[index] = targetNodeLabel
        end
      }
    }
  end

  def draw(type)
    if type == "routes"
      drawRoutes
    elsif type == "subgraph"
      drawSubGraph
    end
  end

  def drawRoutes
    buf = "\n"
    @routes.each{|node, __targets__|
      targets = __targets__.to_s.gsub('"','')
      buf += "\troute #{node} => #{targets}\n"
    }
    return buf
  end

  def drawSubGraph
    buf  = "\n\tsubgraph do\n"
    buf += "\t\tglobal label:'#{@name}'\n"
    @nodes.each{|label, node|
      buf += "\t\tnode #{node}, label:'#{label}'\n"
    }
    buf += "\tend\n"
    return buf
  end
end

class GvisGraph
  ############
  ## LAYOUT ##
  ############
  ## circo dot fdp neato osage patchwork sfdp twopi
  LAYOUT       = "dot"
  PREFIX_CODE  = "require 'gviz'\n\ngv = Gviz.new\n\ngv.graph do\n\tglobal layout: '#{LAYOUT}'\n\nglobal rankdir: 'LR'\n"
  POSTFIX_CODE = "end\ngv.save :filename, :png\n"
  def initialize()
    @subGraphs  = {}
  end
  def push(route, source, target)
    ## Format target/source DBName
    target = target.split("/").last.upcase()
    source = source.split("/").last.sub(/\.rb\Z/,"").sub("Operation","").upcase()
    if(target != source)then
      ## Create SubGraph for Target Node
      createSubGraph(target, route, "target")
      ## Create SubGraph for Source Node 
      createSubGraph(source, route, "source")
      ## Add Info for Route
      pushRoute(source, target, route)
    end
  end
  def update
    @subGraphs.each{|name, subGraph|
      subGraph.updateTargetNodes(@subGraphs)
    }
  end
  def to_gviz(filename, targets=[])
    buf = ""
    ## Perfix Code
    buf += PREFIX_CODE
    ## Draw Routes & SubGraph
    %w[routes subgraph].each do |type|
      @subgGraphs.each do |name, subGraph|
        if targets.empty? || targets.include?(name)
          buf += subGraph.draw(type)
        end
      end
    end
    ## Postfix Code
    buf += POSTFIX_CODE.sub("filename", filename)
    return buf
  end

  private

  def createSubGraph(name,route,type)
    if(@subGraphs[name] == nil)then
      @subGraphs[name] = SubGraph.new(name)
    end
    @subGraphs[name].push(route, type)
  end
  def pushRoute(source, target, route)
    @subGraphs[source].pushRoute(target,route)
  end
end
