
require "gviz"

gv = Gviz.new

gv.graph do
  route :AAA =>  [:BB]
  route :BB
  
  subgraph do
    global label:'Class A'
    node :AAA
  end
  subgraph do
    global label:'Class B'
    node :BB
  end
end


gv.save :test, :png
