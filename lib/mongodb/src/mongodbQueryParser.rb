#
# Copyright (c) 2017, Carnegie Mellon University.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the University nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
# HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

class MongodbQueryParser
  def initialize(logger)
    @logger = logger
  end
  def targetKeys(query)
    keys = []
    query.each{|k,v|
      if(v)then
        v2 = eval(v)
        keys = keys + traverseTargetKey(v2)
      end
    }
    return keys.uniq
  end
  def createKey2RealKey(doc,conds,headonly=false)
    key2Realkey = {}
    conds.each{|k,c|
      if(c.class == Hash)then
        c.each{|k2,c2|
          if(c2.class == String)then
            targetKey = c2.sub("$","")
            if(key2Realkey[c2] == nil)then
              key2Realkey[c2] = deepKey(doc,targetKey)
            end
          end
        }
      elsif(c.class == String)then
        targetKey = c.sub("$","")
        if(key2Realkey[c] == nil)then
          key2Realkey[c] = deepKey(doc,targetKey)
        end
      end
    }
    return key2Realkey
  end
  def deepKey(query,key)
    query.each{|k,v|
      if(k.to_s == key)then
        return "#{k.to_s}"
      elsif(v.class.to_s == "Hash")then
        deepKey = deepKey(v,key)
        if(deepKey)then
          return k.to_s + ".." + deepKey
        end
      end
    }
    return nil
  end
  def createGroupKey(doc,conds)
    key = ""
    conds.each{|k,v|
      if(v.class.to_s == "String")then
        if(doc[v.sub('$','')])then
          key += "#{k}=#{doc[v.sub('$','')].gsub(" ","")}_KEY_"
        else
          key += "#{k}=#{doc[v.sub('$','').to_sym].gsub(" ","")}_KEY_"
        end
      end
    }
    return key
  end
  def getParameter(query)
    # Group
    set = {
      "value" => {},
      "cond"  => {}
    }
    if(query["group"])then
      group = eval(query["group"])
      group.each{|s,v|
        set["cond"][s.to_s]  = v
        case v.class 
        when String then
          set["value"][s.to_s] = v
        when Hash then
          set["value"][s.to_s] = []
        end
      }
    end
    ## unwind
    if(query["unwind"])then
      # args["unwind"]
    end
    return set
  end
  def csv2docs(keys,values)
    docs = []
    rows = values.split("\n")
    rows.each{|row|
      hash = {}
      cols = row.split(",")
      keys.each_index{|index|
        hash[keys[index]] = cols[index]
        docs.push(hash)
      }
    }
    return docs
  end
  private 
  def traverseTargetKey(d)
    keys = []
    if(d.class == String and d[0] == "$")then
      keys.push(d.sub("$",""))
    elsif(d.class == Hash)then
      d.each{|k,v|
        keys = keys + traverseTargetKey(v)
      }
    end
    return keys
  end
end
