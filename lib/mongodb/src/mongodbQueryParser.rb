
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

  def targetkeys(query)
    keys = []
    query.each do |_, v|
      if v
        v2 = eval(v)
        keys += traverse_targetkey(v2)
      end
    end
    keys.uniq
  end

  def createkey2realkey(doc, conds, _)
    key2real = {}
    conds.each do |_, c|
      if c.class == Hash
        c.each do |_, c2|
          realkey = key2realkey(c2, doc, nil)
          if realkey
            key2real[c2] = realkey
          end
        end
      elsif c.class == String
        realkey = key2realkey(c, doc, nil)
        if realkey
          key2real[c] = realkey
        end
      end
    end
    key2real
  end

  def key2realkey(str, doc, key2real)
    if str.class == String
      targetkey = str.sub("$", "")
      if key2real.nil? || key2real[str].nil?
        return deepkey(doc, targetkey)
      end
    end
    nil
  end

  def deepkey(query, key)
    query.each do |k, v|
      if k.to_s == key
        return k.to_s
      elsif v.class.to_s == "Hash"
        deepkey_ = deepkey(v, key)
        if deepkey_
          return k.to_s + ".." + deepkey_
        end
      end
    end
    nil
  end

  def create_groupkey(doc, conds)
    key = ""
    conds.each do |k, v|
      if v.class.to_s == "String"
        key_element = doc[v.sub("$", "")]
        key_element_sym = doc[v.sub("$", "").to_sym]
        key += if key_element
                 "#{k}=#{key_element.delete(" ")}_KEY_"
               elsif key_element_sym
                 "#{k}=#{doc[v.sub('$', '').to_sym].delete(" ")}_KEY_"
               else
                 ""
               end
      end
    end
    key
  end

  def get_parameter(query)
    # Group
    set = {
      "value" => {},
      "cond"  => {},
    }
    if query["group"]
      group = eval(query["group"])
      group.each do |s, v|
        set["cond"][s.to_s] = v
        case v.class
        when String then
          set["value"][s.to_s] = v
        when Hash then
          set["value"][s.to_s] = []
        end
      end
    end
    ## unwind
    if query["unwind"]
      # args["unwind"]
    end
    set
  end

  def csv2docs(keys, values)
    docs = []
    rows = values.split("\n")
    rows.each do |row|
      hash = {}
      cols = row.split(",")
      keys.each_index do |index|
        hash[keys[index]] = cols[index]
        docs.push(hash)
      end
    end
    docs
  end

  private

  def traverse_targetkey(d)
    keys = []
    if d.class == String && d[0] == "$"
      keys.push(d.sub("$", ""))
    elsif d.class == Hash
      d.each do |_, v|
        keys += traverse_targetkey(v)
      end
    end
    keys
  end
end
