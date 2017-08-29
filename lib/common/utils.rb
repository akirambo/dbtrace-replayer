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

require "json"
require "securerandom"

class Utils
  def initialize
  end
  def createString(bytesize)
    val = SecureRandom.urlsafe_base64(bytesize,false)
    val[0] = "T"
    return val
  end

  def createNumberValue(bytesize)
    n =  bytesize.to_i
    return format("%0#{n}d", SecureRandom.random_number(10**n)).to_i
  end
  def parseJSON(data)
    key = ""
    keys = []
    if(data.class == Hash or data.class == Array)then
      return data
    end
    data.chars{|char|
      if(char != " " and char != "{" and char != "}" and char != ":" and 
          char != "[" and char != "]" and char != ",")then
        key += char
      elsif(key.size > 0 and (char == ":" or char == " "))then
        keys.push(key)
        key = ""
      end
    }
    keys.each{|key_|
      if(key_[0] != "\"")then
        data = data.sub("#{key_}:","\"#{key_}\":")
      end
    }
    hash = {}
    hash = JSON.parse(data)
    return hash
  end
  def convJSON(hash)
    str = hash.to_json
    hash.each_key{|key|
      str = str.sub("\"#{key}\":"," #{key}:")
    }
    return str
  end
  def changeNumericWhenNumeric(input)
    if(/^[+-]?[0-9]*[\.]?[0-9]+$/ =~ input.to_s)then
      number = input.to_i
      if(number < 2147483648 and number > -2147483648)then
        return number
      end
      return input.to_s
    end
    return input
  end
  def addDoubleQuotation(hash)
    str = hash.to_json
    hash.each{|k,v|
      if(v.class == String and v.include?(":"))then
        return str
      end
    }
    str.gsub!(/\"/,"")
    str.gsub!("{","{\"")
    str.gsub!(":","\":\"")
    str.gsub!("http\":\"","http:")
    str.gsub!("https\":\"","https:")
    str.gsub!(",","\",\"")
    str.gsub!("}","\"}")
    str.gsub!(/\"(\d+)\"/,'\1')
    str.gsub!("\"{","{")
    str.gsub!("}\"","}")
    str.gsub!(":\"[",":[\"")
    str.gsub!("]\"","\"]")
    str.gsub!("[\"{","\[{")
    str.gsub!("}\"]","}]")
    if(str == "{\"\"}")then
      str = "{}"
    end
    return str
  end
  ## Convert {"a"=>"b"} to {:a => "b"}
  def stringHash2symbolHash(docs)
    return convertHash(docs,"string2symbol")
  end
  ## Convert {:a =>"b"} to {"a" => "b"}
  def symbolHash2stringHash(docs)
    return convertHash(docs,"symbol2string")
  end
  def convertHash(docs, type)
    if(docs.class == Hash)then
      ret = {}
      docs.each{|k,v|
        if(v.class == Hash)then
          ret[changeTypeKey(k,type)] = convertHash(v,type)
        else
          ret[changeTypeKey(k,type)] = v
        end
      }
      return ret
    elsif(docs.class == Array)then
      rets = []
      docs.each{|doc|
        ret = {}
        doc.each{|k,v|
          if(v.class == Hash)then
            ret[changeTypeKey(k,type)] = convertHash(v,type)
          else
            ret[changeTypeKey(k,type)] = v
          end
        }
        rets.push(ret)
      }
      return rets
    end
    return nil
  end
  ## only for convertHash
  def changeTypeKey(k,type)
    if(type == "symbol2string")then
      return k.to_s
    elsif(type == "string2symbol")then
      return k.to_sym
    end
  end
end
