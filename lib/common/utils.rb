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
  def create_string(bytesize)
    val = SecureRandom.urlsafe_base64(bytesize, false)
    val[0] = "T"
    val
  end

  def create_numbervalue(bytesize)
    n = bytesize.to_i
    format("%0#{n}d", SecureRandom.random_number(10**n)).to_i
  end

  def parse_json(data)
    if data.class == Hash || data.class == Array
      return data
    end
    keys = parse_chars(data)
    keys.each do |key_|
      if key_[0] != "\""
        data = data.sub("#{key_}:", "\"#{key_}\":")
      end
    end
    hash = JSON.parse(data)
    hash
  end

  def convert_json(hash)
    str = hash.to_json
    hash.each_key do |key|
      str = str.sub("\"#{key}\":", " #{key}:")
    end
    str
  end

  def change_numeric_when_numeric(input)
    if /^[+-]?[0-9]*[\.]?[0-9]+$/ =~ input.to_s
      number = input.to_i
      if number < 2_147_483_648 && number > -2_147_483_648
        return number
      else
        return input.to_s
      end
    end
    input
  end

  def add_doublequotation(hash)
    str = hash.to_json
    hash.each do |_, v|
      if v.class == String && v.include?(":")
        return str
      end
    end
    str.delete!("\"")
    str.gsub!("{", "{\"")
    str.gsub!(":", "\":\"")
    str.gsub!("http\":\"", "http:")
    str.gsub!("https\":\"", "https:")
    str.gsub!(",", "\",\"")
    str.gsub!("}", "\"}")
    str.gsub!(/\"(\d+)\"/, '\1')
    str.gsub!("\"{", "{")
    str.gsub!("}\"", "}")
    str.gsub!(":\"[", ":[\"")
    str.gsub!("]\"", "\"]")
    str.gsub!("[\"{", "\[{")
    str.gsub!("}\"]", "}]")
    if str == "{\"\"}"
      str = "{}"
    end
    str
  end

  ## Convert {"a"=>"b"} to {:a => "b"}
  def stringhash2symbolhash(docs)
    convert_hash(docs, "string2symbol")
  end

  ## Convert {:a =>"b"} to {"a" => "b"}
  def symbolhash2stringhash(docs)
    convert_hash(docs, "symbol2string")
  end

  def convert_hash(docs, type)
    if docs.class == Hash
      return convert_hash_from_hash(docs, type)
    elsif docs.class == Array
      return convert_hash_from_array(docs, type)
    end
    nil
  end

  def convert_hash_from_hash(docs, type)
    ret = {}
    docs.each do |k, v|
      ret[change_typekey(k, type)] = case v.class.to_s
                                     when "Hash"
                                       convert_hash(v, type)
                                     else
                                       v
                                     end
    end
    ret
  end

  def convert_hash_from_array(docs, type)
    rets = []
    docs.each do |doc|
      rets.push(convert_hash_from_hash(doc, type))
    end
    rets
  end

  def parse_chars(data)
    keys = []
    key = ""
    error_strings = %w[{ } : [ ] ,].freeze
    data.chars do |char|
      if error_strings.none? { |m| char.include?(m) } && char != " "
        key += char
      elsif !key.empty? && (char == ":" || char == " ")
        keys.push(key)
        key = ""
      end
    end
    keys
  end

  ## only for convert_hash
  def change_typekey(k, type)
    if type == "symbol2string"
      k.to_s
    elsif type == "string2symbol"
      k.to_sym
    end
  end
end
