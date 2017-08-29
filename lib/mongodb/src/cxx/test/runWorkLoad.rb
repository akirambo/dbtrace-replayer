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

require "optparse"
require "./mongodbCxxRunner"


class Workload
  attr_accessor :docnum
  def initialize(url,options)
    @client = MongodbCxxRunner.new()
    @client.connect(url)
    @options = options
  end
  def set(database, collection)
    @client.setDatabaseName(database)
    @client.setCollectionName(collection)
  end
  def execute(operand)
    begin
      if(operand != "reset" and @options[:before_reset])then
        reset
      end
      send(operand)
      if(operand != "reset" and @options[:after_reset])then
        reset
      end
    rescue => e
      puts e.message
      puts "[Error] #{operand} is not supported."
    end
  end
  def close
    @client.close()
  end
  private
  def insertMany
    @options[:docnum].times do |idx|
      @client.commitDocument(doc(idx));
    end
    @client.insertMany()
  end
  def insert
    @options[:docnum].times do |idx|
      @client.insertOne(doc(idx))
   end
  end
  def find
    if(@client.find("{}"))then
      ans = @client.getReply()
      if(ans.size > 0)then
        puts "FOUND ITEMS"
      end
    end
  end
  def update
    @options[:docnum].times do |idx|
      @client.update("{\"_id\":\"id-#{idx}\"}","{\"$set\":{\"value\":3}}",false)
    end
  end
  def reset
    @client.drop()
  end
  def doc(idx)
    return "{\"_id\":\"id-#{idx}\",\"value\":\"XXXXXXXXXXXXXXXXXXXXXXX\"}"
  end
end

### OPTION ####
@options = {
  :after_reset  => false,
  :before_reset => false,
  :docnum => 1000000
}
banners = []
banners.push("Usage: #{__FILE__} OPERATION [options]")
banners.push("     : OPERATION  [insertMany, insert, update, find, reset]")
opts = OptionParser.new(banners.join("\n"))
opts.on("-a","--after-reset"){|v| @options[:after_reset] = true}
opts.on("-b","--before-reset"){|v| @options[:before_reset] = true}
opts.on("-n NUMBER","--doc-num NUMBER"){|v| @options[:docnum] = v.to_i}
################

begin
  opts.parse!(ARGV)
  runner = Workload.new("mongodb://127.0.0.1:27017", @options)
  runner.set("test","col")
  runner.execute(ARGV[0])
  runner.close
end
