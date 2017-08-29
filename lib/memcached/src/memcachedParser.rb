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

require "securerandom"

require_relative "memcachedLog"
require_relative "../../common/abstractDBParser"

class MemcachedParser < AbstractDBParser
  BIN2COMMAND = {
    "0x00" => "get",
    "0x01" => "set",
    "0x02" => "add",
    "0x03" => "replace",
    "0x04" => "delete",
    "0x05" => "incr",
    "0x06" => "decr",
    "0x07" => "quit",
    "0x08" => "flush",
    "0x09" => "getq",
    "0x0a" => "no-op",
    "0x0b" => "version",
    "0x0c" => "getk",
    "0x0d" => "getkq",
    "0x0e" => "append",
    "0x0f" => "prepend",
    "0x11" => "stats"
  }
  def initialize(filename, option, logger)
    @typePosition   = [1]
    @skipTypes      = [
      ## common
      "going","NOT","END","FOUND","sending",
      "STORED","NOT_STORED","version",
      "VERSION","connection",
      "handled.","class","server","send","new","Client",
      ## Binarly Protocol
      "0x81","test","read","client","writing","found","not","len",
      "Deleting",
      #"0x04","0x00"
    ]
    @GetKeyCommandFromFOUND = ["append","prepend","delete"]
    ## For yscb-mode
    ## For create supportedCommand
    @command2primitive = {
      "get"     => "READ",
      "set"     => "INSERT",
      "add"     => "INSERT",
      "replace" => "UPDATE",
      "incr"    => "UPDATE",
      "decr"    => "UPDATE",
      "gets"    => "READ",
      "cas"     => "UPDATE",
      "append"  => "UPDATE",
      "prepend" => "UPDATE",
      "delete"  => "UPDATE",
      "flush"   => "UPDATE"
    }
    logs = MemcachedLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys(), option, logger)
  end
  def parse(line)
    data = line.chop.split("\s")
    @typePosition.each{|index|
      if(data.size > index)then
        command = data[index].downcase
        if(@supportedCommand.include?(command))then
          result = Hash.new
          args   = data
          result[command] = args
          return result
        else
          if(!@skipTypes.include?(command) and !integer_string?(command))then
            @logger.warn "[WARNING] :: Unsupported Command #{command}"
          end
        end
      end
    }
    return nil
  end
  def parseMultiLines(filename)
    # incr/decr is able to get incr/decr value
    ## command => the position of argument
    gettableValue = {
      "incr" => 3,
      "decr" => 3
    }
    results = {}
    logs  = []
    File.open(filename, "r"){|f|
      splitTerm = "0x80"
      args = []
      supportedCommandFlag = false
      command = ""
      while line = f.gets
        data = line.chop.split("\s")
        if(data[1] == splitTerm)then
          ######################
          ## flush & register ##
          ######################
          if(args.size > 0)then
            command = args[0]
            if(results[command] == nil)then
              results[command] = []
            end
            results[command].push(args)
            logs.push(args)
            args = []
            supportedCommandFlag = false
          end
          
          ##########################
          ## Parse Request Header ##
          ##########################
          # Add Command
          command = BIN2COMMAND[data[2]]
          if(@supportedCommand.include?(command))then
            args = extractArgs(f,data)
            args.unshift(command)
          else
            supportedCommandFlag = false
            if(!@skipTypes.include?(command) and
               !@skipTypes.include?(data[0]))then
              @logger.warn("Unsupported Command #{command}")
              @logger.debug(data)
            end
          end
        else
          # Add Real KeyName 
          if(data.size > 2 and @supportedCommand.include?(data[1].downcase()))then
            ## CASE1 : command = data[1], key = data[2]
            args.push(data[2])
            command = data[1].downcase()
          elsif(data.size > 2 and @supportedCommand.include?(data[0].downcase()))then
            ## CASE2 : command = data[0], key = data[1]
            args.push(data[1])
            ## Add Real Value
            command = data[0].downcase()
            if(gettableValue.key?(command))then
              args.push(data[2].sub(",","").to_i)
            end
          elsif(data.size > 3 and data.include?("FOUND") and @GetKeyCommandFromFOUND.include?(command))then
            ## Add Real Key
            args.push(data[3])
          end
        end
      end
      ##Push Final Command
      if(args.size > 0)then
        command = args[0]
        logs.push(args)
        results[command].push(args)
        args = []
      end
    }
    registerLogs(logs)
    return results.keys().uniq
  end
  def integer_string?(str)
    begin
      Integer(str)
      return true
    rescue ArgumentError
      return false
    end
  end
  private
  def registerLogs(logs)
    ## log format []
    logs.each{|args|
      ope = {}
      ope[args[0]] = []
      case args.size
      when 3 then
        ## [command, keyLength, valueLength, (key), (value)]
        ## Add key
        if(args[1].to_i > 0)then
          ope[args[0]].push("x"+randomString(args[1].to_i-1))
        end
        ## Add value
        if(args[2].to_i > 0)then
          ope[args[0]].push("x"*args[2].to_i)
        end
        @logs.push(ope)
      when 4 then
        ## [command, keyLength, valueLength, key, (value)]
        ## Add key
        ope[args[0]].push(args[3])
        ## Add value
        if(args[2].to_i > 0)then
          ope[args[0]].push("x"*args[2].to_i)
        end
        @logs.push(ope)
      when 5 then
        ## [command, keyLength, valueLength, key, value]
        ## Add key
        ope[args[0]].push(args[3])
        ## Add value
        ope[args[0]].push(args[4])
        @logs.push(ope)
      else
        @logger.warn("Unsupported Command & Arguments")
      end
    }
  end
  def randomString(num)
    chars = ("a".."z").to_a + ("A".."Z").to_a
    result = ""
    num.times do
      result << chars[rand(chars.length)]
    end
    return result
  end
  def extractArgs(f,data)
    args = []
    ## GET Key Length
    keyLength = data[3] + data[4].sub("0x","")
    keyLength = keyLength.hex
    #p "keyLength :: #{keyLength}"
    args.push(keyLength)
    ## line(skip)
    data = f.gets.chop.split("\s")
    ## GET Extra Length 
    extraLength = data[1].hex
    #p "extraLength :: #{extraLength}"
    ## Value Length
    valueLength = "0x"
    __valueLength__ =  f.gets.chop.split("\s")
    __valueLength__.each{|byte|
      if(byte.include?("0x"))then
        valueLength += byte.sub("0x","")
      end
    }
    valueLength = valueLength.hex - extraLength - keyLength
    #p "valueLength :: #{valueLength}"
    args.push(valueLength)
    ## line(skip)x4
    4.times do
      f.gets.chop.split("\s")
    end
    return args
  end
end
