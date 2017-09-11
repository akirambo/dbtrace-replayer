
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
    "0x11" => "stats",
  }.freeze

  def initialize(filename, option, logger)
    @type_position = [1]
    @skip_types = [
      ## common
      "going", "NOT", "END", "FOUND", "sending",
      "STORED", "NOT_STORED", "version",
      "VERSION", "connection",
      "handled.", "class", "server", "send", "new", "Client",
      ## Binarly Protocol
      "0x81", "test", "read", "client", "writing", "found", "not", "len",
      "Deleting",
      # "0x04","0x00"
    ].freeze
    @get_key_command_from_found = %w[append prepend delete].freeze
    ## For yscb-mode
    ## For create supportedCommand
    @command2primitive = {
      "get" => "READ",
      "set" => "INSERT",
      "add" => "INSERT",
      "replace" => "UPDATE",
      "incr" => "UPDATE",
      "decr" => "UPDATE",
      "gets" => "READ",
      "cas" => "UPDATE",
      "append" => "UPDATE",
      "prepend" => "UPDATE",
      "delete" => "UPDATE",
      "flush" => "UPDATE",
    }.freeze
    logs = MemcachedLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys, option, logger)
  end

  def parse(line)
    data = line.chop.split("\s")
    @type_position.each do |index|
      if data.size > index
        command = data[index].downcase
        if @supportedCommand.include?(command)
          result = {}
          args = data
          result[command] = args
          return result
        else
          if !@skip_types.include?(command) && !integer_string?(command)
            @logger.warn "[WARNING] :: Unsupported Command #{command}"
          end
        end
      end
    end
    nil
  end

  def parse_multilines(filename)
    # incr/decr is able to get incr/decr value
    ## command => the position of argument
    gettable_value = {
      "incr" => 3,
      "decr" => 3,
    }
    results = {}
    logs = []
    File.open(filename, "r") do |f|
      split_term = "0x80"
      args = []
      command = ""
      line = f.gets
      until line.nil?
        data = line.chop.split("\s")
        if data[1] == split_term
          ######################
          ## flush & register ##
          ######################
          unless args.size.zero?
            command = args[0]
            if results[command].nil?
              results[command] = []
            end
            results[command].push(args)
            logs.push(args)
            args = []
          end
          ##########################
          ## Parse Request Header ##
          ##########################
          # Add Command
          command = BIN2COMMAND[data[2]]
          if @supportedCommand.include?(command)
            args = extract_args(f, data)
            args.unshift(command)
          else
            if !@skip_types.include?(command) &&
               !@skip_types.include?(data[0])
              @logger.warn("Unsupported Command #{command}")
              @logger.debug(data)
            end
          end
        else
          # Add Real KeyName
          if data.size > 2 && @supportedCommand.include?(data[1].downcase)
            ## CASE1 : command = data[1], key = data[2]
            args.push(data[2])
            command = data[1].downcase
          elsif data.size > 2 && @supportedCommand.include?(data[0].downcase)
            ## CASE2 : command = data[0], key = data[1]
            args.push(data[1])
            ## Add Real Value
            command = data[0].downcase
            if gettable_value.key?(command)
              args.push(data[2].sub(",", "").to_i)
            end
          elsif data.size > 3 && data.include?("FOUND") && @get_key_command_from_found.include?(command)
            ## Add Real Key
            args.push(data[3])
          end
        end
        ## GO TO NEXT
        line = f.gets
      end
      ## Push Final Command
      unless args.empty?
        command = args[0]
        logs.push(args)
        results[command].push(args)
      end
    end
    register_logs(logs)
    results.keys.uniq
  end

  def integer_string?(str)
    begin
      Integer(str)
    rescue ArgumentError
      return false
    end
    true
  end

  private

  def register_logs(logs)
    ## log format []
    logs.each do |args|
      ope = {}
      ope[args[0]] = []
      case args.size
      when 3 then
        ## [command, keyLength, valueLength, (key), (value)]
        register_logs_three_strings(ope, args)
      when 4 then
        ## [command, keyLength, valueLength, key, (value)]
        register_logs_four_strings(ope, args)
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
    end
  end

  def register_logs_three_strings(ope, args)
    ## Add key
    if args[1].to_i > 0
      ope[args[0]].push("x" + random_string(args[1].to_i - 1))
    end
    ## Add value
    if args[2].to_i > 0
      ope[args[0]].push("x" * args[2].to_i)
    end
    @logs.push(ope)
  end

  def register_logs_four_strings(ope, args)
    ## Add key
    ope[args[0]].push(args[3])
    ## Add value
    if args[2].to_i > 0
      ope[args[0]].push("x" * args[2].to_i)
    end
    @logs.push(ope)
  end

  def random_string(num)
    chars = ("a".."z").to_a + ("A".."Z").to_a
    result = ""
    num.times do
      result << chars[rand(chars.length)]
    end
    result
  end

  def extract_args(f, data)
    args = []
    ## GET Key Length
    key_length = data[3] + data[4].sub("0x", "")
    key_length = key_length.hex
    # p "key_length :: #{key_length}"
    args.push(key_length)
    ## line(skip)
    data = f.gets.chop.split("\s")
    ## GET Extra Length
    extra_length = data[1].hex
    # p "extra_length :: #{extra_length}"
    ## Value Length
    value_length = "0x"
    value_length__ = f.gets.chop.split("\s")
    value_length__.each do |byte|
      if byte.include?("0x")
        value_length += byte.sub("0x", "")
      end
    end
    value_length = value_length.hex - extra_length - key_length
    # p "value_length :: #{value_length}"
    args.push(value_length)
    ## line(skip)x4
    4.times do
      f.gets.chop.split("\s")
    end
    args
  end
end
