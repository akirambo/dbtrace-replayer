
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

module MongodbOperation
  private

  def insert(args)
    v = false
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    if args.empty?
      ## do nothing
      return true
    end
    connect
    args.each do |arg|
      names = arg[0].split(".")
      if names.size == 1
        names.unshift("dummy")
      end
      @client.setDatabaseName(names[0])
      @client.setCollectionName(names[1])
      json = @utils.add_doublequotation(arg[1])
      if @option[:async]
        v = @client.commitDocument(json)
        add_count("INSERT")
      else
        v = @client.syncExecuter(__method__.to_s, json.to_s)
        add_duration(@client.getDuration, "database", __method__)
      end
      unless v.nil?
        close
        return v
      end
    end
    close
    v
  end

  def update(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    query = @utils.add_doublequotation(args["query"])
    doc = @utils.add_doublequotation(args["update"])
    v = @client.update(query, doc, args["multi"])
    close
    v
  end

  def find(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    results = []
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    json = @utils.add_doublequotation(args["filter"])
    v = @client.find(json.to_s)
    add_duration(@client.getDuration, "database", __method__)
    close
    if v
      rows = reply2rows(@client.getReply)
      results = []
      unless rows.size.zero?
        rows.each do |row|
          row.gsub(":", "=>")
          results.push(eval(row.gsub(":", "=>")))
        end
      end
    end
    results
  end

  def delete(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    filter = @utils.add_doublequotation(args["filter"])
    v = @client.deleteExecuter(filter, true)
    add_duration(@client.getDuration, "database", __method__)
    close
    v
  end

  def count(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    names = args["key"].split(".")
    @client.setDatabaseName(names[0])
    @client.setCollectionName(names[1])
    filter = @utils.add_doublequotation(args["query"])
    count = @client.count(filter)
    add_duration(@client.getDuration, "database", __method__)
    close
    count
  end

  def aggregate(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    %w[match group unwind].each do |type|
      if args[type]
        @client.setAggregateCommand(type, args[type])
      end
    end
    v = @client.aggregate
    add_duration(@client.getDuration, "database", __method__)
    close
    if v
      return @client.getReply
    end
    ""
  end

  def group(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    @logger.warn("Unimplemented..")
  end

  def mapreduce(args)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    @logger.warn("Unimplemented..")
  end

  def drop(args, init_flag = false)
    @logger.debug("GENERATED QUERY: #{__method__} #{args}")
    connect
    r = false
    if args.size == 1
      names = args[0].split(".")
      drop_exec(names)
      r = @client.drop
      if @metrics && !init_flag
        add_duration(@client.getDuration, "database", __method__)
      end
    end
    close
    r
  end

  def drop_exec(names)
    if names[0] && names[1]
      @client.setDatabaseName(names[0])
      @client.setCollectionName(names[1])
    elsif names[0]
      @client.setDatabaseName(names[0])
      @client.clearCollectionName
    end
  end

  def reply2rows(str)
    str.gsub(/\"/, '"').split("\n")
  end

  ###########
  # PREPARE #
  ###########
  def prepare_mongodb(operand, args)
    result = {}
    result["operand"] = operand
    result["args"] = @parser.exec(operand, args, true)
    result
  end
end
