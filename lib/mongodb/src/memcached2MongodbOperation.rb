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

module Memcached2MongodbOperation
  private

  # @conv {"SET" => ["INSERT"]}
  def memcached_set(args)
    r = false
    value = change_numeric_when_numeric(args[1])
    if args.size == 3
      r = insert([["testdb.col", { "_id" => args[0], "value" => value, "expire" => args[2] }]])
    elsif args.size == 2
      r = insert([["testdb.col", { "_id" => args[0], "value" => value }]])
    else
      @logger.fatal("Unsupprted Arguments [#{args}] @ #{__method__} ")
    end
    r
  end

  # @conv {"GET" => ["FIND"]}
  def memcached_get(args)
    cond = {
      "key" => "testdb.col",
      "filter" => { "_id" => args[0] },
    }
    v = find(cond)
    ret = []
    v.each do |doc|
      ret.push(doc["value"])
    end
    ret.join(",")
  end

  # @conv {"ADD" => ["SET"]}
  def memcached_add(args)
    memcached_set(args)
  end

  # @conv {"REPLACE" => ["UPDATE"]}
  def memcached_replace(args)
    value = change_numeric_when_numeric(args[1])
    cond = {
      "key" => "testdb.col",
      "query" => { "_id" => args[0] },
      "update" => { "$set" => { "value" => value } },
    }
    update(cond)
  end

  # @conv {"gets" => ["get"]}
  def memcached_gets(args)
    memcached_get(args)
  end

  # @conv {"APPEND" => ["FIND","UPDATE"]}
  def memcached_append(args)
    cond = {
      "key" => "testdb.col",
      "filter" => { "_id" => args[0] },
      "query" => { "_id" => args[0] },
    }
    str = find(cond)[0]["value"].to_s
    str += args.last.to_s
    cond["update"] = { "$set" => { "value" => str } }
    update(cond)
  end

  # @conv {"PREPEND" => ["FIND","UPDATE"]}
  def memcached_prepend(args)
    cond = {
      "key" => "testdb.col",
      "filter" => { "_id" => args[0] },
      "query" => { "_id" => args[0] },
    }
    str = find(cond)[0]["value"].to_s
    str = args.last.to_s + str
    cond["update"] = { "$set" => { "value" => str } }
    update(cond)
  end

  # @conv {"CAS" => ["SET"]}
  def memcached_cas(args)
    args.pop
    memcached_set(args)
  end

  # @conv {"INCR" => ["UPDATE"]}
  def memcached_incr(args)
    memcached_incr_decr(args, "incr")
  end

  # @conv {"DECR" => ["UPDATE"]}
  def memcached_decr(args)
    memcached_incr_decr(args, "decr")
  end

  def memcached_incr_decr(args, type)
    value = if type == "incr"
              change_numeric_when_numeric(args[1])
            elsif type == "decr"
              change_numeric_when_numeric(args[1]) * - 1
            end
    cond = {
      "key" => "testdb.col",
      "query" => { "_id" => args[0] },
      "update" => { "$inc" => { "value" => value } },
      "multi"  => true,
    }
    update(cond)
  end

  # @conv {"DELETE" => ["DELETE"]}
  def memcached_delete(args)
    cond = {
      "key" => "testdb.col",
      "filter" => { "_id" => args[0] },
    }
    delete(cond)
  end

  # @conv {"FLUSH" => ["DROP"]}
  def memcached_flush(_)
    drop(["testdb.col"])
  end

  #############
  ## PREPARE ##
  #############
  def prepare_memcached(operand, args)
    result = {}
    ## PREPARE SPECIAL OPERATION
    if %w[flushall].include?(operand)
      result["operand"] = operand
      return result
    end
    ## PREPARE OPERATION & ARGS
    result["operand"] = "memcached_#{operand}"
    result["args"] = @parser.exec(operand, args)
    result
  end
end
