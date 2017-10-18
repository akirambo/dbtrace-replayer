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

module Memcached2RedisOperation
  require_relative "../../memcached/src/memcached_utils"
  include MemcachedUtils
  private

  # @conv {"set(args x3)" => ["setex"]}
  # @conv {"set(args x2)" => ["set"]}
  def memcached_set(args)
    v = "NG"
    if args.size == 3
      v = setex(args)
    elsif args.size == 2
      v = set(args)
    else
      @logger.error("Unsupported Arguments #{args} @#{__method__}")
    end
    v
  end

  # @conv {"get" => ["get"]}
  def memcached_get(args)
    get(args)
  end

  # @conv {"add(args x3)" => ["get","setex"]}
  # @conv {"add(args x2)" => ["get","set"]}
  def memcached_add(args)
    v = "NG"
    if get([args[0]]).nil?
      v = memcached_set(args)
    end
    v
  end

  # @conv {"replace" => ["get","set"]}
  def memcached_replace(args)
    v = "NG"
    unless get([args[0]]).nil?
      v = memcached_set(args)
    end
    v
  end

  # @conv {"gets" => ["get"]}
  def memcached_gets(args)
    get(args)
  end

  # @conv {"append(args x3)" => ["get","setex"]}
  # @conv {"append(args x2)" => ["get","set"]}
  def memcached_append(args)
    str = get(args) + args.last.to_s
    args[args.length - 1] = str
    memcached_set(args)
  end

  # @conv {"prepend(args x3)" => ["get","setex"]}
  # @conv {"prepend(args x2)" => ["get","set"]}
  def memcached_prepend(args)
    str = get(args)
    pos = args.length - 1
    args[pos] = args[pos].to_s + str
    memcached_set(args)
  end

  # @conv {"cas(args x4)" => ["setex"]}
  # @conv {"cas(args x3)" => ["set"]}
  def memcached_cas(args)
    args.pop
    memcached_set(args)
  end

  # @conv {"incr" => ["incrby"]}
  def memcached_incr(args)
    incrby(args)
  end

  # @conv {"decr" => ["decrby"]}
  def memcached_decr(args)
    decrby(args)
  end

  # @conv {"delete" => ["del"]}
  def memcached_delete(args)
    del(args)
  end

  # @conv {"flush" => ["flushall"]}
  def memcached_flush(args)
    flushall(args)
  end
end
