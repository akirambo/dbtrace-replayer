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

require "./memcachedCxxRunner"

begin
  client = MemcachedCxxRunner.new()
  env_ip = ENV["MEMCACHED_IPADDRESS"]
  unless env_ip 
    env_ip = "127.0.0.1"
  end
  client.connect("#{env_ip}:11211",true)

  ## SET / GET
  client.syncExecuter("set","aaa","AAA",0)
  if(client.getDuration().to_f != 0.0)then
    p "TEST(ruby) [set aaa AAA] PASSED"
  else
    p "TEST(ruby) [set aaa AAA] FAILED"
  end

  client.syncExecuter("get","aaa","",0)
  if(client.getDuration().to_f != 0.0 and
      client.getReply() == "AAA")then
    p "TEST(ruby) [get aaa AAA] PASSED"
  else
    p "TEST(ruby) [get aaa AAA] FAILED"
  end


  ## GET Nothing
  ret = client.syncExecuter("get","nothing","",0)
  if(ret == false)then
    p "TEST(ruby) [get nothing] PASSED"
  else
    p "TEST(ruby) [get nothing] FAILED"
  end

  ## INCR / DECR
  client.syncExecuter("set","num","1",0)
  if(client.getDuration().to_f != 0.0)then
    p "TEST(ruby) [set num 1] PASSED"
  else
    p "TEST(ruby) [set num 1] FAILED"
  end
  client.syncExecuter("incr","num","3",0)
  client.syncExecuter("get","num","",0)
  if(client.getReply() == "4")then
    p "TEST(ruby) [incr num 3] PASSED"
  else
    p "TEST(ruby) [incr num 3] FAILED"
  end

  client.syncExecuter("decr","num","2",0)
  client.syncExecuter("get","num","",0)
  if(client.getReply() == "2")then
    p "TEST(ruby) [decr num 2] PASSED"
  else
    p "TEST(ruby) [decr num 2] FAILED"
  end

  ## KEYS
  client.syncExecuter("set","test00","AAA",0)
  client.syncExecuter("set","test01","AAA",0)
  client.syncExecuter("set","test02","AAA",0)
  client.syncExecuter("set","test03","AAA",0)
  sleep 1
  storedKeys = client.keys().split(",")
  if(storedKeys.include?("test00") and
      storedKeys.include?("test01") and
      storedKeys.include?("test02") and
      storedKeys.include?("test03"))then
    p "TEST(ruby) [keys] PASSED"
  else
    p "TEST(ruby) [keys] FAILED"
  end

  client.syncExecuter("flush","","",0)
  client.close()
end

