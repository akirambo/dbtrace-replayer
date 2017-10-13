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

require "./redisCxxRunner"

def checker(client,api,testName,expected)
  prefix = "TEST(ruby) [#{testName}(#{api})]"
  answer = ""
  if(api == "sync")then
    answer = client.getReply()
  elsif(api == "async")then
    answer = client.getAsyncReply()
  end
  if(answer == expected)then
    puts "#{prefix} :: PASSED"
  else
    puts "#{prefix} :: FAILED"
    p "<answer>  :: #{answer}"
    p "<expected>:: #{expected}"
  end
end



begin
  ###############
  ## Sync Test ##
  ###############

  ## Initialize
  client = RedisCxxRunner.new()
  ip = ENV["REDIS_IPADDRESS"]
  unless ip
    ip = "127.0.0.1"
  end
  client.syncConnect(ip,6379)
  client.syncExecuter("flushall")  
  api = "sync"
  
  ## SET
  client.syncExecuter("set aaa AAA")
  checker(client,api,"set","OK")
  ## GET
  client.syncExecuter("get aaa")
  checker(client,api,"get","AAA")
  ## STRLEN
  client.syncExecuter("strlen aaa")
  checker(client,api,"strlen","3")
  ## HMSET
  client.syncExecuter("hmset key00 field0 value0 field1 value1")
  checker(client,api,"hmset","OK")
  ## HMGET
  client.syncExecuter("hmget key00 field0 field1")
  checker(client,api,"hmget","value0,value1")
  ## HGETALL
  client.syncExecuter("hgetall key00")
  checker(client,api,"hgetall","field0,value0,field1,value1")
  ## HKEYS
  client.syncExecuter("hkeys key00")
  checker(client,api,"hkeys","field0,field1")
  ## HVALS
  client.syncExecuter("hvals key00")
  checker(client,api,"hkeys","value0,value1")
  ## ZRANK
  client.syncExecuter("zadd zranks 100 e1")
  client.syncExecuter("zadd zranks 200 e2")
  client.syncExecuter("zadd zranks 300 e3")
  client.syncExecuter("zrank zranks e3")
  checker(client,api,"zrank","2")
  client.syncExecuter("zrank zranks e2")
  checker(client,api,"zrank","1")
  client.syncExecuter("zrank zranks e1")
  checker(client,api,"zrank","0")

  ## Check SET2[sadd, smember]
  client.syncExecuter("sadd set1  {\"a\":\"A\"}")
  client.syncExecuter("smembers set1")
  checker(client,api,"sadd/smembers","{\"a\":\"A\"}")

  ## 
  client.syncExecuter("keys *")
  checker(client,api,"keys","set1,zranks,key00,aaa")

  # Epilogue
  client.syncExecuter("flushall")
  client.syncClose()

  ################
  ## Async Test ##
  ################
  ## Initialize
  api = "async"
  client.asyncConnect(ip,6379)  
  ## Check Set/Get
  client.commitQuery("set testXX A")
  client.commitQuery("get testXX")
  client.asyncExecuter() 
  checker(client,api,"set/get(single)","A")

  ## Check Set/Get
  client.commitQuery("set test00 A")
  client.commitQuery("set test01 B")
  client.commitQuery("set test02 C")
  client.commitQuery("set test03 D")
  client.commitQuery("get test03")
  client.commitQuery("get test02")
  client.commitQuery("get test01")
  client.commitQuery("get test00")
  client.asyncExecuter()
  checker(client,api,"set/get(multi)","D\nC\nB\nA")
  
  ## Check Hash[hmset/hmget/hgetall/hkeys/hvals]
  client.commitQuery("hmset key0 f0 A f1 B")
  client.commitQuery("hmset key0 f2 C f3 D")
  client.commitQuery("hmget key0 f0 f1")
  client.commitQuery("hmget key0 f2 f3")
  client.commitQuery("hgetall key0")
  client.commitQuery("hkeys key0")
  client.commitQuery("hvals key0")
  client.asyncExecuter()
  ans = "A,B\nC,D\nf0,A,f1,B,f2,C,f3,D\nf0,f1,f2,f3\nA,B,C,D"
  checker(client,api,"hmset/hmget/hgetall/hkeys/hvals",ans)

  ## Check SET[sadd, smember]
  client.commitQuery("sadd set0  AA")
  client.commitQuery("sadd set0  BB")
  client.commitQuery("sadd set0  CC")
  client.commitQuery("smembers set0")
  client.asyncExecuter()
  ans = "CC,BB,AA"
  checker(client,api,"sadd/smembers",ans)

  ## Check SET2[sadd, smember]
  client.commitQuery("sadd set1  {\"a\":\"A\"}")
  client.commitQuery("sadd set1  {\"a\":\"B\"}")
  client.commitQuery("sadd set1  {\"a\":\"C\"}")
  client.commitQuery("smembers set1")
  client.asyncExecuter()
  ans = "{\"a\":\"A\"},{\"a\":\"B\"},{\"a\":\"C\"}"
  checker(client,api,"sadd/smembers",ans)

  
  # Epilogue
  client.asyncClose()
end
