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
  client.connect("127.0.0.1:11211",true)
  client.syncExecuter("set","test00","AAA")
  client.syncExecuter("set","test01","BBB")
  client.syncExecuter("set","test02","CCC")
  client.syncExecuter("set","test03","DDD")
  client.commitGetKey("test00")
  client.commitGetKey("test01")
  client.commitGetKey("test02")
  client.commitGetKey("test03")
  client.mget()
  p client.getDuration()
  if(client.mgetReply("test00") == "AAA")then
    p "[TEST] mgetReply :: PASSED"
  else
    p "[TEST] mgetReply :: FAILED"
  end
  if(client.mgetReply("") == "test00:AAA\ntest01:BBB\ntest02:CCC\ntest03:DDD" )then
    p "[TEST] mgetReply :: PASSED"
  else
    p "[TEST] mgetReply :: FAILED"
  end
  client.syncExecuter("flush","","")

  client.resetGetKeys()
  5000.times do |idx|
    p "test#{idx}"
    client.syncExecuter("set","test#{idx}","A")
  end


  5000.times do |idx|
    client.commitGetKey("test#{idx}")
  end
  client.mget()

  
  1000.times do |idx|
    #p client.mgetReply("test#{idx}")
  end
  client.getDuration()
  client.syncExecuter("flush","","")
  client.close()
end

