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

require "./cassandraCxxRunner"

begin
  client = CassandraCxxRunner.new()
  client.connect("127.0.0.1")

  ## Drop
  if(client.syncExecuter("drop keyspace if exists testdb"))then
    puts "[TEST] DROP :: PASSED"
  else
    puts "[TEST] DROP :: FAILED"
  end
  ## Create Keyspace
  if(client.syncExecuter("create keyspace testdb with replication = {'class':'SimpleStrategy','replication_factor':3}"))then
    puts "[TEST] Create Keyspace :: PASSED"
  else
    puts "[TEST] Create Keyspace :: FAILED"
  end
  ## Create Table
  if(client.syncExecuter("create table testdb.test( id text, value text, primary key (id));"))then
    puts "[TEST] Create Table :: PASSED"
  else
    puts "[TEST] Create Table :: FAILED"
  end



  ## Async Test
  client.commitQuery("insert into testdb.test (id,value) values ('id01', 'AAAA')")
  client.commitQuery("insert into testdb.test (id,value) values ('id02', 'BBBB')")
  client.commitQuery("insert into testdb.test (id,value) values ('id03', 'CCCC')")
  client.commitQuery("insert into testdb.test (id,value) values ('id04', 'DDDD')")
  100.times do |idx|
    client.commitQuery("insert into testdb.test (id,value) values ('mmid_#{idx}', 'value')")
  end
  
  client.asyncExecuter();
  client.resetQuery();
  client.commitQuery("select * from testdb.test")
  client.commitQuery("select count(*) from testdb.test")
  client.asyncExecuter();
  ans = client.getReply(0)
  count = client.getReply(1)

  data = ans.split("\n")
  checkFlag = true
  data.each{|row|
    cols = row.split(",")
    if(cols[0]  == "id01" and cols[1] != "AAAA")then
      checkerFlag = false
      puts "[TEST] Async Execution : FAILED :#{cols[0]},#{cols[1]}"
      break
    elsif(cols[0]  == "id02" and cols[1] != "BBBB")then
      checkerFlag = false
      puts "[TEST] Async Execution : FAILED :#{cols[0]},#{cols[1]}"
      break
    elsif(cols[0] == "id03" and cols[1] != "CCCC")then
      checkerFlag = false
      puts "[TEST] Async Execution : FAILED :#{cols[0]},#{cols[1]}"
      break
    elsif(cols[0] == "id04" and cols[1] != "DDDD")then
      checkerFlag = false
      puts "[TEST] Async Execution : FAILED :#{cols[0]},#{cols[1]}"
      break
    else
      if(cols[0].include?("mmid") and !cols[1].include?("value"))then
        checkerFlag = false
        puts "[TEST] Async Execution : FAILED :#{cols[0]},#{cols[1]}"
        break
      end
    end
  }
  if(checkFlag)then
    puts "[TEST] Async Execution : PASSED"
  end
  
  if(count == "104")then
    puts "[TEST] Async Execution(count) : PASSED"
  else
    puts "[TEST] Async Execution(count) : FAILED"
  end
  client.syncExecuter("drop keyspace if exists testdb")
  
    ## SET / MAP
  client.syncExecuter("drop keyspace if exists test;")
  client.syncExecuter("create keyspace if not exists test with replication = {'class':'SimpleStrategy','replication_factor':3};")
  #### SET
  client.syncExecuter("CREATE TABLE test.sets (id TEXT, val SET<TEXT>, primary key(id));");
  client.syncExecuter("INSERT INTO test.sets (id,val) VALUES ('test0',{'a','b'});")
  client.syncExecuter("INSERT INTO test.sets (id,val) VALUES ('test1',{'a','b'});")
  client.syncExecuter("SELECT * FROM test.sets")
  ans01 = client.getReply(0).gsub(" ","")
  expected01 = "test0,{'a','b'}\ntest1,{'a','b'}"

  if(ans01 == expected01)then
    puts "[TEST] Sync Execution(insert SET) : PASSED"
  else
    puts "[TEST] Sync Execution(insert SET) : FAILED"
    p "ans01"
    p ans01
    p "exp01"
    p expected01
    #puts "#{ans01} for #{expected01}"
  end
  
  #### MAP
  client.syncExecuter("CREATE TABLE test.maps (id TEXT, val MAP<TEXT,TEXT>, primary key(id));");
  client.syncExecuter("INSERT INTO test.maps (id,val) VALUES ('test0',{'key0':'val0','key1':'val1'});")
  client.syncExecuter("INSERT INTO test.maps (id,val) VALUES ('test1',{'key0':'val0','key1':'val1'});")
  client.syncExecuter("SELECT * FROM test.maps")
  ans =client.getReply(0).gsub(" ","")
  expected = "test0,{'key0':'val0','key1':'val1'}\ntest1,{'key0':'val0','key1':'val1'}"
  if(ans == expected)then
    puts "[TEST] Sync Execution(MAP) : PASSED"
  else
    puts "[TEST] Sync Execution(MAP) : FAILED"
    puts "#{ans} for #{expected}"
  end
  client.syncExecuter("drop keyspace if exists test")


  client.syncExecuter("create keyspace testdb with replication = {'class':'SimpleStrategy','replication_factor':3}");
  client.resetDatabase();
  

  client.close()
end
