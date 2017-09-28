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

require "./mongodbCxxRunner"
require "json"

def reply2rows(str)
  rows = str.gsub(/\"/,"").split("\n")
  return rows
end

def testInsertOneAndFind(client)
  # [SETUP]
  client.drop()
  # [TEST] Insert One
  if(client.insertOne("{\"_id\":\"id000\",\"value\":\"AAAA\"}") and
      client.insertOne("{\"_id\":\"id001\",\"value\":\"BBBB\"}") and
      client.insertOne("{\"_id\":\"id002\",\"value\":\"AAAA\"}") and
      client.insertOne("{\"_id\":\"id003\",\"value\":\"BBBB\"}"))then
    puts "PASSED :: TEST(ruby) [InsertOne]"
  else
    puts "FAILED :: TEST(ruby) [InsertOne]"
  end
  # [TEST] Find w/o condition
  if(client.find("{}"))then
    ans = client.getReply()
    if(ans.include?("id000") and ans.include?("id001") and
        ans.include?("id002") and ans.include?("id003"))then
      puts "PASSED :: TEST(ruby) [Find w/o condition]"
    else
      puts "FAILED :: TEST(ruby) [Find w/o condition]"
      puts reply2rows(ans)
    end
  else
    puts "FAILED :: TEST(ruby) [Find w/o condition]"
  end
  # [TEST] Find w/ condition
  if(client.find("{\"value\":\"AAAA\"}"))then
    ans = client.getReply()
    if(ans.include?("id000") and ans.include?("id002"))then
      puts "PASSED :: TEST(ruby) [Find w/ condition]"
    else
      puts "FAILED :: TEST(ruby) [Find w/ condition]"
      puts reply2rows(ans)
    end
  else
    puts "FAILED :: TEST(ruby) [Find w/o condition]"
  end
  # [RESET]
  client.drop()
end

def testDelete(client)
  # [SETUP]
  client.drop()
  client.insertOne("{\"_id\":\"id000\",\"value\":\"AAAA\"}")
  client.insertOne("{\"_id\":\"id001\",\"value\":\"BBBB\"}")
  client.insertOne("{\"_id\":\"id002\",\"value\":\"AAAA\"}")
  client.insertOne("{\"_id\":\"id003\",\"value\":\"BBBB\"}")
  
  # [TEST] Delete w/  condition (single)
  if(client.deleteExecuter("{\"value\":\"AAAA\"}",false) and
      client.find("{\"value\":\"AAAA\"}"))then
    ans = client.getReply()
    if(ans.include?("id000") and ans.include?("id002"))then
      puts "FAILED :: TEST(ruby) [Delete(single) w/ condition]"
    else
      puts "PASSED :: TEST(ruby) [Delete(single) w/ condition]"
    end
  else
    puts "FAILED :: TEST(ruby) [Delete(single) w/ condition]"
  end
  
  # [TEST] Delete w/o condition (multi)
  if(client.deleteExecuter("{\"value\":\"BBBB\"}",true) and
      client.find("{\"value\":\"BBBB\"}"))then
    ans = client.getReply()
    if(ans.include?("id001") or ans.include?("id003"))then
      puts "FAILED :: TEST(ruby) [Delete(multi) w/ condition]"
    else
      puts "PASSED :: TEST(ruby) [Delete(multi) w/ condition]"
    end
  else
    puts "FAILED :: TEST(ruby) [Delete(multi) w/ condition]"
  end
  # [RESET]
  client.drop()
end

def testInsertMany(client)
  # [RESET]
  client.drop()
  # [TEST] Insert Many
  5000.times do |idx|
    client.commitDocument("{\"_id\":\"id-#{idx}\",\"value\":\"XXXXXXXXXXXXXXXXXXXXXXX\"}")
  end
  if(client.insertMany())then
    if(client.find("{}"))then
      ans = client.getReply()
      if(ans.include?("id-0") and ans.include?("id-1") and 
          ans.include?("id-2") and ans.include?("id-3"))then
        puts "PASSED :: TEST(ruby) [Insert Many]"
      else
        puts "FAILED :: TEST(ruby) [Insert Many]"
      end
    else
      puts "FAILED :: TEST(ruby) [Insert Many]"
    end
  else
    puts "FAILED :: TEST(ruby) [Insert Many]"
  end
  # [RESET]
  client.drop()
end

def testUpdate(client)
  # [RESET]
  client.drop()
  
  # [SETUP] 
  client.insertOne("{\"_id\":\"id000\",\"value\":1,\"cond\":10}")
  client.insertOne("{\"_id\":\"id001\",\"value\":1,\"cond\":10}")
  client.insertOne("{\"_id\":\"id002\",\"value\":1,\"cond\":10}")
  
  # update one
  if(client.update("{\"_id\":\"id000\"}","{\"$set\":{\"value\":3}}",false))then
    if(client.find("{}"))then
      ans = client.getReply()
      if(ans.count('3') == 1)then
        puts "PASSED :: TEST(ruby) [Update One]"
      else
        puts "FALSED :: TEST(ruby) [Update One]"
      end
    end
  else
    puts "FAILED :: TEST(ruby) [Update One]"
  end
 
  # update many
  if(client.update("{\"cond\":10}","{\"$set\":{\"value\":3}}",true))then
    if(client.find("{}"))then
      ans = client.getReply()
      if(ans.count('3') == 3)then
        puts "PASSED :: TEST(ruby) [Update Many]"
      else
        puts "FALSED :: TEST(ruby) [Update Many]"
      end
    end
  else
    puts "FAILED :: TEST(ruby) [Update Many]"
  end

  # [RESET]
  client.drop()
end



def testCount(client)
  # [SETUP]
  client.drop()
  client.insertOne("{\"_id\":\"id000\",\"value\":\"AAAA\"}")
  client.insertOne("{\"_id\":\"id001\",\"value\":\"BBBB\"}")
  client.insertOne("{\"_id\":\"id002\",\"value\":\"AAAA\"}")
  client.insertOne("{\"_id\":\"id003\",\"value\":\"BBBB\"}")

  # [TEST] Count 
  if(client.count("{}") == 4)then
    puts "PASSED :: TEST(ruby) [Count w/o condition]"
  else
    puts "FALSED :: TEST(ruby) [Count w/o condition]"
  end
  if(client.count("{\"value\":\"AAAA\"}")  == 2)then
    puts "PASSED :: TEST(ruby) [Count w/ condition]"
  else
    puts "FALSED :: TEST(ruby) [Count w/ condition]"
  end
  # [RESET]
  client.drop()
end

def testAggregate(client)
  # [SETUP]
  client.drop()
  client.insertOne("{\"_id\":\"id000\",\"value\":\"AAAA\",\"num\":10,\"sizes\":[\"S\",\"M\"]}")
  client.insertOne("{\"_id\":\"id001\",\"value\":\"BBBB\",\"num\":20}")
  client.insertOne("{\"_id\":\"id002\",\"value\":\"AAAA\",\"num\":30,\"sizes\":[\"S\",\"M\"]}")
  client.insertOne("{\"_id\":\"id003\",\"value\":\"BBBB\",\"num\":40}")

  # [TEST] GROUP
  client.setAggregateCommand("group","{\"_id\":\"$name\",\"total\":{\"$sum\":\"$num\"}}")
  client.aggregate()
  ans = eval(client.getReply().gsub(/\s/,"").gsub("null","nil"))
  if(ans[:_id] == nil and ans[:total] == 100)then
    puts "PASSED :: TEST(ruby) [Aggregate($group)]"
  else
    puts "FAILED :: TEST(ruby) [Aggregate($group)]"
  end

  # [TEST] MATCH + GROUP
  client.setAggregateCommand("match","{\"value\":\"AAAA\"}");
  client.setAggregateCommand("group","{\"_id\":\"$name\",\"total\":{\"$sum\":\"$num\"},\"max\":{\"$max\":\"$num\"},\"min\":{\"$min\":\"$num\"}}")
  client.aggregate()
  ans = eval(client.getReply().gsub(/\s/,"").gsub("null","nil"))
  if(ans[:_id] == nil and ans[:total] == 40 and
      ans[:max] == 30 and ans[:min] == 10)then
    puts "PASSED :: TEST(ruby) [Aggregate($match,$group)]"
  else
    puts "FAILED :: TEST(ruby) [Aggregate($match,$group)]"
  end
  
  # [TEST] UNWIND
  client.setAggregateCommand("unwind","{\"path\":\"$sizes\"}")
  client.aggregate()
  ans = eval("[#{client.getReply().gsub("\n",",").gsub(/\s/,"")}]")
  if(ans[0][:_id] == "id000" and ans[0][:value] == "AAAA" and
      ans[0][:num] == 10 and ans[0][:sizes] == "S" and
      ans[1][:_id] == "id000" and ans[1][:value] == "AAAA" and
      ans[1][:num] == 10 and ans[1][:sizes] == "M" and
      ans[2][:_id] == "id002" and ans[2][:value] == "AAAA" and
      ans[2][:num] == 30 and ans[2][:sizes] == "S" and
      ans[3][:_id] == "id002" and ans[3][:value] == "AAAA" and
      ans[3][:num] == 30 and ans[3][:sizes] == "M")then
    puts "PASSED :: TEST(ruby) [Aggregate($unwind)]"
  else
    puts "FAILED :: TEST(ruby) [Aggregate($unwind)]"
  end

  # [RESET]
  client.drop()
end

begin
  client = MongodbCxxRunner.new()
  env_ip = ENV["MONGODB_IPADDRESS"]
  unless env_ip
    env_ip = "127.0.0.1"
  end
  client.connect("mongodb://#{env_ip}:27017")
  client.setDatabaseName("testdb")
  client.setCollectionName("testcollection")

  client.drop();

  # [TEST] Insert One & Find
  testInsertOneAndFind(client)

  # [TEST] Delete
  testDelete(client)

  # [TEST] Insert Many
  testInsertMany(client)

  # [TEST] Update
  testUpdate(client)

  # [TEST] Count
  testCount(client)

  # [TEST] Aggregate
  testAggregate(client)


  client.drop();

  client.close()
end
