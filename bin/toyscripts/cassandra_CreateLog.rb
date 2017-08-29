
begin
  5000.times do |i|
    puts "{'page_size': '10000', 'query': 'insert into testdb.test (key,value) values (memtier-#{i},xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx)'} | Execute CQL3 query | 2017-03-21 08:09:28-0400"
    puts "{'page_size': '10000', 'query': 'select value from testdb.test where key = memtier-#{i}'} | Execute CQL3 query | 2017-03-21 08:09:28-0400"
  end
end
