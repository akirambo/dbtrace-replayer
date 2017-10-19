# How to collect Trace

## Redis
```
% redis-cli monitor |tee redis_trace.log
```

## Mongodb
When you boot mongod process with "-vvvv"
```
mongod -vvvv | tee  mongodb_trace.log
```

## Memcached
```
memcached -vvvv | tee memcached_trace.log
```

## Cassandra
```
sudo nodetool --host IP_ADDRESS settraceprobability 1
```

