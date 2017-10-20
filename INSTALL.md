

## Installation
- ruby 2.x or later
- rake
- bundler

### Setup Databases
 Please setup one or more databases from the following databases.
 - memcached
 - redis
 - mongodb
 - cassandra

### Setup This tool
It is required sudo command to finish setup phase.

For ALL
```
rake setup:bundle
```

For Redis
```
rake setup:redis
```

For Memcached
```
rake setup:memcached
```

For Mongodb
```
rake setup:mognodb
```

For Cassandra
```
rake setup:cassandra
```

