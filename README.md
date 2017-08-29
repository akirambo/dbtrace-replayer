# Database Trace Replayer

## What is Database Trace Replayer 
 - A Database Trace Replaying Tools For Non-SQL Database.
 - Auto Genarating Replaying Query Process from Database Traces(Logs).
 - Auto Converting Query Process/Datamodel to target Database from other database trace.

## Supported Platform
 This tool has been tested to work on the following platform.
 - Ubuntu Linux 14.04+ (64-bit)
 - Centos 7.x (64-bit)

## Supported Database
|Database|Version|
|-|-|
|Memcached|1.4|
|Redis|3.0 to 3.2|
|Mongodb|v2.6 to v3.2|
|Cassandra|3.0 - 3.6|

## Quick Trial(Start)
- Under Construction

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

## How to Replay Traces

## License
 Copyright (c) 2017, Carnegie Mellon University.
 All rights reserved.
