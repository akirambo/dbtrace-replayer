# Database Trace Replayer 
[![Test Coverage](https://codeclimate.com/github/akirambo/dbtrace-replayer/badges/coverage.svg)](https://codeclimate.com/github/akirambo/dbtrace-replayer/coverage)
[![Build Status](https://travis-ci.org/akirambo/dbtrace-replayer.svg?branch=master)](https://travis-ci.org/akirambo/dbtrace-replayer)
[![Code Climate](https://codeclimate.com/github/akirambo/dbtrace-replayer/badges/gpa.svg)](https://codeclimate.com/github/akirambo/dbtrace-replayer)

## What does Database Trace Replayer do
It is diffcult and costly work to port NoSQL database processes to other NoSQL database,Because these processes are implemented by different language and data mode.To reduce porting work. Database Trace Replayer is a tool to port NoSQL database queries to other NoSQL database automatically. This tool extracts query workloads from logs, which are generated on a previous database, replaies them on other database.

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

## Quick Start with docker
Please set up docker on your machine.
- Build docker container
```
 ruby bin/docker.rb build
```
- Run TEST
```
 # Unit Test
 ruby bin/docker.rb unittest
 # Test With Database
 ruby bin/docker.rb test
```
- Run Execute
```
 ruby bin/docker.rb rake run [redis,mongodb]
 ruby bin/docker.rb run TRACE_FILE_NAME
```

## How to Replay Traces
under construction

## License
 Copyright (c) 2017, Carnegie Mellon University.
 All rights reserved.
