
# docker file for database trace replay
FROM ubuntu:16.04

## IF IT IS UNDER PROXY PLEASE SET ##
#ENV http_proxy <HTTP_PROXY>
#ENV https_proxy <HTTPS_PROXY>
ENV LD_LIBRARY_PATH /usr/local/lib/x86_64-linux-gnu/
################ FOR TRIAL ###################
ENV http_proxy="http://proxy.pdl.cmu.edu:8080" \
     https_proxy="http://proxy.pdl.cmu.edu:8080"
##############################################

RUN apt-get update && \
    apt-get install -y apt-utils gcc git g++ make ruby ruby-dev sudo && \
    gem install bundler rake && \
    echo "root:Docker!" | chpasswd

# ADD USER
RUN groupadd -g 1000 dbtrace && \
    useradd -g dbtrace -G sudo -m -s /bin/bash replayer && \
    echo 'replayer:password' | chpasswd

WORKDIR /home/replayer/
# BUILD Database Trace Replayer
RUN git clone https://github.com/akirambo/dbtrace-replayer.git
WORKDIR /home/replayer/dbtrace-replayer
RUN rake install:cassandra && \
    rake install:redis && \
    rake install:memcached && \
    rake install:mongodb 

RUN chown replayer:dbtrace -R ../dbtrace-replayer/

USER replayer
WORKDIR /home/replayer/dbtrace-replayer
RUN rake setup:bundle && \
    rake setup:redis && \
    rake setup:memcached && \
    rake setup:cassandra && \
    rake setup:mongodb




