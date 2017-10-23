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
require "optparse"
class CommandLine
  COMMANDS = {
    "build" => "Docker build replay images",
    "rm" => "Remove container",
    "unittest" => "Run UNIT TEST ( run rake unitTest:all)",
    "test" => "Run TEST with Databases (run rake test:all)",
    "run" => "Run rake_command",
  }

  def initialize
    @opt = OptionParser.new
    @opt.banner = "Usage: ruby bin/docker.rb COMMAND\n"
    @opt.banner += " [COMMANDS]\n"
    COMMANDS.each do |command, description|
      @opt.banner += "    #{command} : #{description}\n"
      if command == "run"
        rake_commands = `rake -T`.split("\n")
        rake_commands.each do |rake_command|
          @opt.banner += "               : #{rake_command}\n"
        end
      end
    end
    @opt.version = "0.0.1"
  end

  def parse(argv)
    @opt.parse!(argv)
  end
end

class DockerRunner
  REPLAY = "replay -ti -d replay bash"
  REDIS = "redis_docker -d redis:3.2 redis-server --appendonly yes"
  MONGODB = "mongodb_docker -d mongo:3.2"
  MEMCACHED = "memcached_docker -d memcached"
  CASSANDRA = "cassandra_docker -d cassandra:3.4"
  
  RUNNER_PREFIX = "docker run --name"
  GET_IP_COMMAND = "docker inspect --format '{{ .NetworkSettings.IPAddress }}'"
  def initialize
    @commands = {
      "redis_docker" => "#{RUNNER_PREFIX} #{REDIS}",
      "mongodb_docker" => "#{RUNNER_PREFIX} #{MONGODB}",
      "memcached_docker" => "#{RUNNER_PREFIX} #{MEMCACHED}",
      "cassandra_docker" => "#{RUNNER_PREFIX} #{CASSANDRA}",
      "replay" => "#{RUNNER_PREFIX} #{REPLAY}"
    }
    @runs = []
    @containers = []
    filename = __FILE__.split("/").last
    @home_path = File.expand_path(__FILE__).sub("bin/#{filename}", "")
  end

  def exec(args)
    method = args.shift
    self.send(method, args)
  end

  private

  def build(_ = nil)
    unless exist_image?
      `cd #{@home_path}; docker build -t replay .`
    end
    puts "Finished Build Docker Image"
  end

  def rm(_ = nil)
    check
    @runs.each do |docker_name|
      if @commands.keys.include?(docker_name)
        puts "REMOVE DOCKER CONTAINER : #{docker_name}"
        puts "docker stop #{docker_name}"
        `docker stop #{docker_name}`
      end
    end
    @containers.each do |docker_name|
      if @commands.keys.include?(docker_name)
        puts "docker rm #{docker_name}"
        `docker rm #{docker_name}`
      end
    end
  end

  def unittest(_ = nil)
    start
    run("rake unitTest:all")
  end

  def test(_ = nil)
    start
    run("rake test:all")
  end

  def run(rake_command)
    start
    puts "#{rake_command} on replay(docker)"
    puts `docker exec replay #{rake_command}`
  end

  def commit(filename)
    start
    puts `docker cp #{filename} replay:/home/replayer/dbtrace-replayer/.`
  end

  def start(_ = nil)
    check
    @commands.each do |docker_name, command|
      unless @runs.include?(docker_name)
        puts "START DOCKER CONTAINER : #{docker_name}"
        p command
        puts `#{command}`
      end
    end
    setup
  end

  def setup(_ = nil)
    # Get IP Addresses Of Database Dockers 
    ips = {}
    @commands.each_key do |docker_name|
      env_name = "#{docker_name.sub("_docker", "").upcase}_IPADDRESS"
      ips[env_name] = `#{GET_IP_COMMAND} #{docker_name}`.delete("\n")
    end
    File.open("database.config", "w") do |f|
      ips.each do |env_name, ip|
        f.write("#{env_name},#{ip}\n")
      end
    end
    # CP database.config
    `docker cp database.config replay:/home/replayer/dbtrace-replayer/.`
  end

  def check(_ = nil)
    @runs = []
    out = `docker ps`
    list = out.split("\n")
    list.each do |row_|
      row = row_.split(" ")
      @runs.push(row.last)
    end
    @containers = []
    out = `docker ps -a`
    list = out.split("\n")
    list.each do |row_|
      row = row_.split(" ")
      @containers.push(row.last)
    end
  end

  def exist_image?(_ = nil)
    `docker images | grep replay`.size != 0
  end
end

begin
  command = CommandLine.new
  command.parse(ARGV)
  runner = DockerRunner.new
  runner.exec(ARGV)
end
