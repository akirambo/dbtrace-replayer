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

require_relative "mongodbLog"
require_relative "../../common/abstractDBParser"

class MongodbParser < AbstractDBParser
  def initialize(filename, option, logger)
    @skip_types = %w[ismaster isMaster getMore distinct buildinfo getlasterror whatsmyuri drop listCollections renameCollection createIndexes create buildInfo replSetGetStatus].freeze
    @command2primitive = {
      "insert"        => "INSERT",
      "update"        => "UPDATE",
      "find"          => "SCAN",
      "findandmodify" => "UPDATE",
      "delete"        => "UPDATE",
      "query"         => "READ",
      "count"         => "SCAN",
      "group"         => "SCAN",
      "aggregate"    => "SCAN",
      "mapreduce"    => "SCAN",
    }
    logs = MongodbLogsSimple.new(@command2primitive, option, logger)
    super(filename, logs, @command2primitive.keys, option, logger)
  end

  ## overwrite
  def parse(line)
    if line.include?("D COMMAND")
      line.match(/command (\w+)\.\$cmd { (\w+):(.+)/) do |md|
        if @supported_command.include?(md[2])
          result = {}
          ## add databasename
          collection_name = md[3].split(",")[0]
          database_name = md[3].split(",")[0].sub("\"", "\"#{md[1]}.")
          result[md[2]] = md[3].sub(collection_name, database_name)
          return result
        elsif !@skip_types.include?(md[2])
          @logger.warn("Unsupported Type #{md[2]}")
        end
      end
    end
    nil
  end
end
