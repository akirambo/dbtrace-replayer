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

class AbstractDBParser
  def initialize(filename, logs, supported_command, option, logger)
    @filename = File.expand_path(filename)
    @logs     = logs
    @supported_command = supported_command
    @option = option
    @logger = logger
  end

  def exec()
    log_commands = []
    if !@option[:parseMultiLines]
      begin
        File.open(@filename, "r") do |f|
          while line = f.gets
            parsed = parse(line)
            if parsed
              command = parsed.keys()[0]
              unless log_commands.include?(command)
                log_commands.push(command)
              end
              @logs.push(parsed)
            end
          end
        end
      rescue => e
        @logger.error(e)
      end
    else
      ## @logs is updated in parse_multilines
      logs_command = parse_multilines(@filename)
    end
  end

  def log
    return @logs
  end

  def workload
    return @logs.log
  end

  protected
  
  def parse(_line)
    @logger.warn("Parse Method is not implemeted")
  end

  def operationCount(line)
    @logger.warn("OperationCount method is Not Implemented")
  end
end
