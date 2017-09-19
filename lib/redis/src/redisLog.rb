
require_relative "../../common/abstractDBLog"

class RedisLogsSimple < AbstractDBLog
  def initialize(command2primitive, option, logger)
    @primitive_operation_for_multidata = {
      "HMSET" => { "prefixCount" => 1,
                   "argNumEachPrimitiveCommand" => 2,
                   "postfixCount" => 0 },
      "MSET" => { "prefixCount" => 0,
                  "argNumEachPrimitiveCommand" => 2,
                  "postfixCount" => 0 },
      "MGET" => { "prefixCount" => 0,
                  "argNumEachPrimitiveCommand" => 1,
                  "postfixCount" => 0 },
      "HMGET" => { "prefixCount" => 1,
                   "argNumEachPrimitiveCommand" => 1,
                   "postfixCount" => 0 },
      "ZADD" => { "prefixCount" => 0,
                  "argNumEachPrimitiveCommand" => 3,
                  "postfixCount" => 0 },
      "MSETNX" => { "prefixCount" => 0,
                    "argNumEachPrimitiveCommand" => 2,
                    "postfixCount" => 0 },
      "SMOVE" => { "prefixCount" => 0,
                   "argNumEachPrimitiveCommand" =>
                   { "SCAN" => 2, "INSERT" => 2 },
                   "postfixCount" => 0 },
      "SINTERSTORE" => { "prefixCount" => 0,
                         "argNumEachPrimitiveCommand" =>
                         { "READ" => 1, "INSERT" => 2 },
                         "postfixCount" => 1 },
      "SINTER" => { "prefixCount" => 0,
                    "argNumEachPrimitiveCommand" => 1,
                    "postfixCount" => 0 },
      "SDIFF" => { "prefixCount" => 0,
                   "argNumEachPrimitiveCommand" => 1,
                   "postfixCount" => 0 },
      "SDIFFSTORE" => { "prefixCount" => 0,
                        "argNumEachPrimitiveCommand" =>
                        { "READ" => 1, "INSERT" => 2 },
                        "postfixCount" => 0 },
      "LTRIM" => { "operation" => "range",
                   "arg0" => 1,
                   "arg1" => 2 },
      "ZRANGE" => { "operation" => "range",
                    "arg0" => 1,
                    "arg1" => 2 },
      "ZREVRANGE" => { "operation" => "range",
                       "arg0" => 1,
                       "arg1" => 2 },
      "ZREMRANGEBYRANK" => { "operation" => "range",
                             "arg0" => 1,
                             "arg1" => 2 },
      "ZUNIONSTORE" => { "operation" => "fromArgument",
                         "arg" => 1 },
    }
    super(command2primitive, option, logger)
  end
end
