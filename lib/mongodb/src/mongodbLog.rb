require_relative "../../common/abstractDBLog"

class MongodbLogsSimple < AbstractDBLog
  def initialize(command2primitive, option, logger)
    @primitiveOperationForMultiData = {
      "insert" => {
        "splitWord" => "documents:",
        "keyValue"  => ":",
        "skip"      => 1
      },
      "update" => {
        "splitWord" => "\$set:",
        "keyValue"  => ":",
        "skip"      => 0
      },
      "find"   => {
        "splitWord" => nil,
        "keyValue"  => nil,
        "skip"      => 0
      },
      "findandmodify" => {
        "splitWord" => nil,
        "keyValue"  => nil,
        "skip"      => 0
      },
      "delete" => {
        "splitWord" => nil,
        "keyValue"  => nil,
        "skip"      => 0
      },
      "query" => {
        "splitWord" => nil,
        "keyValue"  => nil,
        "skip"      => nil
      },
      ## -----------------
=begin
         "count" => {
           "splitWord" => nil,
           "keyValue"  => nil,
           "skip"      => nil
         },
         "group" => {
           "splitWord" => nil,
           "keyValue"  => nil,
           "skip"      => nil
         },
         "aggregate" => {
           "splitWord" => nil,
           "keyValue"  => nil,
           "skip"      => nil
         },
         "mapreduce" => {
           "splitWord" => nil,
           "keyValue"  => nil,
           "skip"      => nil
         }
=end
    }
    super(command2primitive, option, logger)
  end
end
